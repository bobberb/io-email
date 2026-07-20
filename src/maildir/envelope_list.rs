//! Maildir envelope-listing coroutine.
//!
//! Composes two io-maildir state machines:
//! 1. [`MaildirMessagesList`] walks `cur/` + `new/` and returns one
//!    [`MaildirEntry`] per file.
//! 2. A second pass batches the entry paths through
//!    [`MaildirYield::WantsFileRead`]; the driver reads each file and
//!    feeds the bytes back so the coroutine can parse RFC 5322 headers
//!    (subject, from, to, date, message-id) via [`mail_parser::Message`].
//!
//! Sorting is by `Date:` header descending; pagination is 1-indexed
//! on the in-memory result.
//!
//! [`MaildirMessagesList`]: io_maildir::coroutines::message_list::MaildirMessagesList

use alloc::{
    collections::{BTreeMap, BTreeSet},
    string::{String, ToString},
    vec::Vec,
};
use core::mem;
use std::path::PathBuf;

use chrono::DateTime;
use io_maildir::{
    coroutine::*,
    coroutines::message_list::{
        MaildirMessagesList as InnerList, MaildirMessagesListError as InnerErr,
    },
    entry::MaildirEntry,
    flag::KeywordHeader,
    maildir::Maildir,
    message::MaildirMessage,
    path::MaildirPath,
};
use log::trace;
use mail_parser::Address as MailParserAddress;
use thiserror::Error;

use crate::{
    address::Address,
    envelope::{Envelope, normalize_message_id},
    maildir::convert::{InvalidMailboxName, flags_from_maildir, paginate, resolve_mailbox},
};

/// Errors produced by [`MaildirEnvelopeList`].
#[derive(Debug, Error)]
pub enum MaildirEnvelopeListError {
    #[error(transparent)]
    List(#[from] InnerErr),
    #[error(transparent)]
    InvalidMailbox(#[from] InvalidMailboxName),
    #[error("coroutine was resumed with a MaildirReply variant it did not request")]
    UnexpectedReply,
    #[error("coroutine was resumed after completion")]
    ResumedAfterDone,
}

/// I/O-free coroutine listing every message inside a single Maildir,
/// sorted by date descending then paginated.
pub struct MaildirEnvelopeList {
    state: State,
    page: Option<u32>,
    page_size: Option<u32>,
    dovecot_table: BTreeMap<char, String>,
    keywords_header: Option<KeywordHeader>,
}

impl MaildirEnvelopeList {
    /// `dovecot_table` is the folder's `dovecot-keywords` slot table
    /// (empty when `dovecot_keywords` is off); `keywords_header`
    /// selects the body header to parse for keywords (none when
    /// unset). Both are read symmetrically with the write path so a
    /// keyword persisted on add/store is surfaced here.
    pub fn new(
        root: impl Into<PathBuf>,
        maildir_plus: bool,
        mailbox: &str,
        page: Option<u32>,
        page_size: Option<u32>,
        dovecot_table: BTreeMap<char, String>,
        keywords_header: Option<KeywordHeader>,
    ) -> Result<Self, MaildirEnvelopeListError> {
        trace!("prepare Maildir envelope listing");
        let path = resolve_mailbox(&root.into(), maildir_plus, mailbox)?;
        let maildir = Maildir::from_path(path);
        Ok(Self {
            state: State::Listing(InnerList::new(maildir)),
            page,
            page_size,
            dovecot_table,
            keywords_header,
        })
    }
}

impl MaildirCoroutine for MaildirEnvelopeList {
    type Yield = MaildirYield;
    type Return = Result<Vec<Envelope>, MaildirEnvelopeListError>;

    fn resume(
        &mut self,
        arg: Option<MaildirReply>,
    ) -> MaildirCoroutineState<Self::Yield, Self::Return> {
        match mem::replace(&mut self.state, State::Done) {
            State::Listing(mut inner) => match inner.resume(arg) {
                MaildirCoroutineState::Yielded(y) => {
                    self.state = State::Listing(inner);
                    MaildirCoroutineState::Yielded(y)
                }
                MaildirCoroutineState::Complete(Ok(entries)) => {
                    if entries.is_empty() {
                        return MaildirCoroutineState::Complete(Ok(Vec::new()));
                    }
                    let paths: BTreeSet<MaildirPath> =
                        entries.iter().map(|e| e.path().clone()).collect();
                    self.state = State::Reading(entries);
                    MaildirCoroutineState::Yielded(MaildirYield::WantsFileRead(paths))
                }
                MaildirCoroutineState::Complete(Err(err)) => {
                    MaildirCoroutineState::Complete(Err(err.into()))
                }
            },
            State::Reading(entries) => {
                let Some(MaildirReply::FileRead(mut contents)) = arg else {
                    self.state = State::Reading(entries);
                    return MaildirCoroutineState::Complete(Err(
                        MaildirEnvelopeListError::UnexpectedReply,
                    ));
                };
                let mut envelopes: Vec<Envelope> = entries
                    .into_iter()
                    .filter_map(|entry| {
                        let bytes = contents.remove(entry.path())?;
                        Some(envelope_from_message(
                            &MaildirMessage::from((entry.path().clone(), bytes)),
                            &self.dovecot_table,
                            self.keywords_header,
                        ))
                    })
                    .collect();
                envelopes.sort_by(|a, b| b.date.cmp(&a.date));
                MaildirCoroutineState::Complete(Ok(paginate(envelopes, self.page, self.page_size)))
            }
            State::Done => {
                MaildirCoroutineState::Complete(Err(MaildirEnvelopeListError::ResumedAfterDone))
            }
        }
    }
}

/// Two-phase state: list entries, then read their bytes for header
/// parsing.
enum State {
    Listing(InnerList),
    Reading(BTreeSet<MaildirEntry>),
    Done,
}

/// Builds an [`Envelope`] from a Maildir message: filename letters
/// plus resolved custom keywords for flags, RFC 5322 headers via
/// mail-parser.
fn envelope_from_message(
    message: &MaildirMessage,
    dovecot_table: &BTreeMap<char, String>,
    keywords_header: Option<KeywordHeader>,
) -> Envelope {
    let id = message.id().unwrap_or_default().to_string();
    let flags = flags_from_maildir(
        message.path(),
        message.contents(),
        dovecot_table,
        keywords_header,
    );
    let size = message.contents().len() as u64;
    let parsed = message.parsed();

    let subject = parsed
        .as_ref()
        .and_then(|m| m.subject())
        .unwrap_or_default()
        .to_string();

    let from = parsed
        .as_ref()
        .and_then(|m| m.from())
        .map(addresses_from)
        .unwrap_or_default();

    let to = parsed
        .as_ref()
        .and_then(|m| m.to())
        .map(addresses_from)
        .unwrap_or_default();

    let date = parsed
        .as_ref()
        .and_then(|m| m.date())
        .and_then(|d| DateTime::parse_from_rfc3339(&d.to_rfc3339()).ok());

    let has_attachment = parsed.as_ref().map(|m| m.attachment_count() > 0);

    let message_id = parsed
        .as_ref()
        .and_then(|m| m.message_id())
        .and_then(normalize_message_id);

    Envelope {
        id,
        message_id,
        flags,
        subject,
        from,
        to,
        date,
        size,
        has_attachment,
    }
}

/// Converts mail-parser's address group into the shared LCD shape.
/// Empty `email` addresses are dropped.
fn addresses_from(addrs: &MailParserAddress<'_>) -> Vec<Address> {
    addrs
        .clone()
        .into_list()
        .into_iter()
        .filter_map(|a| {
            let email = a.address?.into_owned();
            if email.is_empty() {
                return None;
            }
            let name = a.name.map(|s| s.into_owned());
            Some(Address { name, email })
        })
        .collect()
}
