//! Shared helpers for the Maildir coroutines: flag conversions,
//! mailbox-name validation, and `paginate` shared with m2dir.

use alloc::{
    collections::{BTreeMap, BTreeSet},
    string::{String, ToString},
    vec::Vec,
};

use io_maildir::{
    entry::headers::extract_keywords_header,
    flag::types::{KeywordHeader, MaildirFlag, MaildirFlags},
    path::{FsPath, MaildirPath},
};
use thiserror::Error;

use crate::flag::types::{Flag, IanaFlag};

/// Errors produced by the mailbox-name validator.
#[derive(Clone, Debug, Error)]
#[error("invalid Maildir mailbox `{0}`")]
pub struct InvalidMailboxName(pub String);

/// Validates a logical mailbox `name`; rejects `..` segments so
/// callers cannot escape the store. Empty name maps to the store root
/// (INBOX-equivalent under Maildir++).
pub(crate) fn mailbox_path(name: &str) -> Result<MaildirPath, InvalidMailboxName> {
    if name.split('/').any(|seg| seg == "..") {
        return Err(InvalidMailboxName(name.to_string()));
    }
    Ok(MaildirPath::from(name))
}

/// Maps a shared [`Flag`] to a [`MaildirFlag`]; non-IANA keywords go
/// through [`MaildirFlag::Keyword`] for the dovecot-keywords sidecar.
pub(crate) fn flag_to_maildir(flag: &Flag) -> MaildirFlag {
    match flag.iana() {
        Some(IanaFlag::Seen) => MaildirFlag::Seen,
        Some(IanaFlag::Answered) => MaildirFlag::Replied,
        Some(IanaFlag::Flagged) => MaildirFlag::Flagged,
        Some(IanaFlag::Draft) => MaildirFlag::Draft,
        Some(IanaFlag::Deleted) => MaildirFlag::Trashed,
        Some(IanaFlag::Forwarded) => MaildirFlag::Passed,
        Some(_) | None => MaildirFlag::Keyword(flag.raw().to_string()),
    }
}

/// Shared flag slice to [`MaildirFlags`].
pub(crate) fn flags_to_maildir(flags: &[Flag]) -> MaildirFlags {
    flags.iter().map(flag_to_maildir).collect()
}

/// Maildir info-section letter (S/R/F/D/T/P) to shared [`Flag`];
/// `None` for letters outside the standard six.
pub(crate) fn flag_from_char(c: char) -> Option<Flag> {
    match c {
        'S' => Some(Flag::from_iana(IanaFlag::Seen)),
        'R' => Some(Flag::from_iana(IanaFlag::Answered)),
        'F' => Some(Flag::from_iana(IanaFlag::Flagged)),
        'D' => Some(Flag::from_iana(IanaFlag::Draft)),
        'T' => Some(Flag::from_iana(IanaFlag::Deleted)),
        'P' => Some(Flag::from_iana(IanaFlag::Forwarded)),
        _ => None,
    }
}

/// Rebuilds the shared [`Flag`] set of a Maildir message, mirroring
/// the keyword persistence of
/// [`io_maildir::client::MaildirClient::store`].
///
/// Standard info-section letters map to their IANA flags. Custom
/// keywords come back through the `dovecot_table` slot letters, or
/// through the `keywords_header` line, or not at all when both are
/// off, which is the strict-Maildir default.
pub(crate) fn flags_from_maildir(
    path: &FsPath,
    contents: &[u8],
    dovecot_table: &BTreeMap<char, String>,
    keywords_header: Option<KeywordHeader>,
) -> BTreeSet<Flag> {
    let mut flags = BTreeSet::new();

    if let Some(name) = path.file_name() {
        if let Some((_, letters)) = name.rsplit_once(',') {
            for c in letters.chars() {
                if let Some(flag) = flag_from_char(c) {
                    flags.insert(flag);
                } else if c.is_ascii_lowercase() {
                    if let Some(keyword) = dovecot_table.get(&c) {
                        flags.insert(Flag::from_raw(keyword.clone()));
                    }
                }
            }
        }
    }

    if let Some(header) = keywords_header {
        for keyword in extract_keywords_header(contents, header) {
            flags.insert(Flag::from_raw(keyword));
        }
    }

    flags
}

/// 1-indexed in-memory pagination shared with m2dir; `page_size = None`
/// returns the full slice; size 0 or a page past the end returns empty.
pub(crate) fn paginate<T>(items: Vec<T>, page: Option<u32>, page_size: Option<u32>) -> Vec<T> {
    let Some(size) = page_size else {
        return items;
    };
    if size == 0 {
        return Vec::new();
    }
    let page = page.unwrap_or(1).max(1);
    let skip = ((page - 1) as usize).saturating_mul(size as usize);
    if skip >= items.len() {
        return Vec::new();
    }
    items.into_iter().skip(skip).take(size as usize).collect()
}
