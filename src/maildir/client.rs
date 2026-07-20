//! Std-blocking Maildir client.
//!
//! Holds an inner [`io_maildir::client::MaildirClient`] wrapping the
//! filesystem root and its per-store knobs (`dovecot_keywords`,
//! `keywords_header`, `strip_headers`, `maildir_plus`,
//! `maildirpp_inbox`, `fs_layout`).
//!
//! [`Self::run`] pumps io-email Maildir coroutines directly against the
//! local filesystem; the inner client's own helpers stay reachable
//! through [`Self::inner`] for ops that the shared API does not cover.

use alloc::{string::String, vec::Vec};
use std::{
    fs, io, process,
    time::{SystemTime, UNIX_EPOCH},
};

use alloc::collections::BTreeMap;

use gethostname::gethostname;
use io_maildir::{
    client::MaildirClient as InnerMaildirClient,
    coroutine::*,
    maildir::{Maildir, MaildirSubdir},
    path::MaildirPath,
};
use log::trace;
use thiserror::Error;

use crate::{
    envelope::Envelope,
    flag::{Flag, FlagOp},
    mailbox::Mailbox,
    maildir::{
        convert::{flags_to_maildir, resolve_mailbox},
        envelope_list::{MaildirEnvelopeList, MaildirEnvelopeListError},
        flag_store::MaildirFlagStoreError,
        mailbox_create::{MaildirMailboxCreate, MaildirMailboxCreateError},
        message_add::MaildirMessageAddError,
        mailbox_delete::{MaildirMailboxDelete, MaildirMailboxDeleteError},
        mailbox_list::{MaildirMailboxList, MaildirMailboxListError},
        message_copy::{MaildirMessageCopy, MaildirMessageCopyError},
        message_delete::{MaildirMessageDelete, MaildirMessageDeleteError},
        message_get::{MaildirMessageGet, MaildirMessageGetError},
        message_move::{MaildirMessageMove, MaildirMessageMoveError},
    },
};
#[cfg(feature = "search")]
use crate::{
    maildir::envelope_search::{MaildirEnvelopeSearch, MaildirEnvelopeSearchError},
    search::query::SearchEmailsQuery,
};

/// Errors surfaced by [`MaildirClient`] while running a coroutine.
///
/// One variant per shared-API Maildir coroutine.
#[derive(Debug, Error)]
pub enum MaildirClientError {
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error(transparent)]
    MailboxList(#[from] MaildirMailboxListError),
    #[error(transparent)]
    EnvelopeList(#[from] MaildirEnvelopeListError),
    #[cfg(feature = "search")]
    #[error(transparent)]
    EnvelopeSearch(#[from] MaildirEnvelopeSearchError),
    #[error(transparent)]
    FlagStore(#[from] MaildirFlagStoreError),
    #[error(transparent)]
    MailboxCreate(#[from] MaildirMailboxCreateError),
    #[error(transparent)]
    MailboxDelete(#[from] MaildirMailboxDeleteError),
    #[error(transparent)]
    MessageAdd(#[from] MaildirMessageAddError),
    #[error(transparent)]
    MessageCopy(#[from] MaildirMessageCopyError),
    #[error(transparent)]
    MessageDelete(#[from] MaildirMessageDeleteError),
    #[error(transparent)]
    MessageGet(#[from] MaildirMessageGetError),
    #[error(transparent)]
    MessageMove(#[from] MaildirMessageMoveError),
    #[error(transparent)]
    Inner(#[from] io_maildir::client::MaildirClientError),
    #[error(transparent)]
    InvalidMailbox(#[from] crate::maildir::convert::InvalidMailboxName),
}

/// Std-blocking Maildir client built on a filesystem root.
///
/// All per-store behaviour knobs (`dovecot_keywords`, `keywords_header`,
/// `strip_headers`, `maildir_plus`, `maildirpp_inbox`, `fs_layout`)
/// live on [`Self::inner`] and are read through it on every shared-API
/// call.
pub struct MaildirClient {
    pub inner: InnerMaildirClient,
}

impl MaildirClient {
    /// Wraps a fresh inner client rooted at `root`. All knobs default
    /// to strict-Maildir behaviour; flip them on [`Self::inner`] before
    /// running coroutines.
    pub fn new(root: impl Into<MaildirPath>) -> Self {
        Self {
            inner: InnerMaildirClient::new(root),
        }
    }

    /// Pumps any standard-shape Maildir coroutine
    /// (`Yield = MaildirYield`, `Return = Result<T, E>`) against the
    /// local filesystem until it terminates.
    ///
    /// Reaches into [`Self::inner`] for the root rather than delegating
    /// to [`io_maildir::client::MaildirClient::run`] so error variants
    /// route through [`MaildirClientError`] directly.
    pub fn run<C, T, E>(&self, mut coroutine: C) -> Result<T, MaildirClientError>
    where
        C: MaildirCoroutine<Yield = MaildirYield, Return = Result<T, E>>,
        MaildirClientError: From<E>,
    {
        let mut arg: Option<MaildirReply> = None;

        loop {
            match coroutine.resume(arg.take()) {
                MaildirCoroutineState::Complete(Ok(out)) => return Ok(out),
                MaildirCoroutineState::Complete(Err(err)) => return Err(err.into()),
                MaildirCoroutineState::Yielded(MaildirYield::WantsFileExists(paths)) => {
                    let mut out = alloc::collections::BTreeMap::new();
                    for path in paths {
                        let exists = fs::metadata(path.as_str())
                            .map(|m| m.is_file())
                            .unwrap_or(false);
                        trace!("file_exists {path}: {exists}");
                        out.insert(path, exists);
                    }
                    arg = Some(MaildirReply::FileExists(out));
                }
                MaildirCoroutineState::Yielded(MaildirYield::WantsDirExists(paths)) => {
                    let mut out = alloc::collections::BTreeMap::new();
                    for path in paths {
                        let exists = fs::metadata(path.as_str())
                            .map(|m| m.is_dir())
                            .unwrap_or(false);
                        trace!("dir_exists {path}: {exists}");
                        out.insert(path, exists);
                    }
                    arg = Some(MaildirReply::DirExists(out));
                }
                MaildirCoroutineState::Yielded(MaildirYield::WantsDirRead(paths)) => {
                    let mut entries = alloc::collections::BTreeMap::new();
                    for path in paths {
                        trace!("read_dir {path}");
                        let mut names = alloc::collections::BTreeSet::new();
                        match fs::read_dir(path.as_str()) {
                            Ok(iter) => {
                                for entry in iter {
                                    let entry = entry?;
                                    names.insert(normalize_path(entry.path()));
                                }
                            }
                            Err(err) if err.kind() == io::ErrorKind::NotFound => {}
                            Err(err) => return Err(err.into()),
                        }
                        entries.insert(path, names);
                    }
                    arg = Some(MaildirReply::DirRead(entries));
                }
                MaildirCoroutineState::Yielded(MaildirYield::WantsFileRead(paths)) => {
                    let mut contents = alloc::collections::BTreeMap::new();
                    for path in paths {
                        trace!("read_file {path}");
                        let bytes = fs::read(path.as_str())?;
                        contents.insert(path, bytes);
                    }
                    arg = Some(MaildirReply::FileRead(contents));
                }
                MaildirCoroutineState::Yielded(MaildirYield::WantsFileCreate(files)) => {
                    for (path, bytes) in files {
                        trace!("write {path} ({} bytes)", bytes.len());
                        if let Some(parent) = std::path::Path::new(path.as_str()).parent() {
                            fs::create_dir_all(parent)?;
                        }
                        fs::write(path.as_str(), &bytes)?;
                    }
                    arg = Some(MaildirReply::FileCreate);
                }
                MaildirCoroutineState::Yielded(MaildirYield::WantsDirCreate(paths)) => {
                    for path in paths {
                        trace!("create_dir_all {path}");
                        fs::create_dir_all(path.as_str())?;
                    }
                    arg = Some(MaildirReply::DirCreate);
                }
                MaildirCoroutineState::Yielded(MaildirYield::WantsDirRemove(paths)) => {
                    for path in paths {
                        trace!("remove_dir_all {path}");
                        fs::remove_dir_all(path.as_str())?;
                    }
                    arg = Some(MaildirReply::DirRemove);
                }
                MaildirCoroutineState::Yielded(MaildirYield::WantsRename(pairs)) => {
                    for (from, to) in pairs {
                        trace!("rename {from} -> {to}");
                        fs::rename(from.as_str(), to.as_str())?;
                    }
                    arg = Some(MaildirReply::Rename);
                }
                MaildirCoroutineState::Yielded(MaildirYield::WantsCopy(pairs)) => {
                    for (from, to) in pairs {
                        trace!("copy {from} -> {to}");
                        fs::copy(from.as_str(), to.as_str())?;
                    }
                    arg = Some(MaildirReply::Copy);
                }
                MaildirCoroutineState::Yielded(MaildirYield::WantsTime) => {
                    let ts = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
                    arg = Some(MaildirReply::Time {
                        secs: ts.as_secs(),
                        nanos: ts.subsec_nanos(),
                    });
                }
                MaildirCoroutineState::Yielded(MaildirYield::WantsPid) => {
                    arg = Some(MaildirReply::Pid(process::id()));
                }
                MaildirCoroutineState::Yielded(MaildirYield::WantsHostname) => {
                    let hostname = gethostname().into_string().unwrap_or_default();
                    arg = Some(MaildirReply::Hostname(hostname));
                }
            }
        }
    }

    /// Resolves `mailbox` to its on-disk [`Maildir`], honouring the
    /// inner client's `maildir_plus` layout. Used to reach the inner
    /// client's keyword-aware read/write helpers.
    fn open_maildir(&self, mailbox: &str) -> Result<Maildir, MaildirClientError> {
        let root = std::path::PathBuf::from(self.inner.root().clone());
        let path = resolve_mailbox(&root, self.inner.maildir_plus, mailbox)?;
        Ok(Maildir::from_path(path))
    }

    /// Loads the folder's `dovecot-keywords` slot table when the
    /// `dovecot_keywords` knob is set, else an empty table. Read and
    /// write paths share this so a keyword persisted on write is
    /// resolvable on read.
    fn dovecot_table_for(
        &self,
        maildir: &Maildir,
    ) -> Result<BTreeMap<char, String>, MaildirClientError> {
        if self.inner.dovecot_keywords {
            Ok(self.inner.load_dovecot_keywords(maildir)?)
        } else {
            Ok(BTreeMap::new())
        }
    }

    /// Lists every Maildir under the configured root. `with_counts`
    /// is currently a no-op; see [`MaildirMailboxList`] for the path
    /// to surfacing per-mailbox totals.
    pub fn list_mailboxes(&self, with_counts: bool) -> Result<Vec<Mailbox>, MaildirClientError> {
        let root = self.inner.root().clone();
        let maildir_plus = self.inner.maildir_plus;
        self.run(MaildirMailboxList::new(
            std::path::PathBuf::from(root),
            maildir_plus,
            with_counts,
        ))
    }

    /// Lists envelopes from `mailbox`. `page = None` and
    /// `page_size = None` return the whole listing. The
    /// `with_attachment` knob is currently ignored on Maildir.
    pub fn list_envelopes(
        &self,
        mailbox: &str,
        page: Option<u32>,
        page_size: Option<u32>,
        _with_attachment: bool,
    ) -> Result<Vec<Envelope>, MaildirClientError> {
        let root = self.inner.root().clone();
        let maildir_plus = self.inner.maildir_plus;
        let dovecot_table = self.dovecot_table_for(&self.open_maildir(mailbox)?)?;
        self.run(MaildirEnvelopeList::new(
            std::path::PathBuf::from(root),
            maildir_plus,
            mailbox,
            page,
            page_size,
            dovecot_table,
            self.inner.keywords_header,
        )?)
    }

    /// Searches envelopes in `mailbox` against the shared query.
    /// Filter / sort / paginate are applied client-side.
    #[cfg(feature = "search")]
    pub fn search_envelopes(
        &self,
        mailbox: &str,
        query: Option<&SearchEmailsQuery>,
        page: Option<u32>,
        page_size: Option<u32>,
        _with_attachment: bool,
    ) -> Result<Vec<Envelope>, MaildirClientError> {
        let root = self.inner.root().clone();
        let maildir_plus = self.inner.maildir_plus;
        let dovecot_table = self.dovecot_table_for(&self.open_maildir(mailbox)?)?;
        self.run(MaildirEnvelopeSearch::new(
            std::path::PathBuf::from(root),
            maildir_plus,
            mailbox,
            query,
            page,
            page_size,
            dovecot_table,
            self.inner.keywords_header,
        )?)
    }

    /// Adds, sets, or removes `flags` on a Maildir id set.
    ///
    /// Delegates to the inner client's keyword-aware flag helpers so
    /// that custom keywords in `flags` are persisted through the
    /// `dovecot-keywords` slot table when the `dovecot_keywords` knob
    /// is set (symmetrically with the read path). With the knob off,
    /// keywords are dropped — strict-Maildir default, unchanged.
    ///
    /// Note: the `keywords_header` mechanism only applies at message
    /// creation ([`Self::add_message`]); a flag store is a filename
    /// rename and cannot inject a body header, matching the inner
    /// io-maildir flag-store behaviour.
    pub fn store_flags(
        &self,
        mailbox: &str,
        ids: &[&str],
        flags: &[Flag],
        op: FlagOp,
    ) -> Result<(), MaildirClientError> {
        let maildir = self.open_maildir(mailbox)?;
        let md_flags = flags_to_maildir(flags);
        for id in ids {
            match op {
                FlagOp::Add => self.inner.add_flags(maildir.clone(), *id, md_flags.clone())?,
                FlagOp::Set => self.inner.set_flags(maildir.clone(), *id, md_flags.clone())?,
                FlagOp::Remove => self
                    .inner
                    .remove_flags(maildir.clone(), *id, md_flags.clone())?,
            }
        }
        Ok(())
    }

    /// Reads one message's raw RFC 5322 bytes from `mailbox`.
    pub fn get_message(&self, mailbox: &str, id: &str) -> Result<Vec<u8>, MaildirClientError> {
        let root = self.inner.root().clone();
        let maildir_plus = self.inner.maildir_plus;
        self.run(MaildirMessageGet::new(
            std::path::PathBuf::from(root),
            maildir_plus,
            mailbox,
            id,
        )?)
    }

    /// Appends `raw` to `mailbox` under `cur/` with the given flags.
    /// Returns the Maildir filename minus the `:2,FLAGS` suffix.
    ///
    /// Delegates to the inner client's keyword-aware `store` so custom
    /// keywords in `flags` are persisted per the configured knobs:
    /// `keywords_header` injects an `X-Keywords`/`X-Label` line into
    /// `raw`, and `dovecot_keywords` allocates slot letters and
    /// updates the folder table (read back symmetrically by
    /// [`Self::list_envelopes`]). With both off, keywords are dropped
    /// — strict-Maildir default, byte-for-byte unchanged.
    pub fn add_message(
        &self,
        mailbox: &str,
        flags: &[Flag],
        raw: Vec<u8>,
    ) -> Result<String, MaildirClientError> {
        let maildir = self.open_maildir(mailbox)?;
        let md_flags = flags_to_maildir(flags);
        let (id, _path) = self
            .inner
            .store(maildir, MaildirSubdir::Cur, md_flags, raw)?;
        Ok(id)
    }

    /// Creates `name` as a new Maildir under the configured root.
    pub fn create_mailbox(&self, name: &str) -> Result<(), MaildirClientError> {
        let root = self.inner.root().clone();
        let maildir_plus = self.inner.maildir_plus;
        self.run(MaildirMailboxCreate::new(
            std::path::PathBuf::from(root),
            maildir_plus,
            name,
        )?)
    }

    /// Recursively removes the Maildir named `name`.
    pub fn delete_mailbox(&self, name: &str) -> Result<(), MaildirClientError> {
        let root = self.inner.root().clone();
        let maildir_plus = self.inner.maildir_plus;
        self.run(MaildirMailboxDelete::new(
            std::path::PathBuf::from(root),
            maildir_plus,
            name,
        )?)
    }

    /// Flags `id` in `mailbox` as Trashed. Maildir has no atomic
    /// "remove" primitive; pair with a periodic expunge to reclaim
    /// space.
    pub fn delete_message(&self, mailbox: &str, id: &str) -> Result<(), MaildirClientError> {
        let root = self.inner.root().clone();
        let maildir_plus = self.inner.maildir_plus;
        self.run(MaildirMessageDelete::new(
            std::path::PathBuf::from(root),
            maildir_plus,
            mailbox,
            id,
        )?)
    }

    /// Copies every id from `from` to `to`.
    pub fn copy_messages(
        &self,
        from: &str,
        to: &str,
        ids: &[&str],
    ) -> Result<(), MaildirClientError> {
        let root = self.inner.root().clone();
        let maildir_plus = self.inner.maildir_plus;
        self.run(MaildirMessageCopy::new(
            std::path::PathBuf::from(root),
            maildir_plus,
            from,
            to,
            ids,
        )?)
    }

    /// Moves every id from `from` to `to`.
    pub fn move_messages(
        &self,
        from: &str,
        to: &str,
        ids: &[&str],
    ) -> Result<(), MaildirClientError> {
        let root = self.inner.root().clone();
        let maildir_plus = self.inner.maildir_plus;
        self.run(MaildirMessageMove::new(
            std::path::PathBuf::from(root),
            maildir_plus,
            from,
            to,
            ids,
        )?)
    }
}

/// Bulk-normalises a [`std::path::PathBuf`] into the Unix-styled
/// [`MaildirPath`] used by the io-maildir core.
fn normalize_path(path: std::path::PathBuf) -> MaildirPath {
    let s = path.to_string_lossy().into_owned();
    #[cfg(windows)]
    let s = s.replace('\\', "/");
    MaildirPath::new(s)
}

#[cfg(test)]
mod tests {
    use alloc::{
        string::{String, ToString},
        vec::Vec,
    };

    use io_maildir::flag::KeywordHeader;

    use super::*;
    use crate::flag::Flag;

    const KEYWORD: &str = "NonJunk";

    fn raw_message() -> Vec<u8> {
        b"Subject: keyword round-trip\r\n\
          From: a@b\r\n\
          To: c@d\r\n\
          Date: Thu, 15 May 2026 10:00:00 +0000\r\n\
          \r\n\
          body\r\n"
            .to_vec()
    }

    /// Returns the set of raw flag spellings surfaced for the single
    /// message the caller just wrote into `INBOX`.
    fn read_back_flags(client: &MaildirClient) -> Vec<String> {
        let envelopes = client
            .list_envelopes("INBOX", None, None, false)
            .expect("list_envelopes");
        assert_eq!(envelopes.len(), 1, "expected exactly one message");
        envelopes[0]
            .flags
            .iter()
            .map(|f| f.raw().to_string())
            .collect()
    }

    fn setup(dir: &std::path::Path) -> MaildirClient {
        let client = MaildirClient::new(dir.to_string_lossy().into_owned());
        client.create_mailbox("INBOX").expect("create INBOX");
        client
    }

    #[test]
    fn dovecot_keywords_round_trip_through_public_api() {
        let tmp = tempfile::tempdir().unwrap();
        let mut client = setup(tmp.path());
        client.inner.dovecot_keywords = true;

        client
            .add_message("INBOX", &[Flag::from_raw(KEYWORD)], raw_message())
            .expect("add_message");

        // The sidecar table must exist and the keyword must survive the
        // filename-slot round-trip through list_envelopes.
        let dovecot_file = tmp.path().join("INBOX").join("dovecot-keywords");
        assert!(dovecot_file.is_file(), "dovecot-keywords file not written");

        let raws = read_back_flags(&client);
        assert!(
            raws.iter().any(|r| r == KEYWORD),
            "keyword `{KEYWORD}` not surfaced on read; got {raws:?}"
        );
    }

    #[test]
    fn keywords_header_round_trip_through_public_api() {
        let tmp = tempfile::tempdir().unwrap();
        let mut client = setup(tmp.path());
        client.inner.keywords_header = Some(KeywordHeader::XKeywords);

        let id = client
            .add_message("INBOX", &[Flag::from_raw(KEYWORD)], raw_message())
            .expect("add_message");

        // The keyword must be persisted as an injected header, not a
        // dovecot sidecar (which stays absent under this knob).
        let raw = client.get_message("INBOX", &id).expect("get_message");
        let text = String::from_utf8_lossy(&raw);
        assert!(
            text.contains("X-Keywords:") && text.contains(KEYWORD),
            "X-Keywords header not injected; got:\n{text}"
        );
        assert!(
            !tmp.path().join("INBOX").join("dovecot-keywords").exists(),
            "dovecot-keywords file written despite header-only knob"
        );

        let raws = read_back_flags(&client);
        assert!(
            raws.iter().any(|r| r == KEYWORD),
            "keyword `{KEYWORD}` not surfaced on read; got {raws:?}"
        );
    }

    #[test]
    fn strict_default_drops_keyword() {
        let tmp = tempfile::tempdir().unwrap();
        let client = setup(tmp.path());
        // Both knobs at their strict-Maildir defaults (off / None).

        client
            .add_message("INBOX", &[Flag::from_raw(KEYWORD)], raw_message())
            .expect("add_message");

        assert!(
            !tmp.path().join("INBOX").join("dovecot-keywords").exists(),
            "dovecot-keywords file written under strict default"
        );

        let raws = read_back_flags(&client);
        assert!(
            !raws.iter().any(|r| r == KEYWORD),
            "keyword leaked under strict default; got {raws:?}"
        );
    }

    #[test]
    fn store_flags_persists_keyword_via_dovecot() {
        let tmp = tempfile::tempdir().unwrap();
        let mut client = setup(tmp.path());
        client.inner.dovecot_keywords = true;

        // Write clean, then add the keyword via a flag store.
        let id = client
            .add_message("INBOX", &[], raw_message())
            .expect("add_message");
        client
            .store_flags("INBOX", &[&id], &[Flag::from_raw(KEYWORD)], FlagOp::Add)
            .expect("store_flags add");

        let raws = read_back_flags(&client);
        assert!(
            raws.iter().any(|r| r == KEYWORD),
            "keyword not persisted by store_flags; got {raws:?}"
        );
    }
}
