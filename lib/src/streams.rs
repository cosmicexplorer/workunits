/*
 * Description: ???
 *
 * Copyright (C) 2023 Danny McClanahan <dmcC2@hypnicjerk.ai>
 * SPDX-License-Identifier: LGPL-3.0
 *
 * FIXME: is this sufficient license notice?
 * Licensed under the Lesser GPL, Version 3.0 (see LICENSE).
 */

//! ???


pub(crate) mod referencing {
  macro_rules! uuid_struct {
    ($name:ident) => {
      /// <id: {0}>
      #[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, ::displaydoc::Display)]
      pub struct $name(::uuid::Uuid);

      impl $name {
        pub fn new() -> Self { Self(::uuid::Uuid::new_v4()) }
      }
    };
  }

  uuid_struct![ReadStreamId];
  uuid_struct![WriteStreamId];
}

pub mod indexing {
  use displaydoc::Display;

  pub type MatchResult = bool;

  pub trait Matches {
    type Key;
    fn matches(&self, key: &Self::Key) -> MatchResult;
  }

  /* TODO: enable namespacing/prefixing/tagging. */
  /// <stream name: {0}>
  #[derive(Clone, Hash, Eq, PartialEq, Ord, PartialOrd, Debug, Display)]
  pub struct Name(pub String);

  impl Name {
    pub fn new(s: String) -> Self { Self(s) }
  }

  /// <stream name query: {0}>
  #[derive(Clone, Hash, Eq, PartialEq, Ord, PartialOrd, Debug, Display)]
  pub struct Query(pub String);

  impl Query {
    pub fn new(s: String) -> Self { Self(s) }
  }

  impl Matches for Query {
    type Key = Name;

    /* TODO: make this match more complex expressions, etc. */
    fn matches(&self, name: &Name) -> MatchResult { self.0 == name.0 }
  }

  pub(crate) mod caching {
    use super::{MatchResult, Matches};
    use crate::interns::{Index, InternHandle, InternTable};

    use indexmap::IndexMap;
    use parking_lot::RwLock;

    use std::{
      borrow::{Borrow, Cow, ToOwned},
      hash::Hash,
      sync::Arc,
    };

    macro_rules! interned_handle {
      ($name:ident) => {
        #[derive(Hash, Eq, PartialEq, Ord, PartialOrd, Copy, Clone, Debug)]
        pub(crate) struct $name(usize);

        impl InternHandle for $name {}

        impl From<usize> for $name {
          fn from(x: usize) -> Self { Self(x) }
        }
      };
    }

    interned_handle![InternedQuery];
    interned_handle![InternedName];

    pub trait MatchIndex {
      type Name: ToOwned<Owned=Self::Name>;
      type Query: Matches<Key=Self::Name>+ToOwned<Owned=Self::Query>;
      fn evaluate(&self, query: Cow<'_, Self::Query>, name: Cow<'_, Self::Name>) -> MatchResult;
    }

    #[derive(Copy, Clone, Eq, PartialEq, Hash)]
    struct CacheKey(InternedQuery, InternedName);

    #[derive(Clone)]
    pub struct QueryMatchIndex<Name, Query> {
      names: Index<Name, InternedName>,
      queries: Index<Query, InternedQuery>,
      cached: Arc<RwLock<IndexMap<CacheKey, MatchResult>>>,
    }

    impl<Name, Query> QueryMatchIndex<Name, Query> {
      pub fn new() -> Self {
        Self {
          names: Index::new(),
          queries: Index::new(),
          cached: Arc::new(RwLock::new(IndexMap::new())),
        }
      }
    }

    impl<Name, Query> MatchIndex for QueryMatchIndex<Name, Query>
    where
      Name: Hash+Eq+ToOwned<Owned=Name>,
      Query: Hash+Eq+ToOwned<Owned=Query>+Matches<Key=Name>,
    {
      type Name = Name;
      type Query = Query;

      fn evaluate(&self, query: Cow<'_, Self::Query>, name: Cow<'_, Self::Name>) -> MatchResult {
        /* (1) Try to retrieve the value from the cache without cloning the Cows. */
        if let Some(query_id) = self
          .queries
          .intern_soft(query.borrow()) &&
          let Some(name_id) = self.names.intern_soft(name.borrow()) &&
          let Some(cached_result) = self.cached.read().get(&CacheKey(query_id, name_id))
        {
          return *cached_result;
        }

        /* (2) Ensure both components are interned, check if the cache was filled in
         * the meantime, then fill it if not. */
        let query_id = self.queries.intern(query.clone());
        let name_id = self.names.intern(name.clone());
        *self
          .cached
          .write()
          .entry(CacheKey(query_id, name_id))
          .or_insert_with(|| query.matches(name.borrow()))
      }
    }
  }
}

pub mod mux {
  use super::{
    indexing::{
      caching::{MatchIndex, QueryMatchIndex},
      Matches, Name, Query,
    },
    referencing::{ReadStreamId, WriteStreamId},
  };

  use bytes::Bytes;
  use indexmap::{IndexMap, IndexSet};
  use log::{debug, error, warn};
  use parking_lot::RwLock;
  use rayon::prelude::*;
  use tokio::{self, sync::mpsc};

  use std::{
    borrow::Cow,
    cmp,
    collections::VecDeque,
    hash::Hash,
    io, mem,
    pin::Pin,
    sync::{
      atomic::{AtomicBool, Ordering},
      Arc,
    },
    task::{Context, Poll},
    time::{Duration, Instant},
  };

  pub trait StreamID {
    type ID: Eq+Hash+Copy;
    fn id(&self) -> Self::ID;
  }

  pub type Payload = Bytes;

  pub trait ListenStream: StreamID+io::Read+tokio::io::AsyncRead {
    fn signal_end(&self);
  }

  pub trait WriteStream: StreamID+io::Write+tokio::io::AsyncWrite {}

  pub trait Streamer {
    type Listener: ListenStream;
    type Query: Matches<Key=Self::Name>;
    fn open_listener(&self, query: Self::Query) -> Self::Listener;

    type Writer: WriteStream;
    type Name;
    fn open_writer(&self, name: Self::Name) -> Self::Writer;
  }

  pub struct ListenMux {
    id: ReadStreamId,
    recv: mpsc::Receiver<Payload>,
    remainder: VecDeque<u8>,
    end_signal: AtomicBool,
    index: Arc<RwLock<StreamMux>>,
  }

  impl ListenMux {
    pub(crate) fn new(
      id: ReadStreamId,
      recv: mpsc::Receiver<Payload>,
      index: Arc<RwLock<StreamMux>>,
    ) -> Self {
      Self {
        id,
        recv,
        remainder: VecDeque::new(),
        end_signal: AtomicBool::new(false),
        index,
      }
    }
  }

  impl StreamID for ListenMux {
    type ID = ReadStreamId;

    fn id(&self) -> Self::ID { self.id }
  }

  impl io::Read for ListenMux {
    /// A read() which may block and may not fill the whole buffer, but which
    /// will only block the minimum possible time necessary in order to
    /// return a non-zero value.
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
      if buf.is_empty() {
        return Ok(0);
      }

      let written = self.remainder.read(buf)?;
      /* If we could at least partially satisfy the request from the buffer, return
       * early. This does not signal EOF as long as n > 0 according to
       * https://doc.rust-lang.org/nightly/std/io/trait.Read.html#tymethod.read. */
      if written > 0 {
        return Ok(written);
      }
      assert!(self.remainder.is_empty());

      /* Check to see if we've been told there's no use waiting anymore. */
      let payload = if self.end_signal.load(Ordering::Acquire) {
        match self.recv.try_recv() {
          /* This occurs when all senders disconnect, which should never happen because we always
           * cache an extra SyncSender in ListenerContext. */
          Err(mpsc::error::TryRecvError::Disconnected) => unreachable!(),
          /* If we've been signalled that it's all empty, then we can signal EOF. */
          Err(mpsc::error::TryRecvError::Empty) => {
            return Ok(0);
          },
          Ok(payload) => payload,
        }
      } else {
        /* Otherwise, we wait to pull in anything (we *don't* wait to fill the whole
         * buffer). */
        match self.recv.blocking_recv() {
          /* This occurs when all senders disconnect, which should never happen because we always
           * cache an extra SyncSender in ListenerContext. */
          None => unreachable!(),
          Some(payload) => payload,
        }
      };
      assert!(!payload.is_empty());
      /* TODO: consider ringbuf crate! https://docs.rs/ringbuf/latest/ringbuf/ */
      self.remainder.extend(payload);
      /* We read from the beginning of buf because we early returned if we had
       * already written any bytes to buf earlier. */
      let written = self.remainder.read(buf)?;
      /* We checked that buf and payload are non-empty, so this should always be
       * true. Any nonzero value is acceptable for a read()
       * implementation without implicitly signaling EOF. */
      assert!(written > 0);
      /* self.remainder may or may not be empty at this point (either is allowed). */
      Ok(written)
    }
  }

  impl tokio::io::AsyncRead for ListenMux {
    fn poll_read(
      self: Pin<&mut Self>,
      _cx: &mut Context<'_>,
      buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
      if buf.remaining() == 0 {
        return Poll::Ready(Ok(()));
      }

      let self_ref = Pin::into_inner(self);
      let buffered_length_to_copy = cmp::min(buf.remaining(), self_ref.remainder.len());
      if buffered_length_to_copy > 0 {
        let copied: Vec<u8> = self_ref
          .remainder
          .drain(..buffered_length_to_copy)
          .collect();
        debug_assert!(copied.len() == buffered_length_to_copy);
        buf.put_slice(&copied);
        return Poll::Ready(Ok(()));
      }
      assert!(self_ref.remainder.is_empty());

      let end_signal = self_ref.end_signal.load(Ordering::Acquire);
      match self_ref.recv.try_recv() {
        /* This occurs when all senders disconnect, which should never happen because we always
         * cache an extra SyncSender in ListenerContext. */
        Err(mpsc::error::TryRecvError::Disconnected) => unreachable!(),
        Err(mpsc::error::TryRecvError::Empty) => {
          if end_signal {
            Poll::Ready(Ok(()))
          } else {
            Poll::Pending
          }
        },
        Ok(payload) => {
          assert!(!payload.is_empty());
          self_ref.remainder.extend(payload);
          let buffered_length_to_copy = cmp::min(buf.remaining(), self_ref.remainder.len());
          assert!(buffered_length_to_copy > 0);
          /* TODO: split off at slice lengths to avoid going through the iterator (?) */
          let copied: Vec<u8> = self_ref
            .remainder
            .drain(..buffered_length_to_copy)
            .collect();
          debug_assert!(copied.len() == buffered_length_to_copy);
          buf.put_slice(&copied);
          Poll::Ready(Ok(()))
        },
      }
    }
  }

  impl ListenStream for ListenMux {
    fn signal_end(&self) { self.end_signal.store(true, Ordering::Release); }
  }

  impl Drop for ListenMux {
    fn drop(&mut self) { self.index.write().remove_listener(self.id()); }
  }

  pub struct WriteMux {
    id: WriteStreamId,
    tees: Arc<RwLock<IndexMap<ReadStreamId, mpsc::Sender<Payload>>>>,
    remainder: Option<(Payload, Instant, IndexSet<ReadStreamId>)>,
    index: Arc<RwLock<StreamMux>>,
  }

  impl WriteMux {
    pub(crate) fn new(
      id: WriteStreamId,
      tees: Arc<RwLock<IndexMap<ReadStreamId, mpsc::Sender<Payload>>>>,
      index: Arc<RwLock<StreamMux>>,
    ) -> Self {
      Self {
        id,
        tees,
        remainder: None,
        index,
      }
    }
  }

  impl StreamID for WriteMux {
    type ID = WriteStreamId;

    fn id(&self) -> Self::ID { self.id }
  }

  impl io::Write for WriteMux {
    /// A write() which always succeeds in full, but may block when writing to
    /// bounded channels.
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
      if buf.is_empty() {
        return Ok(0);
      }

      let tees = self.tees.read();
      if tees.is_empty() {
        /* If no outputs, then writes are a no-op. */
        return Ok(buf.len());
      }

      /* A Bytes is cheaply cloneable, so it can be sent to all teed outputs
       * without additional copies. */
      let buf = Bytes::copy_from_slice(buf);
      /* NB: Not a good candidate for rayon iteration as each element may block an
       * arbitrary amount. */
      for sender in tees.values() {
        /* FIXME: not possible to send synchronously but with a max timeout. */
        #[allow(clippy::single_match_else)]
        if sender.blocking_send(buf.clone()).err().is_some() {
          /* This only occurs if the receiver has hung up in between when we read from
           * self.tees and this .send() call (see https://docs.rs/tokio/latest/tokio/sync/mpsc/error/enum.SendTimeoutError.html).
           * In this case, we log that this occurred in case it signals an issue but
           * otherwise ignore it. */
          debug!("receiver disconnected, which is fine");
        }
      }
      Ok(buf.len())
    }

    /// We have no concept of a "flush" for this unseekable stream, as we have
    /// no internal buffering.
    fn flush(&mut self) -> io::Result<()> { Ok(()) }
  }

  pub const MAX_WRITE_WAIT: Duration = Duration::from_millis(100);

  impl tokio::io::AsyncWrite for WriteMux {
    fn poll_write(
      self: Pin<&mut Self>,
      _cx: &mut Context<'_>,
      buf: &[u8],
    ) -> Poll<io::Result<usize>> {
      if buf.is_empty() {
        return Poll::Ready(Ok(0));
      }

      let self_ref = Pin::into_inner(self);
      let tees = self_ref.tees.read();
      if tees.is_empty() {
        /* If we had a remainder, kill it. We have no target streams now. */
        if self_ref.remainder.take().is_some() {
          warn!("non-empty remainder was deleted after output tees all disappeared");
        }

        /* If no outputs, then writes are a no-op. */
        return Poll::Ready(Ok(buf.len()));
      }

      if let Some((buf, start_time, blocked_readers)) = self_ref.remainder.take() {
        let blocked_readers: IndexSet<ReadStreamId> = blocked_readers
          .into_par_iter()
          .filter_map(|read_id| {
            let sender = match tees.get(&read_id) {
              Some(sender) => sender,
              None => {
                warn!(
                  "remainder read id {:?} was deleted since last poll",
                  read_id
                );
                return None;
              },
            };
            sender.try_send(buf.clone()).err().and_then(|e| match e {
              mpsc::error::TrySendError::Closed(_) => {
                debug!("receiver disconnected, which is fine");
                None
              },
              mpsc::error::TrySendError::Full(_) => Some(read_id),
            })
          })
          .collect();
        if blocked_readers.is_empty() {
          Poll::Ready(Ok(buf.len()))
        } else if start_time.elapsed() > MAX_WRITE_WAIT {
          error!(
            "dropping buf sized {}; couldn't send to blocked readers {:?} in time",
            buf.len(),
            blocked_readers,
          );
          Poll::Ready(Ok(buf.len()))
        } else {
          debug!(
            "AGAIN: couldn't send buf sized {} to blocked readers {:?}, pushing to remainder",
            buf.len(),
            blocked_readers
          );
          assert!(self_ref.remainder.is_none());
          let _ = self_ref
            .remainder
            .insert((buf, start_time, blocked_readers));
          Poll::Pending
        }
      } else {
        let start_time = Instant::now();
        let buf = Bytes::copy_from_slice(buf);
        let blocked_readers: IndexSet<ReadStreamId> = tees
          .par_iter()
          .filter_map(|(read_id, sender)| {
            sender.try_send(buf.clone()).err().and_then(|e| match e {
              mpsc::error::TrySendError::Closed(_) => {
                debug!("receiver disconnected, which is fine");
                None
              },
              mpsc::error::TrySendError::Full(_) => Some(*read_id),
            })
          })
          .collect();
        if !blocked_readers.is_empty() {
          debug!(
            "couldn't send buf sized {} to blocked readers {:?}, pushing to remainder",
            buf.len(),
            blocked_readers
          );
          assert!(self_ref.remainder.is_none());
          let _ = self_ref
            .remainder
            .insert((buf, start_time, blocked_readers));
          Poll::Pending
        } else {
          Poll::Ready(Ok(buf.len()))
        }
      }
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
      let self_ref = Pin::into_inner(self);
      let tees = self_ref.tees.read();
      if tees.is_empty() {
        /* If we had a remainder, kill it. We have no target streams now. */
        if self_ref.remainder.take().is_some() {
          warn!("non-empty remainder was deleted after output tees all disappeared");
        }

        /* If no outputs, then writes are a no-op. */
        return Poll::Ready(Ok(()));
      }

      /* FIXME: remove this duplicated logic. Also add logic to drop a listener from the muxing if
       * they keep reading too slowly. */
      if let Some((buf, start_time, blocked_readers)) = self_ref.remainder.take() {
        let blocked_readers: IndexSet<ReadStreamId> = blocked_readers
          .into_par_iter()
          .filter_map(|read_id| {
            let sender = match tees.get(&read_id) {
              Some(sender) => sender,
              None => {
                warn!(
                  "remainder read id {:?} was deleted since last poll",
                  read_id
                );
                return None;
              },
            };
            sender.try_send(buf.clone()).err().and_then(|e| match e {
              mpsc::error::TrySendError::Closed(_) => {
                debug!("receiver disconnected, which is fine");
                None
              },
              mpsc::error::TrySendError::Full(_) => Some(read_id),
            })
          })
          .collect();
        if blocked_readers.is_empty() {
          Poll::Ready(Ok(()))
        } else if start_time.elapsed() > MAX_WRITE_WAIT {
          error!(
            "dropping buf sized {}; couldn't send to blocked readers {:?} in time",
            buf.len(),
            blocked_readers,
          );
          Poll::Ready(Ok(()))
        } else {
          debug!(
            "AGAIN: couldn't send buf sized {} to blocked readers {:?}, pushing to remainder",
            buf.len(),
            blocked_readers
          );
          assert!(self_ref.remainder.is_none());
          let _ = self_ref
            .remainder
            .insert((buf, start_time, blocked_readers));
          Poll::Pending
        }
      } else {
        Poll::Ready(Ok(()))
      }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
      /* FIXME: do more work here if we retain more than a single frame across all
       * targets in self.remainder. */
      self.poll_flush(cx)
    }
  }

  impl WriteStream for WriteMux {}

  impl Drop for WriteMux {
    fn drop(&mut self) {
      let Self {
        id, tees, index, ..
      } = self;
      /* NB: This needs to run before .remove_writer() to ensure that the StreamMux
       * handle has the only Arc<RwLock<IndexMap<...>>> handle so it can call
       * Arc::into_inner(). */
      let _ = mem::replace(tees, Arc::new(RwLock::new(IndexMap::new())));
      index.write().remove_writer(*id);
    }
  }

  struct ListenerContext {
    /* This needs to be evaluated every time a new reader comes online to see if it matches. */
    pub query: Query,
    /* This needs to be cloned every time a new reader comes online and matches the query, even
     * if there were no matching readers when the listener came online. */
    pub sender: mpsc::Sender<Payload>,
    /* This is needed for cleanup when a listener is dropped. */
    pub live_inputs: Arc<RwLock<IndexSet<WriteStreamId>>>,
  }

  struct WriterContext {
    /* This needs to be evaluated every time a new listener comes online to see if it matches. */
    pub name: Name,
    /* This is accessed in the hot write path. If there are no matching listeners, the output
     * will be discarded (like redirecting to /dev/null). */
    pub live_outputs: Arc<RwLock<IndexMap<ReadStreamId, mpsc::Sender<Payload>>>>,
  }

  pub(crate) struct StreamMux {
    /* This is the "reverse index", only used for bookkeeping when adding and removing r/w
     * streams. */
    listeners: IndexMap<ReadStreamId, ListenerContext>,
    /* This is used in write-heavy code paths for output teeing. */
    writers: IndexMap<WriteStreamId, WriterContext>,
    query_match_index: QueryMatchIndex<Name, Query>,
  }

  impl StreamMux {
    pub fn new() -> Self {
      Self {
        listeners: IndexMap::new(),
        writers: IndexMap::new(),
        query_match_index: QueryMatchIndex::new(),
      }
    }

    fn find_matching_writers(&self, query: &Query) -> IndexSet<WriteStreamId> {
      /* TODO: use some caching to avoid a linear scan each time? */
      self
        .writers
        .par_iter()
        .filter_map(|(writer_id, WriterContext { name, .. })| {
          if self
            .query_match_index
            .evaluate(Cow::Borrowed(query), Cow::Borrowed(name))
          {
            Some(*writer_id)
          } else {
            None
          }
        })
        .collect()
    }

    pub(crate) fn add_new_listener(
      &mut self,
      query: Query,
      sender: mpsc::Sender<Payload>,
    ) -> ReadStreamId {
      let new_read_id = ReadStreamId::new();

      /* (1) Add all the write targets teeing to this read stream into the reverse
       * index. */
      let write_targets = self.find_matching_writers(&query);
      assert!(self
        .listeners
        .insert(new_read_id, ListenerContext {
          query,
          sender: sender.clone(),
          live_inputs: Arc::new(RwLock::new(write_targets.clone())),
        })
        .is_none());

      /* (2) Add this read stream to all the write streams' tee outputs. */
      write_targets.into_par_iter().for_each(|target| {
        let live_outputs: Arc<RwLock<IndexMap<ReadStreamId, mpsc::Sender<Payload>>>> = self
          .writers
          .get(&target)
          .map(|WriterContext { live_outputs, .. }| live_outputs.clone())
          .expect("writer stream id should already be registered");
        assert!(live_outputs
          .write()
          .insert(new_read_id, sender.clone())
          .is_none());
      });
      new_read_id
    }

    pub(crate) fn remove_listener(&mut self, s: ReadStreamId) {
      let ListenerContext { live_inputs, .. } =
        self.listeners.remove(&s).expect("listener id not found");

      let final_inputs: IndexSet<WriteStreamId> = Arc::into_inner(live_inputs)
        .expect("should be the only handle to live_inputs data")
        .into_inner();

      final_inputs.into_par_iter().for_each(|target| {
        let live_outputs: Arc<RwLock<IndexMap<ReadStreamId, mpsc::Sender<Payload>>>> = self
          .writers
          .get(&target)
          .map(|WriterContext { live_outputs, .. }| live_outputs.clone())
          .expect("writer should exist");
        assert!(
          live_outputs.write().remove(&s).is_some(),
          "this writer was expected to have pointed to the reader to remove"
        );
      });
    }

    fn find_matching_listeners(
      &self,
      name: &Name,
    ) -> IndexMap<ReadStreamId, mpsc::Sender<Payload>> {
      self
        .listeners
        .par_iter()
        .filter_map(|(listener_id, ListenerContext { query, sender, .. })| {
          if self
            .query_match_index
            .evaluate(Cow::Borrowed(query), Cow::Borrowed(name))
          {
            Some((*listener_id, sender.clone()))
          } else {
            None
          }
        })
        .collect()
    }

    pub(crate) fn add_new_writer(&mut self, name: Name) -> WriteStreamId {
      let new_write_id = WriteStreamId::new();

      let current_listeners = self.find_matching_listeners(&name);
      let listener_ids: Vec<_> = current_listeners.keys().cloned().collect();

      assert!(self
        .writers
        .insert(new_write_id, WriterContext {
          name,
          live_outputs: Arc::new(RwLock::new(current_listeners)),
        })
        .is_none());

      listener_ids.into_par_iter().for_each(|target| {
        let live_inputs: Arc<RwLock<IndexSet<WriteStreamId>>> = self
          .listeners
          .get(&target)
          .map(|ListenerContext { live_inputs, .. }| live_inputs.clone())
          .expect("reader stream id should already be registered");
        assert!(
          live_inputs.write().insert(new_write_id),
          "this write target is new, so should not have been registered to any read targets yet",
        );
      });

      new_write_id
    }

    pub(crate) fn remove_writer(&mut self, t: WriteStreamId) {
      let WriterContext { live_outputs, .. } =
        self.writers.remove(&t).expect("writer id not found");

      /* Simply drop the senders, as they do not expose any explicit EOF signal. */
      let final_outputs: IndexMap<ReadStreamId, _> = Arc::into_inner(live_outputs)
        .expect("should be the only handle to live_outputs data")
        .into_inner();

      final_outputs.par_keys().for_each(|target| {
        let live_inputs: Arc<RwLock<IndexSet<WriteStreamId>>> = self
          .listeners
          .get(target)
          .map(|ListenerContext { live_inputs, .. }| live_inputs.clone())
          .expect("reader should exist");
        assert!(
          live_inputs.write().remove(&t),
          "this reader was expected to have pointed to the writer to remove"
        );
      });
    }

    pub(crate) fn live_outputs_for_writer(
      &self,
      t: &WriteStreamId,
    ) -> Arc<RwLock<IndexMap<ReadStreamId, mpsc::Sender<Payload>>>> {
      let WriterContext { live_outputs, .. } = self
        .writers
        .get(t)
        .expect("writer id not found for live outputs");
      live_outputs.clone()
    }
  }

  ///```
  /// use workunits::streams::{indexing, mux::{self, ListenStream, Streamer}};
  /// use std::io::{Read, Write};
  ///
  /// let mux_handle = mux::MuxHandle::new();
  /// let query = indexing::Query("exactly-this-name".to_string());
  /// let mut listener = mux_handle.open_listener(query);
  /// let name = indexing::Name("exactly-this-name".to_string());
  /// let mut writer = mux_handle.open_writer(name);
  ///
  /// writer.write_all(b"hey").unwrap();
  /// listener.signal_end();
  /// let mut msg: String = String::new();
  /// listener.read_to_string(&mut msg).unwrap();
  ///
  /// assert!(&msg == "hey");
  /// ```
  pub struct MuxHandle {
    mux: Arc<RwLock<StreamMux>>,
  }

  impl MuxHandle {
    pub fn new() -> Self {
      Self {
        mux: Arc::new(RwLock::new(StreamMux::new())),
      }
    }
  }

  /* TODO: how to set this? */
  const CHANNEL_SIZE: usize = 50;

  impl Streamer for MuxHandle {
    type Listener = ListenMux;
    type Name = Name;
    type Query = Query;
    type Writer = WriteMux;

    fn open_listener(&self, query: Self::Query) -> Self::Listener {
      let (sender, receiver) = mpsc::channel::<Payload>(CHANNEL_SIZE);
      let id = self.mux.write().add_new_listener(query, sender);
      ListenMux::new(id, receiver, self.mux.clone())
    }

    fn open_writer(&self, name: Self::Name) -> Self::Writer {
      let id = self.mux.write().add_new_writer(name);
      let live_outputs = self.mux.read().live_outputs_for_writer(&id);
      WriteMux::new(id, live_outputs, self.mux.clone())
    }
  }
}

#[cfg(test)]
mod test {
  use super::{
    indexing,
    mux::{self, ListenStream, Streamer},
  };

  #[test]
  fn interleaved() {
    use std::io::{Read, Write};

    let mux_handle = mux::MuxHandle::new();

    let mut listener1 = mux_handle.open_listener(indexing::Query("asdf".to_string()));
    let mut other_listener = mux_handle.open_listener(indexing::Query("bbb".to_string()));

    let mut listener2 = {
      let mut writer1 = mux_handle.open_writer(indexing::Name("asdf".to_string()));
      writer1.write_all(b"wow").unwrap();

      /* This should not interact with the "asdf" streams at all. */
      let mut other_writer = mux_handle.open_writer(indexing::Name("bbb".to_string()));
      other_writer.write_all(b"isolated").unwrap();

      let listener2 = mux_handle.open_listener(indexing::Query("asdf".to_string()));
      {
        let mut writer2 = mux_handle.open_writer(indexing::Name("asdf".to_string()));
        writer2.write_all(b"hey").unwrap();
      }

      writer1.write_all(b"woah").unwrap();

      listener2
    };

    let mut buf1 = [0u8; 10];
    listener1.read_exact(&mut buf1).unwrap();
    assert_eq!(&buf1, b"wowheywoah");
    /* All matching listeners should receive a copy of all the data sent by
     * matching writers while the listener was registered, in the exact same
     * order. */
    let mut buf2 = [0u8; 7];
    listener2.read_exact(&mut buf2).unwrap();
    assert_eq!(&buf2, b"heywoah");

    let mut other_buf = [0u8; 8];
    other_listener.read_exact(&mut other_buf).unwrap();
    assert_eq!(&other_buf, b"isolated");
  }

  #[test]
  fn signal_end() {
    use std::io::{Read, Write};

    let mux_handle = mux::MuxHandle::new();

    let mut listener = mux_handle.open_listener(indexing::Query("asdf".to_string()));

    let mut writer = mux_handle.open_writer(indexing::Name("asdf".to_string()));
    writer.write_all(b"wow").unwrap();

    /* Without this, the call to .read_to_end() would never return. */
    listener.signal_end();

    let mut buf: Vec<u8> = Vec::new();
    listener.read_to_end(&mut buf).unwrap();
    assert_eq!(&buf, b"wow");
  }

  #[tokio::test]
  async fn async_streams() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let mux_handle = mux::MuxHandle::new();

    let mut listener = mux_handle.open_listener(indexing::Query("asdf".to_string()));

    let mut writer = mux_handle.open_writer(indexing::Name("asdf".to_string()));
    writer.write_all(b"wow").await.unwrap();

    listener.signal_end();

    let mut buf: Vec<u8> = Vec::new();
    listener.read_to_end(&mut buf).await.unwrap();
    assert_eq!(&buf, b"wow");
  }
}
