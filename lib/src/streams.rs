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

  pub trait Matches {
    type Key;
    fn matches(&self, key: &Self::Key) -> bool;
  }

  /* TODO: enable namespacing/prefixing/tagging. */
  /// <stream name: {0}>
  #[derive(Clone, Eq, PartialEq, Debug, Display)]
  pub struct Name(pub String);

  impl Name {
    pub fn new(s: String) -> Self { Self(s) }
  }

  /// <stream name query: {0}>
  #[derive(Clone, Eq, PartialEq, Debug, Display)]
  pub struct Query(pub String);

  impl Query {
    pub fn new(s: String) -> Self { Self(s) }
  }

  impl Matches for Query {
    type Key = Name;

    /* TODO: make this match more complex expressions, etc. */
    fn matches(&self, name: &Name) -> bool { self.0 == name.0 }
  }
}

pub mod mux {
  use super::{
    indexing::{Matches, Name, Query},
    referencing::{ReadStreamId, WriteStreamId},
  };

  use bytes::Bytes;
  use indexmap::{IndexMap, IndexSet};
  use log::debug;
  use parking_lot::RwLock;

  use std::{
    collections::VecDeque,
    hash::Hash,
    io, mem,
    sync::{
      atomic::{AtomicBool, Ordering},
      mpsc, Arc,
    },
  };

  pub trait StreamID {
    type ID: Eq+Hash+Copy;
    fn id(&self) -> Self::ID;
  }

  pub type Payload = Bytes;

  pub trait ListenStream: StreamID+io::Read {
    fn signal_end(&self);
  }

  pub trait WriteStream: StreamID+io::Write {}

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
          Err(mpsc::TryRecvError::Disconnected) => unreachable!(),
          /* If we've been signalled that it's all empty, then we can signal EOF. */
          Err(mpsc::TryRecvError::Empty) => {
            return Ok(0);
          },
          Ok(payload) => payload,
        }
      } else {
        /* Otherwise, we wait to pull in anything (we *don't* wait to fill the whole
         * buffer). */
        match self.recv.recv() {
          /* This occurs when all senders disconnect, which should never happen because we always
           * cache an extra SyncSender in ListenerContext. */
          Err(mpsc::RecvError) => unreachable!(),
          Ok(payload) => payload,
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

  impl ListenStream for ListenMux {
    fn signal_end(&self) { self.end_signal.store(true, Ordering::Release); }
  }

  impl Drop for ListenMux {
    fn drop(&mut self) { self.index.write().remove_listener(self.id()); }
  }

  pub struct WriteMux {
    id: WriteStreamId,
    tees: Arc<RwLock<IndexMap<ReadStreamId, mpsc::SyncSender<Payload>>>>,
    index: Arc<RwLock<StreamMux>>,
  }

  impl WriteMux {
    pub(crate) fn new(
      id: WriteStreamId,
      tees: Arc<RwLock<IndexMap<ReadStreamId, mpsc::SyncSender<Payload>>>>,
      index: Arc<RwLock<StreamMux>>,
    ) -> Self {
      Self { id, tees, index }
    }
  }

  impl StreamID for WriteMux {
    type ID = WriteStreamId;

    fn id(&self) -> Self::ID { self.id }
  }

  /// It's recommended to wrap this in a buffered writer as well to work around
  /// blocking.
  impl io::Write for WriteMux {
    /// A write() which always succeeds in full, but may block when writing to
    /// bounded channels.
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
      if buf.is_empty() {
        return Ok(0);
      }

      /* A Bytes is cheaply cloneable, so it can be sent to all teed outputs
       * without additional copies. */
      let buf = Bytes::copy_from_slice(buf);
      /* TODO: rayon/etc */
      for sender in self.tees.read().values() {
        #[allow(clippy::single_match_else)]
        match sender.send(buf.clone()) {
          Ok(()) => (),
          /* This only occurs if the receiver has hung up in between when we read from self.tees
           * and this .send() call (see https://doc.rust-lang.org/std/sync/mpsc/struct.SendError.html). In this case, we log that this occurred in case it signal an
           * issue but otherwise ignore it. */
          Err(mpsc::SendError(_)) => debug!("receiver disconnected, which is fine"),
        }
      }
      Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> { Ok(()) }
  }

  impl WriteStream for WriteMux {}

  impl Drop for WriteMux {
    fn drop(&mut self) {
      let Self { id, tees, index } = self;
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
    pub sender: mpsc::SyncSender<Payload>,
    /* This is needed for cleanup when a listener is dropped. */
    pub live_inputs: Arc<RwLock<IndexSet<WriteStreamId>>>,
  }

  struct WriterContext {
    /* This needs to be evaluated every time a new listener comes online to see if it matches. */
    pub name: Name,
    /* This is accessed in the hot write path. If there are no matching listeners, the output
     * will be discarded (like redirecting to /dev/null). */
    pub live_outputs: Arc<RwLock<IndexMap<ReadStreamId, mpsc::SyncSender<Payload>>>>,
  }

  pub(crate) struct StreamMux {
    /* This is the "reverse index", only used for bookkeeping when adding and removing r/w
     * streams. */
    listeners: IndexMap<ReadStreamId, ListenerContext>,
    /* This is used in write-heavy code paths for output teeing. */
    writers: IndexMap<WriteStreamId, WriterContext>,
  }

  impl StreamMux {
    pub fn new() -> Self {
      Self {
        listeners: IndexMap::new(),
        writers: IndexMap::new(),
      }
    }

    fn find_matching_writers<'a>(
      &'a self,
      query: &'a Query,
    ) -> impl Iterator<Item=WriteStreamId>+'a {
      /* TODO: rayon? */
      /* TODO: use some caching to avoid a linear scan each time? */
      self
        .writers
        .iter()
        .filter_map(|(writer_id, WriterContext { name, .. })| {
          if query.matches(name) {
            Some(*writer_id)
          } else {
            None
          }
        })
    }

    pub(crate) fn add_new_listener(
      &mut self,
      query: Query,
      sender: mpsc::SyncSender<Payload>,
    ) -> ReadStreamId {
      let new_read_id = ReadStreamId::new();
      /* (1) Add all the write targets teeing to this read stream into the reverse
       * index. */
      let write_targets: IndexSet<WriteStreamId> = self.find_matching_writers(&query).collect();
      assert!(self
        .listeners
        .insert(new_read_id, ListenerContext {
          query,
          sender: sender.clone(),
          live_inputs: Arc::new(RwLock::new(write_targets.clone())),
        })
        .is_none());
      /* (2) Add this read stream to all the write streams' tee outputs. */
      /* TODO: rayon? */
      for target in write_targets.into_iter() {
        let live_outputs: Arc<RwLock<IndexMap<ReadStreamId, mpsc::SyncSender<Payload>>>> = self
          .writers
          .get(&target)
          .map(|WriterContext { live_outputs, .. }| live_outputs.clone())
          .expect("writer stream id should already be registered");
        assert!(live_outputs
          .write()
          .insert(new_read_id, sender.clone())
          .is_none());
      }
      new_read_id
    }

    pub(crate) fn remove_listener(&mut self, s: ReadStreamId) {
      let ListenerContext { live_inputs, .. } =
        self.listeners.remove(&s).expect("listener id not found");
      let final_inputs: IndexSet<WriteStreamId> = Arc::into_inner(live_inputs)
        .expect("should be the only handle to live_inputs data")
        .into_inner();
      for target in final_inputs.into_iter() {
        let live_outputs: Arc<RwLock<IndexMap<ReadStreamId, mpsc::SyncSender<Payload>>>> = self
          .writers
          .get(&target)
          .map(|WriterContext { live_outputs, .. }| live_outputs.clone())
          .expect("writer should exist");
        assert!(
          live_outputs.write().remove(&s).is_some(),
          "this writer was expected to have pointed to the reader to remove"
        );
      }
    }

    fn find_matching_listeners<'a>(
      &'a self,
      name: &'a Name,
    ) -> impl Iterator<Item=(ReadStreamId, mpsc::SyncSender<Payload>)>+'a {
      self
        .listeners
        .iter()
        .filter_map(|(listener_id, ListenerContext { query, sender, .. })| {
          if query.matches(name) {
            Some((*listener_id, sender.clone()))
          } else {
            None
          }
        })
    }

    pub(crate) fn add_new_writer(&mut self, name: Name) -> WriteStreamId {
      let new_write_id = WriteStreamId::new();
      let current_listeners: IndexMap<ReadStreamId, mpsc::SyncSender<Payload>> =
        self.find_matching_listeners(&name).collect();
      let listener_ids: Vec<_> = current_listeners.keys().cloned().collect();

      assert!(self
        .writers
        .insert(new_write_id, WriterContext {
          name,
          live_outputs: Arc::new(RwLock::new(current_listeners)),
        })
        .is_none());

      for target in listener_ids.into_iter() {
        let live_inputs: Arc<RwLock<IndexSet<WriteStreamId>>> = self
          .listeners
          .get(&target)
          .map(|ListenerContext { live_inputs, .. }| live_inputs.clone())
          .expect("reader stream id should already be registered");
        assert!(
          live_inputs.write().insert(new_write_id),
          "this write target is new, so should not have been registered to any read targets yet",
        );
      }

      new_write_id
    }

    pub(crate) fn remove_writer(&mut self, t: WriteStreamId) {
      let WriterContext { live_outputs, .. } =
        self.writers.remove(&t).expect("writer id not found");
      /* Simply drop the senders, as they do not provide any explicit EOF signal. */
      let final_outputs: IndexMap<ReadStreamId, _> = Arc::into_inner(live_outputs)
        .expect("should be the only handle to live_outputs data")
        .into_inner();
      for target in final_outputs.into_keys() {
        let live_inputs: Arc<RwLock<IndexSet<WriteStreamId>>> = self
          .listeners
          .get(&target)
          .map(|ListenerContext { live_inputs, .. }| live_inputs.clone())
          .expect("reader should exist");
        assert!(
          live_inputs.write().remove(&t),
          "this reader was expected to have pointed to the writer to remove"
        );
      }
    }

    pub(crate) fn live_outputs_for_writer(
      &self,
      t: &WriteStreamId,
    ) -> Arc<RwLock<IndexMap<ReadStreamId, mpsc::SyncSender<Payload>>>> {
      let WriterContext { live_outputs, .. } = self
        .writers
        .get(t)
        .expect("writer id not found for live outputs");
      live_outputs.clone()
    }
  }

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
      let (sender, receiver) = mpsc::sync_channel::<Payload>(CHANNEL_SIZE);
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

  use std::io::{Read, Write};

  #[test]
  fn interleaved() {
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
    /* All matching listeners should receive a copy of all the data sent by matching writers while
     * the listener was registered, in the exact same order. */
    let mut buf2 = [0u8; 7];
    listener2.read_exact(&mut buf2).unwrap();
    assert_eq!(&buf2, b"heywoah");

    let mut other_buf = [0u8; 8];
    other_listener.read_exact(&mut other_buf).unwrap();
    assert_eq!(&other_buf, b"isolated");
  }

  #[test]
  fn signal_end() {
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
}
