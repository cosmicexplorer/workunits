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


pub mod referencing {
  macro_rules! uuid_struct {
    ($name:id) => {
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
    pub fn matches(&self, name: &Name) -> bool { self.0 == name.0 }
  }
}

pub mod mux {
  use super::{
    indexing::{Matches, Name, Query},
    referencing::{ReadStreamId, WriteStreamId},
  };

  use indexmap::{IndexMap, IndexSet};
  use parking_lot::RwLock;

  use std::{
    io,
    sync::{mpsc, Arc},
  };

  pub trait StreamID {
    type ID: Eq+Hash+Copy;
    fn id(&self) -> Self::ID;
  }

  pub type Payload = Vec<u8>;

  pub trait ListenStream: StreamID+io::Read {}

  pub trait WriteStream: StreamID+io::Write {}

  pub trait Streamer {
    type Listener: ListenStream;
    type Query: Matches<Key=Self::Name>;
    fn open_listener(&mut self, query: Self::Query) -> Self::Listener;

    type Writer: WriteStream;
    type Name;
    fn open_writer(&mut self, name: Self::Name) -> Self::Writer;
  }

  pub struct ListenMux {
    id: ReadStreamId,
    recv: mpsc::Receiver<Payload>,
    index: Arc<RwLock<StreamMux>>,
  }

  impl ListenMux {
    pub(crate) fn new(
      id: ReadStreamId,
      recv: mpsc::Receiver<Payload>,
      index: Arc<RwLock<StreamMux>>,
    ) -> Self {
      Self { id, recv, index }
    }
  }

  impl StreamID for ListenMux {
    type ID = ReadStreamId;

    fn id(&self) -> Self::ID { *self.id }
  }

  impl io::Read for ListenMux {}

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

    fn id(&self) -> Self::ID { *self.id }
  }

  impl Drop for WriteMux {
    fn drop(&mut self) { self.index.write().remove_writer(self.id()); }
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

  struct StreamMux {
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

    fn find_matching_writers(&self, query: &Query) -> impl Iterator<Item=WriteStreamId> {
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
      if let Some(_) = self.listeners.insert(new_read_id, ListenerContext {
        query,
        sender: sender.clone(),
        live_inputs: Arc::new(RwLock::new(write_targets.clone())),
      }) {
        unreachable!(
          "assumed listener stream id {:?} was not already registered",
          new_read_id
        );
      }
      /* (2) Add this read stream to all the write streams' tee outputs. */
      /* TODO: rayon? */
      for target in write_targets.into_iter() {
        let live_outputs: Arc<RwLock<IndexMap<ReadStreamId, mpsc::SyncSender<Payload>>>> = self
          .writers
          .get(target)
          .map(|WriterContext { live_outputs, .. }| live_outputs.clone())
          .expect("writer stream id should already be registered");
        if let Some(_) = live_outputs.write().insert(new_read_id, sender.clone()) {
          unreachable!(
            "this read target is new, so should not have been registered to any write targets yet"
          );
        }
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

    fn find_matching_listeners(
      &self,
      name: &Name,
    ) -> impl Iterator<Item=(ReadStreamId, mpsc::SyncSender<Payload>)> {
      self
        .listeners
        .iter()
        .filter_map(|(listener_id, ListenerContext { query, sender, .. })| {
          if query.matches(name) {
            Some((listener_id, sender.clone()))
          } else {
            None
          }
        })
    }

    pub(crate) fn add_new_writer(&mut self, name: Name) -> WriteStreamId {
      let new_write_id = WriteStreamId::new();
      let current_listeners: IndexMap<ReadStreamId, mpsc::SyncSender<Payload>> =
        self.find_matching_listeners(&name).collect();
      let listener_ids: Vec<_> = current_listeners.keys().collect();

      if let Some(_) = self.writers.insert(new_write_id, WriterContext {
        name,
        live_outputs: Arc::new(RwLock::new(current_listeners)),
      }) {
        unreachable!(
          "assumed writer stream id {:?} was not already registered",
          new_write_id
        );
      }

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
      for targets in final_outputs.into_keys() {
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

  const CHANNEL_SIZE: usize = 50;

  impl Streamer for MuxHandle {
    type Listener = ListenMux;
    type Name = Name;
    type Query = Query;
    type Writer = WriteMux;

    fn open_listener(&mut self, query: Self::Query) -> Self::Listener {
      let (sender, receiver) = mpsc::sync_channel::<Payload>(CHANNEL_SIZE);
      let id = self.mux.write().add_new_listener(query, sender);
      ListenMux::new(id, receiver, self.mux.clone())
    }

    fn open_writer(&mut self, name: Self::Name) -> Self::Writer {
      let id = self.mux.write().add_new_writer(name);
      let live_outputs = self.mux.read().live_outputs_for_writer(&id);
      WriteMux::new(id, live_outputs, self.mux.clone())
    }
  }
}
