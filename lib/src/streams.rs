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

use indexmap::{IndexMap, IndexSet};
use parking_lot::RwLock;

use std::sync::Arc;


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
use referencing::{ReadStreamId, WriteStreamId};

pub mod indexing {
  use displaydoc::Display;

  /// <stream name: {0}>
  #[derive(Clone, Eq, PartialEq, Debug, Display)]
  pub struct StreamName(String);

  impl StreamName {
    pub fn new(s: String) -> Self { Self(s) }
  }

  /// <stream name query: {0}>
  #[derive(Clone, Eq, PartialEq, Debug, Display)]
  pub struct Query(String);

  impl Query {
    pub fn new(s: String) -> Self { Self(s) }

    pub fn matches(&self, name: &StreamName) -> bool { self.0 == name }
  }
}
use indexing::{Query, StreamName};

pub struct Streamer {
  /* This is the "reverse index", only used for bookkeeping when adding and removing r/w
   * streams. */
  listeners: IndexMap<ReadStreamId, (Query, Arc<RwLock<IndexSet<WriteStreamId>>>)>,
  /* This is used in write-heavy code paths for output teeing. */
  writers: IndexMap<WriteStreamId, (StreamName, Arc<RwLock<IndexSet<ReadStreamId>>>)>,
}

impl Streamer {
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
      .filter_map(|(writer_id, (stream_name, _))| {
        if query.matches(stream_name) {
          Some(*writer_id)
        } else {
          None
        }
      })
  }

  pub fn add_new_listener(&mut self, s: ReadStreamId, query: Query) {
    /* (1) Add all the write targets teeing to this read stream into the reverse
     * index. */
    let write_targets: IndexSet<WriteStreamId> = self.find_matching_writers(&query).collect();
    if let Some(_) = self
      .listeners
      .insert(s, (query, Arc::new(RwLock::new(write_targets.clone()))))
    {
      unreachable!(
        "assumed listener stream id {:?} was not already registered",
        s
      );
    }
    /* (2) Add this read stream to all the write streams' tee outputs. */
    /* TODO: rayon? */
    for target in write_targets.into_iter() {
      let read_targets: Arc<RwLock<IndexSet<ReadStreamId>>> = self
        .writers
        .get(target)
        .map(|(_, read_targets)| read_targets.clone())
        .expect("writer stream id should already be registered");
      assert!(
        read_targets.write().insert(s),
        "this read target is new, so should not have been registered to any write targets yet"
      );
    }
  }

  pub fn remove_listener(&mut self, s: ReadStreamId) {
    let (_, writer_streams) = self.listeners.remove(&s).expect("listener id not found");
    let writer_streams: IndexSet<WriteStreamId> = Arc::into_inner(writer_streams)
      .expect("should be the only handle to this data")
      .into_inner();
    for target in writer_streams.into_iter() {
      let readers: Arc<RwLock<IndexSet<ReadStreamId>>> = self
        .writers
        .get(&target)
        .map(|(_, readers)| readers.clone())
        .expect("writer should exist");
      assert!(
        readers.write().remove(&s),
        "this reader was expected to have pointed to the writer to remove"
      );
    }
  }

  fn find_matching_listeners(&self, name: &StreamName) -> impl Iterator<Item=ReadStreamId> {
    self
      .listeners
      .iter()
      .filter_map(|(listener_id, (query, _))| {
        if query.matches(name) {
          Some(listener_id)
        } else {
          None
        }
      })
  }

  pub fn add_new_writer(&mut self, t: WriteStreamId, name: StreamName) {
    let listeners: IndexSet<ReadStreamId> = self.find_matching_listeners(&name).collect();
    if let Some(_) = self
      .writers
      .insert(t, (name, Arc::new(RwLock::new(listeners.clone()))))
    {
      unreachable!(
        "assumed writer stream id {:?} was not already registered",
        t
      );
    }
    for target in listeners.into_iter() {
      let write_targets: Arc<RwLock<IndexSet<WriteStreamId>>> = self
        .listeners
        .get(target)
        .map(|(_, write_targets)| write_targets.clone())
        .expect("reader stream id should already be registered");
      assert!(
        write_targets.write().insert(t),
        "this write target is new, so should not have been registered to any read targets yet",
      );
    }
  }
}
