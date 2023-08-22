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

/* These clippy lint descriptions are purely non-functional and do not affect the functionality
 * or correctness of the code. */
// #![warn(missing_docs)]

/* Note: run clippy with: rustup run nightly cargo-clippy! */
#![deny(unsafe_code)]
/* Ensure any doctest warnings fails the doctest! */
#![doc(test(attr(deny(warnings))))]
/* Enable all clippy lints except for many of the pedantic ones. It's a shame this needs to be
 * copied and pasted across crates, but there doesn't appear to be a way to include inner
 * attributes from a common source. */
#![deny(
  clippy::all,
  clippy::default_trait_access,
  clippy::expl_impl_clone_on_copy,
  clippy::if_not_else,
  clippy::needless_continue,
  clippy::single_match_else,
  clippy::unseparated_literal_suffix,
  clippy::used_underscore_binding
)]
/* It is often more clear to show that nothing is being moved. */
#![allow(clippy::match_ref_pats)]
/* Subjective style. */
#![allow(
  clippy::derived_hash_with_manual_eq,
  clippy::len_without_is_empty,
  clippy::redundant_field_names,
  clippy::too_many_arguments,
  clippy::single_component_path_imports,
  clippy::double_must_use
)]
/* Default isn't as big a deal as people seem to think it is. */
#![allow(clippy::new_without_default, clippy::new_ret_no_self)]
/* Arc<Mutex> can be more clear than needing to grok Orderings. */
#![allow(clippy::mutex_atomic)]

use async_trait::async_trait;
use uuid::Uuid;


#[derive(Clone, Eq, PartialEq, Debug)]
pub struct StreamTypeName(String);


#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub enum ContentType {
  Text,
  Binary,
}


#[derive(Clone)]
pub struct StreamSpec {
  pub type_name: StreamTypeName,
  pub content_type: ContentType,
}


#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub struct StreamId(Uuid);

impl StreamId {
  pub fn new() -> Self { Self(Uuid::new_v4()) }
}


#[derive(Clone, Debug)]
pub struct NamespacedStreamName {
  pub parents: Vec<StreamTypeName>,
  pub current: StreamTypeName,
}

impl NamespacedStreamName {
  /// Interpret a `.`-delimited string as a sequence of parents and a final
  /// current stream name.
  pub fn parse_spec(spec: &str) -> Result<Self, String>;
}


pub trait A {
  /// Register an atomic stream type by name and content.
  fn register_stream_type(&mut self, spec: StreamSpec);
  /// Anything written to this stream is teed to all matching listeners.
  fn open_workunit_write_stream(
    &mut self,
    type_name: StreamTypeName,
    parent: Option<StreamId>,
  ) -> (StreamId, WriteStreamType);
  /// When closed, this stream is removed from the tee listeners for all matching streams.
  fn listen_to_joined_stream(&mut self, name: NamespacedStreamName) -> ReadStreamType;
}


pub fn f(x: usize) -> usize { x + 3 }

#[cfg(test)]
mod test {
  use super::*;

  #[test]
  fn it_works() {
    assert_eq!(f(3), 6);
  }
}
