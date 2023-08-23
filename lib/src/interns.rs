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

use indexmap::IndexSet;
use parking_lot::{lock_api::RwLockUpgradableReadGuard, RwLock};

use std::{
  borrow::{Borrow, Cow, ToOwned},
  hash::Hash,
  marker::PhantomData,
  sync::Arc,
};


pub trait InternHandle: Hash+Eq+Ord+Copy {}

trait MutableInternTable {
  type Value: Hash+Eq;
  type Handle: InternHandle;

  fn intern_soft(&self, v: &Self::Value) -> Option<Self::Handle>;
  fn intern_hard(&mut self, v: Self::Value) -> Self::Handle;
}

struct MutableIndex<Value, Handle> {
  set: IndexSet<Value>,
  _h: PhantomData<Handle>,
}

impl<Value, Handle> MutableIndex<Value, Handle> {
  pub fn new() -> Self {
    Self {
      set: IndexSet::new(),
      _h: PhantomData,
    }
  }
}

impl<Value, Handle> MutableInternTable for MutableIndex<Value, Handle>
where
  Value: Hash+Eq,
  Handle: InternHandle+From<usize>,
{
  type Handle = Handle;
  type Value = Value;

  fn intern_soft(&self, v: &Self::Value) -> Option<Self::Handle> {
    self.set.get_index_of(v).map(Handle::from)
  }

  fn intern_hard(&mut self, v: Self::Value) -> Self::Handle {
    let (idx, _) = self.set.insert_full(v);
    Handle::from(idx)
  }
}

pub trait InternTable {
  type Value: Hash+Eq+ToOwned;
  type Handle: InternHandle;

  fn intern_soft(&self, v: &Self::Value) -> Option<Self::Handle>;
  fn intern(&self, v: Cow<'_, Self::Value>) -> Self::Handle;
}

#[derive(Clone)]
pub struct Index<Value, Handle> {
  inner: Arc<RwLock<MutableIndex<Value, Handle>>>,
}

impl<Value, Handle> Index<Value, Handle> {
  pub fn new() -> Self {
    Self {
      /* TODO: MutableIndex<> is supposed to be Send??? And unsafe impling it by hand doesn't
       * make this go away?? */
      #[allow(clippy::arc_with_non_send_sync)]
      inner: Arc::new(RwLock::new(MutableIndex::new())),
    }
  }
}

impl<Value, Handle> InternTable for Index<Value, Handle>
where
  Value: Hash+Eq+ToOwned<Owned=Value>,
  Handle: InternHandle+From<usize>,
{
  type Handle = Handle;
  type Value = Value;

  fn intern_soft(&self, v: &Self::Value) -> Option<Self::Handle> {
    self.inner.read().intern_soft(v)
  }

  fn intern(&self, v: Cow<'_, Self::Value>) -> Self::Handle {
    /* Upgradable reads are mutually exclusive with other upgradable reads (see
     * https://morestina.net/blog/1739/upgradable-parking_lotrwlock-might-not-be-what-you-expect),
     * so we expose .intern_soft() to keep the fast path when applicable. */
    let read_guard = self.inner.upgradable_read();
    if let Some(handle) = read_guard.intern_soft(v.borrow()) {
      return handle;
    }
    RwLockUpgradableReadGuard::<'_, _, _>::upgrade(read_guard).intern_hard(v.into_owned())
  }
}


#[cfg(test)]
mod test {
  use super::{Index, InternHandle, InternTable};

  use std::borrow::Cow;

  #[test]
  fn make_table() {
    #[derive(Hash, Eq, PartialEq, Ord, PartialOrd, Copy, Clone)]
    struct Id(usize);
    impl InternHandle for Id {}
    impl From<usize> for Id {
      fn from(x: usize) -> Self { Self(x) }
    }

    #[derive(Hash, Eq, PartialEq, Clone)]
    struct Name(pub String);

    let index = Index::<Name, Id>::new();

    let name1 = Name("asdf".to_string());
    let name2 = Name("bbb".to_string());

    let id1 = index.intern(Cow::Owned(name1.clone()));
    let id_ref1 = index.intern(Cow::Borrowed(&name1));
    assert!(id1 == id_ref1);
    let id_ref2 = index.intern_soft(&name1);
    assert!(Some(id1) == id_ref2);
    assert!(index.intern_soft(&name2).is_none());
    let id2 = index.intern(Cow::Owned(name2));
    assert!(id1 != id2);
  }
}
