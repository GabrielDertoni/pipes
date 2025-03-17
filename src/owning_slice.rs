#![allow(dead_code)]

use std::mem::MaybeUninit;

use crate::PushErr;

// TODO: swap head and tail meanings
pub struct OwningSlice<'a, T> {
    // TODO: change to a smallvec of slices
    slice: &'a mut [MaybeUninit<T>],
    // INVARIANT: head >= tail
    // INVARIANT: slice[tail..head] is initialized
    head: usize,
    tail: usize,
}

impl<T: 'static> OwningSlice<'static, T> {
    pub(crate) fn empty() -> Self {
        OwningSlice {
            slice: &mut [] as &mut [MaybeUninit<T>],
            head: 0,
            tail: 0,
        }
    }
}

impl<'a, T> OwningSlice<'a, T> {
    pub(crate) fn new_unfilled(slice: &'a mut [MaybeUninit<T>]) -> Self {
        OwningSlice {
            slice,
            head: 0,
            tail: 0,
        }
    }

    pub(crate) fn new_filled(slice: &'a mut [T]) -> Self {
        let head = slice.len();
        OwningSlice {
            slice: unsafe { &mut *(slice as *mut [T] as *mut [MaybeUninit<T>]) },
            head,
            tail: 0,
        }
    }

    pub(crate) fn unfilled(&self) -> usize {
        self.capacity() - self.len()
    }

    pub(crate) fn len(&self) -> usize {
        self.head - self.tail
    }

    pub(crate) fn capacity(&self) -> usize {
        self.slice.len()
    }

    pub(crate) fn get(&self, index: usize) -> Option<&T> {
        let offset = index + self.tail;
        if self.init_range().contains(&offset) {
            unsafe { Some(self.slice.get_unchecked(offset).assume_init_ref()) }
        } else {
            None
        }
    }

    pub(crate) fn get_mut(&mut self, index: usize) -> Option<&mut T> {
        let offset = index + self.tail;
        if self.init_range().contains(&offset) {
            unsafe { Some(self.slice.get_unchecked_mut(offset).assume_init_mut()) }
        } else {
            None
        }
    }

    pub(crate) fn push_back(&mut self, item: T) -> Result<(), PushErr<T>> {
        let head = self.head;
        let Some(ptr) = self.slice.get_mut(head) else {
            return Err(PushErr(item));
        };
        ptr.write(item);
        self.head += 1;
        Ok(())
    }

    pub(crate) fn pop_front(&mut self) -> Option<T> {
        if self.tail == self.head {
            return None;
        }
        let tail = self.tail;
        self.tail += 1;
        unsafe { Some(self.slice.get_unchecked(tail).assume_init_read()) }
    }

    pub(crate) fn init_slice(&self) -> &[T] {
        let range = self.init_range();
        unsafe { &*((&self.slice[range]) as *const [MaybeUninit<T>] as *const [T]) }
    }
}

impl<'a, T> OwningSlice<'a, T> {
    fn init_range(&self) -> std::ops::Range<usize> {
        self.tail..self.head
    }
}

unsafe impl<T> Send for OwningSlice<'_, T> {}
