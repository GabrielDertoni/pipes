use std::mem::MaybeUninit;
use std::ops::Range;
use std::ptr::NonNull;
use std::sync::{
    Arc,
    atomic::{AtomicU32, Ordering},
};
use std::task::{Context, Poll, ready};

use crossbeam_utils::CachePadded;
use slice_dst::SliceWithHeader;

use crate::ctrl::Config;
use crate::owning_slice::OwningSlice;
use crate::{
    PipeReader, PipeReaderImpl, PipeWriter, PipeWriterImpl, ReadBatch, ReadBatchCallback,
    WriteBatch, WriteBatchCallback,
};
use crate::{PushErr, ctrl::Ctrl};

pub fn pipe<T: Send + 'static>(buf_size: usize, config: Config) -> (PipeReader<T>, PipeWriter<T>) {
    assert!(buf_size < u32::MAX as usize);
    let buf = RingBuf::new(buf_size as u32);
    let ctrl = Arc::new(Ctrl::new(config));
    let (writer, reader) = buf.split();
    (
        PipeReader::new(Box::new(RingPipeReader {
            ctrl: Arc::clone(&ctrl),
            buf: reader,
            synced: 0,
        })),
        PipeWriter::new(Box::new(RingPipeWriter {
            ctrl,
            buf: writer,
            flushed: 0,
        })),
    )
}

pub struct RingPipeWriter<T> {
    ctrl: Arc<Ctrl>,
    buf: Writer<T>,
    flushed: usize,
}

impl<T: Send + 'static> PipeWriterImpl for RingPipeWriter<T> {
    type Item = T;

    fn batch<'a>(&'a mut self, min_capacity: usize) -> WriteBatch<'a, Self::Item> {
        // TODO: use min_capacity
        let slice = self.buf.writable_slice();
        assert!(min_capacity <= slice.unfilled());
        let produced = self.ctrl.lock().produced();
        WriteBatch::new(produced, slice, &mut self.ctrl)
    }

    fn poll_flush(&mut self, cx: &mut Context) -> Poll<bool> {
        let mut ctrl = self.ctrl.lock();
        unsafe { self.buf.advance((ctrl.produced() - self.flushed) as u32) }
        self.flushed = ctrl.produced();
        self.buf.sync();
        ctrl.poll_wait_to_produce(cx)
    }
}

impl<T> Drop for RingPipeWriter<T> {
    fn drop(&mut self) {
        self.ctrl.lock().set_producer_complete();
    }
}

impl WriteBatchCallback for Arc<Ctrl> {
    fn produce_to(&mut self, produced: usize) {
        // println!("produce_to: {produced}");
        self.lock().produce_to(produced);
    }
}

pub struct RingPipeReader<T> {
    ctrl: Arc<Ctrl>,
    buf: Reader<T>,
    synced: usize,
}

impl<T: Send + 'static> PipeReaderImpl for RingPipeReader<T> {
    type Item = T;

    fn poll_next_batch<'a>(
        &'a mut self,
        // TODO: use min_size
        _min_size: usize,
        cx: &mut Context,
    ) -> Poll<ReadBatch<'a, T>> {
        let consumed;
        let examined;
        let done;
        {
            let mut ctrl = self.ctrl.lock();
            consumed = ctrl.consumed();
            examined = ctrl.examined();
            done = ready!(ctrl.poll_wait_to_consume(cx));
        }
        unsafe {
            self.buf.advance((consumed - self.synced) as u32);
        }
        self.buf.sync();
        Poll::Ready(ReadBatch::new(
            consumed,
            examined,
            done,
            self.buf.readable_slice(),
            &mut self.ctrl,
        ))
    }
}

impl<T> Drop for RingPipeReader<T> {
    fn drop(&mut self) {
        self.ctrl.lock().set_consumer_complete();
    }
}

impl ReadBatchCallback for Arc<Ctrl> {
    fn advance_to(&mut self, consumed: usize, examined: usize) {
        // println!("consumed: {consumed}, examined: {examined}");
        self.lock().advance_to(consumed, examined);
    }
}

pub struct RingBuf<T> {
    // This represents a unique Arc reference
    inner: Arc<Inner<T>>,
}

impl<T> RingBuf<T> {
    pub fn new(capacity: u32) -> Self {
        assert_eq!((capacity & (capacity - 1)), 0, "must be power of 2");
        // If capacity is allowed to be u32::MAX, there would be a situation
        // where, with a full buffer, it wouldn't be possible to tell whether
        // the buffer is filled or empty.
        assert!(
            capacity < u32::MAX,
            "capacity must be smaller than u32::MAX"
        );
        RingBuf {
            inner: unsafe { Inner::new_unchecked(capacity) },
        }
    }

    pub fn split(mut self) -> (Writer<T>, Reader<T>) {
        let inner = Arc::get_mut(&mut self.inner).unwrap();
        let header = LocalHeader::load_exclusive(&mut inner.0.header);
        (
            Writer {
                header,
                inner: Arc::clone(&self.inner),
            },
            Reader {
                header,
                inner: self.inner,
            },
        )
    }
}

unsafe impl<T> Send for RingBuf<T> {}

pub struct Writer<T> {
    header: LocalHeader,
    inner: Arc<Inner<T>>,
}

impl<T> Writer<T> {
    pub fn push(&mut self, item: T) -> Result<(), PushErr<T>> {
        if self.remaining() == 0 {
            return Err(PushErr(item));
        }
        unsafe { self.push_unchecked(item) }
        Ok(())
    }

    /// # Safety
    /// The caller must ensure there is enough capacity left for the item to be pushed. That is, `remaining() > 0`.
    pub unsafe fn push_unchecked(&mut self, item: T) {
        unsafe {
            self.inner
                .slice_ptr()
                .add(self.header.tail() as usize)
                .write(item);
        }
        self.header.tail = self.header.tail.wrapping_add(1);
    }

    pub fn writable_slice<'a>(&'a mut self) -> OwningSlice<'a, T> {
        let head = self.header.head() as usize;
        let tail = self.header.tail() as usize;
        let ptr = unsafe { self.inner.uninit_slice_ptr().add(tail) };
        let mut slice_ptr = if tail >= head && self.header.unfilled() > 0 {
            NonNull::slice_from_raw_parts(ptr, self.header.capacity() as usize - tail)
        } else {
            NonNull::slice_from_raw_parts(ptr, head - tail)
        };
        OwningSlice::new_unfilled(unsafe { slice_ptr.as_mut() })
    }

    /// # Safety
    /// The caller must ensure `n <= self.remaining()` and that the next `n` items were initialized in the buffer sequence.
    pub unsafe fn advance(&mut self, n: u32) {
        assert!(n <= self.remaining());
        self.header.tail = self.header.tail.wrapping_add(n);
    }

    pub fn sync(&mut self) {
        let shared = self.inner.header();
        shared.tail.store(self.header.tail, Ordering::Release);
        self.header.head = shared.head.load(Ordering::Acquire);
    }

    pub fn len(&self) -> u32 {
        self.header.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn remaining(&self) -> u32 {
        self.header.unfilled()
    }
}

unsafe impl<T> Send for Writer<T> {}

impl<T> Drop for Writer<T> {
    fn drop(&mut self) {
        self.sync();
    }
}

pub struct Reader<T> {
    header: LocalHeader,
    inner: Arc<Inner<T>>,
}

impl<T> Reader<T> {
    pub fn readable_slice<'a>(&'a mut self) -> OwningSlice<'a, T> {
        let head = self.header.head() as usize;
        let tail = self.header.tail() as usize;
        let ptr = unsafe { self.inner.uninit_slice_ptr().add(head) };
        let mut slice_ptr = if head >= tail && self.header.len() > 0 {
            NonNull::slice_from_raw_parts(ptr, self.header.capacity() as usize - head)
        } else {
            NonNull::slice_from_raw_parts(ptr, tail - head)
        };
        OwningSlice::new_filled(unsafe { slice_ptr.as_mut().assume_init_mut() })
    }

    pub fn pop(&mut self) -> Option<T> {
        if self.is_empty() {
            return None;
        }
        Some(unsafe { self.pop_unchecked() })
    }

    /// # Safety
    /// The caller must ensure `self.len() > 0`.
    pub unsafe fn pop_unchecked(&mut self) -> T {
        let value = unsafe {
            self.inner
                .slice_ptr()
                .add(self.header.head() as usize)
                .read()
        };
        self.header.head = self.header.head.wrapping_add(1);
        value
    }

    /// # Safety
    /// The caller must ensure that `n` elements have been consumed from the raw buffer.
    pub unsafe fn advance(&mut self, n: u32) {
        assert!(n <= self.len());
        self.header.head = self.header.head.wrapping_add(n);
    }

    pub fn skip(&mut self, n: u32) {
        for _ in 0..n.min(self.len()) {
            unsafe { self.pop_unchecked() };
        }
    }

    pub fn get(&self, i: u32) -> Option<&T> {
        if i < self.len() {
            Some(unsafe { self.get_unchecked(i) })
        } else {
            None
        }
    }

    /// # Safety
    /// The caller must ensure `i < self.len()`.
    pub unsafe fn get_unchecked(&self, i: u32) -> &T {
        unsafe {
            self.inner
                .slice_ptr()
                .add(self.header.head_offset(i))
                .as_ref()
        }
    }

    pub fn get_mut(&mut self, i: u32) -> Option<&mut T> {
        if i < self.len() {
            Some(unsafe { self.get_unchecked_mut(i) })
        } else {
            None
        }
    }

    /// # Safety
    /// The caller must ensure `i < self.len()`.
    pub unsafe fn get_unchecked_mut(&mut self, i: u32) -> &mut T {
        unsafe {
            self.inner
                .slice_ptr()
                .add(self.header.head_offset(i))
                .as_mut()
        }
    }

    pub fn slices(&self) -> (&[T], &[T]) {
        unsafe { self.header.slices(&self.inner.0.slice) }
    }

    pub fn sync(&mut self) {
        let shared = self.inner.header();
        shared.head.store(self.header.head, Ordering::Release);
        self.header.tail = shared.tail.load(Ordering::Acquire);
    }

    pub fn len(&self) -> u32 {
        self.header.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

unsafe impl<T> Send for Reader<T> {}

impl<T> Drop for Reader<T> {
    fn drop(&mut self) {
        self.sync();
    }
}

#[repr(transparent)]
struct Inner<T>(SliceWithHeader<Header, MaybeUninit<T>>);

impl<T> Inner<T> {
    fn header(&self) -> &Header {
        &self.0.header
    }

    fn slice_ptr(&self) -> NonNull<T> {
        // FIXME: Is this safe? Rust thinks this is immutable, but we'll mutate it
        unsafe { NonNull::new_unchecked(self.0.slice.as_ptr() as *mut T) }
    }

    fn uninit_slice_ptr(&self) -> NonNull<MaybeUninit<T>> {
        self.slice_ptr().cast()
    }

    unsafe fn new_unchecked(capacity: u32) -> Arc<Self> {
        type Slice<T> = SliceWithHeader<Header, MaybeUninit<T>>;

        // NOTE: Unfortunatelly this requires an unnecessary copy. But it is
        // better than having to roll my own Arc.
        let arc: Arc<Slice<T>> = SliceWithHeader::new(
            Header {
                head: CachePadded::new(AtomicU32::new(0)),
                tail: CachePadded::new(AtomicU32::new(0)),
                mask: capacity - 1,
            },
            std::iter::repeat_with(MaybeUninit::uninit).take(capacity as usize),
        );
        // SAFETY: Inner is `repr(transparent)`
        unsafe { std::mem::transmute::<Arc<Slice<T>>, Arc<Inner<T>>>(arc) }
    }
}

impl<T> Drop for Inner<T> {
    fn drop(&mut self) {
        let header = LocalHeader::load_exclusive(&mut self.0.header);
        unsafe {
            let (start, end) = header.slices_mut(&mut self.0.slice);
            NonNull::from(start).drop_in_place();
            NonNull::from(end).drop_in_place();
        }
    }
}

struct Header {
    head: CachePadded<AtomicU32>,
    tail: CachePadded<AtomicU32>,
    mask: u32,
}

#[derive(Clone, Copy)]
struct LocalHeader {
    head: u32,
    tail: u32,
    mask: u32,
}

impl LocalHeader {
    // This does not mutate `header` it just acts as proof of an exclusive
    // reference to it.
    fn load_exclusive(header: &mut Header) -> Self {
        // We have an exclusive reference, so we know all synchronization was
        // already performed. Therefore there are no extra atomic ordering
        // requirements for this operation.
        LocalHeader {
            head: header.head.load(Ordering::Relaxed),
            tail: header.tail.load(Ordering::Relaxed),
            mask: header.mask,
        }
    }

    fn len(&self) -> u32 {
        self.tail.wrapping_sub(self.head)
    }

    fn unfilled(&self) -> u32 {
        self.capacity() - self.len()
    }

    fn capacity(&self) -> u32 {
        self.mask + 1
    }

    fn head(&self) -> u32 {
        self.head & self.mask
    }

    fn tail(&self) -> u32 {
        self.tail & self.mask
    }

    fn head_offset(&self, i: u32) -> usize {
        (self.head.wrapping_add(i) & self.mask) as usize
    }

    fn ranges(&self) -> (Range<usize>, Range<usize>) {
        if self.head() + self.len() <= self.capacity() {
            (
                self.head() as usize..(self.head() + self.len()) as usize,
                self.capacity() as usize..self.capacity() as usize,
            )
        } else {
            (
                0..self.tail() as usize,
                self.head() as usize..self.capacity() as usize,
            )
        }
    }

    unsafe fn slices<'a, T>(&self, buf: &'a [MaybeUninit<T>]) -> (&'a [T], &'a [T]) {
        let (start, end) = self.ranges();
        assert!(start.start <= start.end && start.end <= end.start && end.start <= end.end);

        // TODO: Improve this
        let (_, buf) = buf.split_at(start.start);
        let (start_buf, buf) = buf.split_at(start.end - start.start);
        let (_, buf) = buf.split_at(end.start - start.end);
        let (end_buf, _) = buf.split_at(end.end - end.start);
        unsafe { (end_buf.assume_init_ref(), start_buf.assume_init_ref()) }
    }

    unsafe fn slices_mut<'a, T>(
        &self,
        buf: &'a mut [MaybeUninit<T>],
    ) -> (&'a mut [T], &'a mut [T]) {
        let (start, end) = self.ranges();
        assert!(
            start.start <= start.end && start.end <= end.start && end.start <= end.end,
            "start: {start:?}, end: {end:?}"
        );

        // TODO: Improve this
        let (_, buf) = buf.split_at_mut(start.start);
        let (start_buf, buf) = buf.split_at_mut(start.end - start.start);
        let (_, buf) = buf.split_at_mut(end.start - start.end);
        let (end_buf, _) = buf.split_at_mut(end.end - end.start);
        unsafe { (end_buf.assume_init_mut(), start_buf.assume_init_mut()) }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn can_push_pop() {
        let buf = RingBuf::new(16);
        let (mut writer, mut reader) = buf.split();

        assert_eq!(writer.len(), 0);
        assert_eq!(writer.push(123), Ok(()));
        assert_eq!(writer.len(), 1);
        assert_eq!(reader.len(), 0);
        writer.sync();
        assert_eq!(reader.len(), 0);
        reader.sync();
        assert_eq!(reader.len(), 1);
        assert_eq!(reader.pop(), Some(123));
        assert_eq!(reader.len(), 0);
    }

    #[test]
    fn fails_to_push_over_capacity() {
        let (mut writer, _) = RingBuf::new(2).split();

        assert_eq!(writer.push('a'), Ok(()));
        assert_eq!(writer.push('b'), Ok(()));
        assert_eq!(writer.push('c'), Err(PushErr('c')));
        assert_eq!(writer.len(), 2);
    }

    #[test]
    fn can_roll_over() {
        let (mut writer, mut reader) = RingBuf::new(2).split();

        assert_eq!(writer.push('a'), Ok(()));
        assert_eq!(writer.push('b'), Ok(()));
        writer.sync();
        reader.sync();
        assert_eq!(reader.pop(), Some('a'));
        reader.sync();
        assert_eq!(writer.push('c'), Err(PushErr('c')), "didn't sync yet");
        writer.sync();
        assert_eq!(writer.push('c'), Ok(()));
        assert_eq!(writer.len(), 2);
        writer.sync();
        reader.sync();
        assert_eq!(reader.pop(), Some('b'));
        assert_eq!(reader.pop(), Some('c'));
        assert_eq!(reader.pop(), None);
    }

    #[test]
    fn reader_allows_access_to_slices() {
        let (mut writer, mut reader) = RingBuf::new(8).split();

        for i in 0..8 {
            assert_eq!(writer.push(i), Ok(()))
        }
        writer.sync();
        reader.sync();

        let (first, last) = reader.slices();
        assert_eq!(first.len() + last.len(), reader.len() as usize);
        assert_eq!(
            (first, last),
            (&[] as &[i32], &[0, 1, 2, 3, 4, 5, 6, 7] as &[i32])
        );

        reader.skip(5);
        assert_eq!(reader.len(), 3);
        reader.sync();
        writer.sync();

        for i in 8..12 {
            assert_eq!(writer.push(i), Ok(()));
        }
        writer.sync();
        reader.sync();

        let (first, last) = reader.slices();
        assert_eq!(first.len() + last.len(), reader.len() as usize);
        assert_eq!(
            (first, last),
            (&[5, 6, 7] as &[i32], &[8, 9, 10, 11] as &[i32])
        );
    }

    #[test]
    fn can_write_and_read_parallel() {
        use std::sync::mpsc;

        const BATCHES: usize = 1000;
        const ITEMS_PER_BATCH: usize = 50;
        const BUF_SIZE: u32 = 128;

        assert!(BUF_SIZE as usize > 2 * ITEMS_PER_BATCH);

        let (mut writer, mut reader) = RingBuf::new(BUF_SIZE).split();
        let (batch_ack_tx, batch_ack_rx) = mpsc::sync_channel(1);
        let (batch_range_tx, batch_range_rx) = mpsc::sync_channel(1);

        batch_ack_tx.send(()).unwrap();

        std::thread::scope(|scope| {
            scope.spawn(move || {
                for b in 0..BATCHES {
                    // Wait for previous batch to be acked
                    batch_ack_rx.recv().unwrap();
                    // Sync with the previous ack
                    writer.sync();

                    for i in 0..ITEMS_PER_BATCH {
                        let item = b * ITEMS_PER_BATCH + i;
                        // In this scheme we never expect to overflow
                        assert_eq!(writer.push(item), Ok(()));
                    }
                    // Send the batch
                    writer.sync();
                    batch_range_tx.send(()).unwrap();
                }
            });

            scope.spawn(move || {
                let mut expected_item = 0;
                while let Ok(()) = batch_range_rx.recv() {
                    reader.sync(); // Get batch
                    // Signal that the producer can write more data, while we
                    // read.
                    let sender_done = batch_ack_tx.send(()).is_err();
                    while let Some(item) = reader.pop() {
                        assert_eq!(item, expected_item);
                        expected_item += 1;
                    }
                    if sender_done {
                        assert_eq!(expected_item, BATCHES * ITEMS_PER_BATCH);
                        return;
                    }
                }
            });
        });
    }
}
