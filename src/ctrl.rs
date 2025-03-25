use std::sync::{Mutex, MutexGuard};
use std::task::{Context, Poll, Waker};

use crate::Config;

pub(crate) struct Ctrl<C = Config, S = Mutex<SharedCtrl>> {
    config: C,
    shared: S,
}

pub(crate) type LockedCtrl<'a> = Ctrl<&'a Config, MutexGuard<'a, SharedCtrl>>;

impl Ctrl {
    pub(crate) fn new(config: Config) -> Self {
        Ctrl {
            config,
            shared: Mutex::new(SharedCtrl::new()),
        }
    }

    #[allow(dead_code)]
    pub(crate) fn config(&self) -> &Config {
        &self.config
    }

    pub(crate) fn lock(&self) -> LockedCtrl {
        Ctrl {
            config: &self.config,
            shared: self.shared.lock().unwrap(),
        }
    }
}

impl Ctrl<&Config, MutexGuard<'_, SharedCtrl>> {
    pub(crate) fn produced(&self) -> usize {
        self.shared.produced
    }

    pub(crate) fn consumed(&self) -> usize {
        self.shared.consumed
    }

    pub(crate) fn examined(&self) -> usize {
        self.shared.examined
    }

    pub(crate) fn produce_to(&mut self, produced: usize) {
        self.shared.produce_to(produced);
        self.shared.wake_consumer();
    }

    pub(crate) fn advance_to(&mut self, consumed: usize, examined: usize) {
        self.shared.advance_to(consumed, examined);
        self.shared.wake_producer(self.config);
    }

    pub(crate) fn set_producer_complete(&mut self) {
        self.shared.set_producer_complete();
    }

    pub(crate) fn set_consumer_complete(&mut self) {
        self.shared.set_consumer_complete();
    }

    pub(crate) fn poll_wait_to_produce(&mut self, cx: &mut Context) -> Poll<bool> {
        if self.shared.should_wake_producer(self.config) {
            self.shared.reset_producer_waker();
            return Poll::Ready(self.shared.is_consumer_complete());
        }
        self.shared.set_producer_waker(cx.waker().clone());
        Poll::Pending
    }

    pub(crate) fn poll_wait_to_consume(&mut self, cx: &mut Context) -> Poll<bool> {
        if self.shared.should_wake_consumer() {
            self.shared.reset_consumer_waker();
            return Poll::Ready(self.shared.is_producer_complete());
        }
        self.shared.set_consumer_waker(cx.waker().clone());
        Poll::Pending
    }
}

pub(crate) struct SharedCtrl {
    produced: usize,
    examined: usize,
    consumed: usize,
    // TODO: Allow consumers to ask for a particular number of elements
    // min_ask: usize,
    write_waker: Option<Waker>,
    read_waker: Option<Waker>,
    reader_complete: bool,
    writer_complete: bool,
}

impl SharedCtrl {
    fn new() -> Self {
        SharedCtrl {
            produced: 0,
            examined: 0,
            consumed: 0,
            write_waker: None,
            read_waker: None,
            reader_complete: false,
            writer_complete: false,
        }
    }

    fn unexamined(&self) -> usize {
        debug_assert!(self.examined <= self.produced);
        self.produced - self.examined
    }

    fn produce_to(&mut self, produced: usize) {
        // println!("produced_to: {}", produced);
        debug_assert!(self.produced <= produced);
        self.produced = produced;
    }

    fn advance_to(&mut self, consumed: usize, examined: usize) {
        debug_assert!(self.consumed <= consumed);
        debug_assert!(self.examined <= examined);
        self.consumed = consumed;
        self.examined = examined;
    }

    fn should_wake_consumer(&self) -> bool {
        if self.is_producer_complete() {
            return true;
        }
        self.unexamined() > 0
    }

    fn wake_consumer(&mut self) {
        if self.should_wake_consumer() {
            if let Some(waker) = self.read_waker.take() {
                waker.wake();
            }
        }
    }

    fn should_wake_producer(&self, config: &Config) -> bool {
        if self.is_consumer_complete() {
            return true;
        }
        self.unexamined() < config.backpressure
    }

    fn wake_producer(&mut self, config: &Config) {
        if self.should_wake_producer(config) {
            if let Some(waker) = self.write_waker.take() {
                waker.wake();
            }
        }
    }

    fn set_producer_waker(&mut self, waker: Waker) {
        self.write_waker = Some(waker);
    }

    fn reset_producer_waker(&mut self) {
        self.write_waker = None;
    }

    fn set_consumer_waker(&mut self, waker: Waker) {
        self.read_waker = Some(waker);
    }

    fn reset_consumer_waker(&mut self) {
        self.read_waker = None;
    }

    fn is_producer_complete(&self) -> bool {
        self.writer_complete
    }

    fn is_consumer_complete(&self) -> bool {
        self.reader_complete
    }

    fn set_producer_complete(&mut self) {
        self.writer_complete = true;
    }

    fn set_consumer_complete(&mut self) {
        self.reader_complete = true;
    }
}
