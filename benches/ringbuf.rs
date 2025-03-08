use std::sync::mpsc;
use std::hint::black_box;

use criterion::{criterion_group, criterion_main, Criterion, Throughput};

use pipes::ringbuf::RingBuf;

fn criterion_benchmark(c: &mut Criterion) {
    const BATCHES: usize = 10;
    const ITEMS_PER_BATCH: usize = 1000;
    const BUF_SIZE: u32 = (2 * ITEMS_PER_BATCH + ITEMS_PER_BATCH / 2).next_power_of_two() as u32;

    assert!(BUF_SIZE as usize > 2 * ITEMS_PER_BATCH);

    let (batch_dispatch_tx, batch_done_rx, t1, t2) = {
        let (mut writer, mut reader) = RingBuf::new(BUF_SIZE).split();
        let (batch_dispatch_tx, batch_dispatch_rx) = mpsc::sync_channel(1);
        let (batch_done_tx, batch_done_rx) = mpsc::sync_channel(1);
        let (batch_range_tx, batch_range_rx) = mpsc::sync_channel(1);
        let (batch_ack_tx, batch_ack_rx) = mpsc::sync_channel(1);

        let t1 = std::thread::spawn(move || {
            while batch_dispatch_rx.recv().is_ok() {
                for b in 0..BATCHES {
                    if b > 0 {
                        // Wait for previous batch to be acked
                        batch_ack_rx.recv().unwrap();
                        writer.sync();
                    }

                    for i in 0..ITEMS_PER_BATCH {
                        _ = black_box(writer.push(i));
                    }
                    writer.sync();
                    batch_range_tx.send(b).unwrap();
                }
                batch_ack_rx.recv().unwrap();
            }
        });

        let t2 = std::thread::spawn(move || {
            while let Ok(b) = batch_range_rx.recv() {
                reader.sync(); // Get batch
                _ = batch_ack_tx.send(());
                while let Some(item) = reader.pop() {
                    black_box(item);
                }
                if b == BATCHES - 1 {
                    batch_done_tx.send(()).unwrap();
                }
            }
        });

        (batch_dispatch_tx, batch_done_rx, t1, t2)
    };

    let mut group = c.benchmark_group("batched");

    group.throughput(Throughput::Elements((BATCHES * ITEMS_PER_BATCH) as u64));

    group.bench_function("ringbuf", |b| b.iter(|| {
        batch_dispatch_tx.send(()).unwrap();
        batch_done_rx.recv().unwrap();
    }));

    drop(batch_dispatch_tx);
    drop(batch_done_rx);
    t1.join().unwrap();
    t2.join().unwrap();

    let (batch_dispatch_tx, batch_done_rx, t1, t2) = {
        let (sender, receiver) = mpsc::sync_channel(BUF_SIZE as usize);
        let (batch_dispatch_tx, batch_dispatch_rx) = mpsc::sync_channel(1);
        let (batch_done_tx, batch_done_rx) = mpsc::sync_channel(1);
        let (batch_range_tx, batch_range_rx) = mpsc::sync_channel(1);
        let (batch_ack_tx, batch_ack_rx) = mpsc::sync_channel(1);

        let t1 = std::thread::spawn(move || {
            while batch_dispatch_rx.recv().is_ok() {
                for b in 0..BATCHES {
                    if b > 0 {
                        // Wait for previous batch to be acked
                        batch_ack_rx.recv().unwrap();
                    }

                    for i in 0..ITEMS_PER_BATCH {
                        _ = black_box(sender.send(i));
                    }
                    batch_range_tx.send(b).unwrap();
                }
                batch_ack_rx.recv().unwrap();
            }
        });

        let t2 = std::thread::spawn(move || {
            while let Ok(b) = batch_range_rx.recv() {
                _ = batch_ack_tx.send(());
                while let Ok(item) = receiver.try_recv() {
                    black_box(item);
                }
                if b == BATCHES - 1 {
                    batch_done_tx.send(()).unwrap();
                }
            }
        });

        (batch_dispatch_tx, batch_done_rx, t1, t2)
    };

    group.bench_function("mpsc", |b| b.iter(|| {
        batch_dispatch_tx.send(()).unwrap();
        batch_done_rx.recv().unwrap();
    }));

    drop(batch_dispatch_tx);
    drop(batch_done_rx);
    t1.join().unwrap();
    t2.join().unwrap();

    let (batch_dispatch_tx, batch_done_rx, t1, t2) = {
        let (batch_dispatch_tx, batch_dispatch_rx) = mpsc::sync_channel(1);
        let (batch_done_tx, batch_done_rx) = mpsc::sync_channel(1);
        let (batch_range_tx, batch_range_rx) = mpsc::sync_channel(1);
        let (batch_ack_tx, batch_ack_rx) = mpsc::sync_channel(1);

        let t1 = std::thread::spawn(move || {
            while batch_dispatch_rx.recv().is_ok() {
                for b in 0..BATCHES {
                    if b > 0 {
                        // Wait for previous batch to be acked
                        batch_ack_rx.recv().unwrap();
                    }

                    for i in 0..ITEMS_PER_BATCH {
                        black_box(i);
                    }
                    batch_range_tx.send(b).unwrap();
                }
                batch_ack_rx.recv().unwrap();
            }
        });

        let t2 = std::thread::spawn(move || {
            while let Ok(b) = batch_range_rx.recv() {
                _ = batch_ack_tx.send(());
                for i in 0..ITEMS_PER_BATCH {
                    black_box(i);
                }
                if b == BATCHES - 1 {
                    batch_done_tx.send(()).unwrap();
                }
            }
        });

        (batch_dispatch_tx, batch_done_rx, t1, t2)
    };

    group.bench_function("baseline", |b| b.iter(|| {
        batch_dispatch_tx.send(()).unwrap();
        batch_done_rx.recv().unwrap();
    }));

    drop(batch_dispatch_tx);
    drop(batch_done_rx);
    t1.join().unwrap();
    t2.join().unwrap();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
