Flexible thread pool implementations for all your multithreaded processing needs.

## Overview

This library includes two thread pool implementations, [`Threadpool`] and [`OrderedThreadpool`].

* [`Threadpool`] consumes jobs passed to it, processes them, and then yields them back out in the order they finish.
* [`OrderedThreadpool`] consumes jobs passed to it, processes them, and then yields them out in the same order they were received in.

Aside from a few small details, they share many of the same features and properties.

Thread pools can be used for many purposes: they can be spawned momentarily to process a single iterator, and then destroyed (although for that purpose, `rayon` is likely a better choice); they can be spawned for long-running tasks that require multiple workers to respond, like a webservice or database management system; they can be spun up and reused multiple times to evade the overhead of repeatedly creating and destroying threads to perform repeated parallel work; the list goes on.

Additionally, this library provides a little helper utility [`ReduceAsync::reduce_async`], which implements a unique feedback-loop style fully parallelized reducing algorithm. It pairs nicely when combined with the thread pools, so there are a suite of filtering, mapping, and reduction extensions available to make including its use in your code simple and straightforward. 

These are all the current iteration extensions implemented in this library:

 * [`FilterMapMultithread::filter_map_multithread`]
 * [`FilterMapAsync::filter_map_async`]
 * [`FilterMapAsyncUnordered::filter_map_async_unordered`]
 * [`ReduceAsync::reduce_async`]
 * [`ReduceAsyncCommutative::reduce_async_commutative`]
 * [`FilterMapReduceAsync::filter_map_reduce_async`]
 * [`FilterMapReduceAsyncCommutative::filter_map_reduce_async_commutative`]
 * [`Pipe::pipe`]

TL;DR: Spawn a thread pool, use it to asynchronously process data, then discard it once you finish. Or don't, I'm not your dad.

## Examples

### Synchronously process a sequence of elements:

```rust
use std::thread::{self, scope};
use threadpools::*;

fn is_prime(n: usize) -> bool {
    if n < 2 {
        return false;
    }
    for i in 2..=((n as f64).sqrt() as usize) {
        if n % i == 0 {
            return false;
        }
    }
    true
}

scope(|scope| {
    let pool = Threadpool::new(|x| is_prime(x).then_some(x), scope);
    pool.submit_all(0..1000);
    let total: usize = pool.into_iter().sum();
    assert_eq!(total, 76127);
})
```

### Asynchronously process a sequence of elements:

```rust
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread::{self, scope};
use std::time::Duration;
use threadpools::*;

fn is_prime(n: usize) -> bool {
    if n < 2 {
        return false;
    }
    for i in 2..=((n as f64).sqrt() as usize) {
        if n % i == 0 {
            return false;
        }
    }
    true
}

let total = AtomicUsize::new(0);
scope(|scope| {
    let pool = Threadpool::new(|x| is_prime(x).then_some(x), scope);
    pool.producer(0..1000);
    pool.consumer(|x| {
        total.fetch_add(x, Ordering::SeqCst);
    });
    pool.wait_until_finished();
    // unavoidable necessity to wait for the consumer to finish running;
    // this is a nonstandard usage of the consumer.
    // A real world application would have a more robust, purpose-built
    // synchronization strategy.
    thread::sleep(Duration::from_millis(50));
    let total = total.load(Ordering::Acquire);
    assert_eq!(total, 76127);
})
```

### Filter, Map, & Reduce Asynchronously:

```rust
use threadpools::*;

let vals = 0..10000usize;

let sequential_result = vals
    .clone()
    .filter_map(|x: usize| {
        let x = x.pow(3) % 100;
        (x > 50).then_some(x)
    })
    .reduce(|a, b| a + b)
    .unwrap();

let parallel_result = vals
    .filter_map_reduce_async_commutative(
        |x: usize| {
            let x = x.pow(3) % 100;
            (x > 50).then_some(x)
        },
        |a, b| a + b,
    )
    .unwrap();

assert_eq!(sequential_result, parallel_result);
```

### Chain mutliple pools together:

```rust
use threadpools::*;
use std::thread::scope;

scope(|scope| {
    let sequential_result = (0..10000usize)
        .filter_map(|x: usize| {
            let x = x.pow(3) % 100;
            (x > 50).then_some(x)
        })
        .reduce(|a, b| a + b)
        .unwrap();

    let parallel_result = (0..10000usize)
        .pipe(Threadpool::new(|x: usize| Some(x.pow(3)), scope))
        .pipe(Threadpool::new(|x: usize| Some(x % 100), scope))
        .pipe(Threadpool::new(|x: usize| (x > 50).then_some(x), scope))
        .into_iter()
        .reduce_async_commutative(|a, b| a + b)
        .unwrap();

    assert_eq!(sequential_result, parallel_result);
});
```

---

See `/tests` for more usage examples.