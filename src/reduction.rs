//! Multithreaded reducer.
//!
//! See [`ReduceAsync::reduce_async`] for more detailed documentation.

use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        mpmc::channel,
    },
    thread::{self, available_parallelism},
};

// imports for documentation
#[allow(unused_imports)]
use crate::unordered::Threadpool;

fn reduce_async_inner<I, F>(it: I, f: F) -> Option<I::Item>
where
    I: Iterator,
    I::Item: Send + Sync,
    F: Fn(I::Item, I::Item) -> I::Item + Send + Sync,
{
    let work_finished = AtomicBool::new(false);
    thread::scope(|scope| {
        let (outbox, inbox) = channel();
        let num_cpus: usize = available_parallelism().map(usize::from).unwrap_or(1);
        let workers: Vec<_> = (0..num_cpus)
            .map(|_| {
                let inbox = inbox.clone();
                let outbox = outbox.clone();
                let f = &f;
                let work_finished = &work_finished;
                scope.spawn(move || {
                    let mut stock = None;
                    loop {
                        while let Ok(Some(elem)) = inbox.try_recv() {
                            stock = match stock {
                                Some(other) => {
                                    outbox.send(Some(f(elem, other))).unwrap();
                                    None
                                }
                                None => Some(elem),
                            };
                        }
                        if work_finished.load(Ordering::Acquire) {
                            break;
                        }
                    }
                    stock
                })
            })
            .collect();
        it.for_each(|elem| outbox.send(Some(elem)).unwrap());
        work_finished.store(true, Ordering::Release);
        workers
            .into_iter()
            .filter_map(|worker| worker.join().unwrap())
            .collect::<Vec<_>>()
    })
    .into_iter()
    .reduce(f)
}

/// Extension trait to provide the `reduce_async` function
/// for iterators.
pub trait ReduceAsync: Iterator {
    /// Reduce an iterator in parallel.
    ///
    /// Pairs nicely when chained with the result of one
    /// or more [`Threadpool`]s.
    ///
    /// ```
    /// use threadpools::*;
    /// use std::thread::scope;
    ///
    /// scope(|scope| {
    ///     let vals = 0..10000usize;
    ///    
    ///     let sequential_result = vals
    ///         .clone()
    ///         .filter_map(|x: usize| {
    ///             let x = x.pow(3) % 100;
    ///             (x > 50).then_some(x)
    ///         })
    ///         .reduce(|a, b| a + b)
    ///         .unwrap();
    ///    
    ///     let parallel_result = vals
    ///         .filter_map_multithread_async(
    ///             |x: usize| {
    ///                 let x = x.pow(3) % 100;
    ///                 (x > 50).then_some(x)
    ///             },
    ///             scope,
    ///         )
    ///         .reduce_async(|a, b| a + b)
    ///         .unwrap();
    ///    
    ///     assert_eq!(sequential_result, parallel_result);
    /// })
    /// ```
    fn reduce_async<F>(self, f: F) -> Option<Self::Item>
    where
        Self::Item: Send + Sync,
        F: Fn(Self::Item, Self::Item) -> Self::Item + Send + Sync;
}

impl<I> ReduceAsync for I
where
    I: Iterator,
    I::Item: Send + Sync,
{
    fn reduce_async<F>(self, f: F) -> Option<Self::Item>
    where
        Self::Item: Send + Sync,
        F: Fn(Self::Item, Self::Item) -> Self::Item + Send + Sync,
    {
        reduce_async_inner(self, f)
    }
}
