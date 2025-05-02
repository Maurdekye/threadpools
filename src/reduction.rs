//! Multithreaded reducer.
//!
//! See [`ReduceAsync::reduce_async`] for more detailed documentation.

use core::range::RangeInclusive;
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        mpmc::{Receiver, channel},
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
                        while let Ok(elem) = inbox.try_recv() {
                            stock = match stock {
                                Some(other) => {
                                    outbox.send(f(elem, other)).unwrap();
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
        it.for_each(|elem| outbox.send(elem).unwrap());
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
    /// The reducing function must be both associative (ie. the order in which
    /// pairs are evaluated does not affect the result), and commutative
    /// (the ordering of the two arguments with regards to each other does not
    /// affect the result), or else the result will be noneterministic.
    ///
    /// For a version that respects commutativity (but is slower), see
    /// [`ReduceAsyncCommutative::reduce_async_commutative`]
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

fn reduce_async_commutative_inner<I, F>(it: I, f: F) -> Option<I::Item>
where
    I: Iterator,
    I::Item: Send + Sync,
    F: Fn(I::Item, I::Item) -> I::Item + Send + Sync,
{
    let work_finished = AtomicBool::new(false);
    let mut sub_results: Vec<_> = thread::scope(|scope| {
        let (outbox, inbox) = channel();
        let num_cpus: usize = available_parallelism().map(usize::from).unwrap_or(1);
        let workers: Vec<_> = (0..num_cpus)
            .map(|_| {
                let inbox: Receiver<(I::Item, RangeInclusive<usize>)> = inbox.clone();
                let outbox = outbox.clone();
                let f = &f;
                let work_finished = &work_finished;
                scope.spawn(move || {
                    let mut stock: Option<(I::Item, RangeInclusive<usize>)> = None;
                    loop {
                        while let Ok((elem, range)) = inbox.try_recv() {
                            stock = match stock {
                                Some((other, other_range)) => {
                                    // determine the order these elements should be paired in, if
                                    // pairing is possible.
                                    // the low likelihood of a pairing being immediately feasible
                                    // is the primary cause of this algorithm's inefficiency.
                                    if range.end + 1 == other_range.start {
                                        outbox
                                            .send((
                                                f(elem, other),
                                                (range.start..=other_range.end).into(),
                                            ))
                                            .unwrap()
                                    } else if other_range.start + 1 == range.end {
                                        outbox
                                            .send((
                                                f(other, elem),
                                                (other_range.start..=range.end).into(),
                                            ))
                                            .unwrap()
                                    } else {
                                        // these elements are not pairable, release both back to the
                                        // pool and retrieve new ones
                                        outbox.send((elem, range)).unwrap();
                                        // we release the stock as well in order to prevent deadlocks
                                        // where two separate workers hold pairable chunks in their stocks, but
                                        // dont agree to give them up to allow the final result to complete.
                                        outbox.send((other, other_range)).unwrap();
                                    }
                                    None
                                }
                                None => Some((elem, range)),
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
        it.enumerate()
            .for_each(|(i, elem)| outbox.send((elem, (i..=i).into())).unwrap());
        work_finished.store(true, Ordering::Release);
        workers
            .into_iter()
            .filter_map(|worker| worker.join().unwrap())
            .collect::<Vec<_>>()
    })
    .into_iter()
    .collect();
    sub_results.sort_by_key(|x| x.1.start);
    sub_results.into_iter().map(|x| x.0).reduce(f)
}

/// Extension trait to provide the `reduce_async_commutative` function
/// for iterators.
pub trait ReduceAsyncCommutative: Iterator {
    /// Reduce an iterator in parallel, respecting commutativity.
    ///
    /// Warning: This function is *astronomically slow*
    /// compared to its noncommutative counterpart [`ReduceAsync::reduce_async`]; it's
    /// likely much faster to simply perform a linear, singlethreaded reduction via
    /// [`Iterator::reduce`] rather than use this function. For this reason, the function
    /// is currently marked as deprecated until the underlying algorithm can be improved.
    /// It's advised that you either use the noncommutative version, or if commutative
    /// asynchronous reduction is absolutely necessary, the crate `rayon` provides a
    /// useful parallel reduction method which respects commutativity.
    ///
    /// The reducing function must be associative (ie. the order in which
    /// pairs are evaluated does not affect the result), but does not need
    /// to be commutative. The result will be deterministic assuming the
    /// function is associative.
    /// ```
    /// use threadpools::*;
    ///
    /// let str = "qwertyuiopasdfghjklzxcvbnm"
    ///     .chars()
    ///     .map(|x| x.to_string())
    ///     .cycle()
    ///     .take(1000);
    /// let reduced = str.clone().reduce_async_commutative(|a, b| a + &b).unwrap();
    /// let sequential_result = str.collect::<Vec<_>>().join("");
    ///
    /// assert_eq!(reduced, sequential_result);
    /// ```
    #[deprecated]
    fn reduce_async_commutative<F>(self, f: F) -> Option<Self::Item>
    where
        Self: Sized, // Required because reduce_async_commutative_inner consumes the iterator
        Self::Item: Send + Sync,
        F: Fn(Self::Item, Self::Item) -> Self::Item + Send + Sync;
}

impl<I> ReduceAsyncCommutative for I
where
    I: Iterator,
    I::Item: Send + Sync,
{
    fn reduce_async_commutative<F>(self, f: F) -> Option<Self::Item>
    where
        Self: Sized,
        Self::Item: Send + Sync,
        F: Fn(Self::Item, Self::Item) -> Self::Item + Send + Sync,
    {
        reduce_async_commutative_inner(self, f)
    }
}
