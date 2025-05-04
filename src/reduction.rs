//! Multithreaded reducers.
//!
//! See [`ReduceAsync::reduce_async`] & [`ReduceAsyncCommutative::reduce_async_commutative`] 
//! for more detailed documentation.

use core::range::RangeInclusive;
use std::{
    collections::VecDeque,
    ops::ControlFlow::{self, *},
    sync::{
        Mutex,
        atomic::{AtomicBool, Ordering},
        mpmc::channel,
    },
    thread::{self, scope},
};

use crate::num_cpus;
// imports for documentation
#[allow(unused_imports)]
use crate::{OrderedThreadpool, Threadpool};

fn reduce_async_commutative_inner<I, F>(it: I, f: F) -> Option<I::Item>
where
    I: Iterator,
    I::Item: Send + Sync,
    F: Fn(I::Item, I::Item) -> I::Item + Send + Sync,
{
    let work_finished = AtomicBool::new(false);
    scope(|scope| {
        let (outbox, inbox) = channel();
        let workers: Vec<_> = (0..num_cpus().into())
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
                        thread::yield_now();
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

/// Extension trait to provide the `reduce_async_commutative` function
/// for iterators.
pub trait ReduceAsyncCommutative: Iterator {
    /// Reduce an iterator in parallel, without respect to the ordering
    /// of the input elements.
    ///
    /// The reducing function must be both associative (ie. the order in which
    /// pairs are evaluated does not affect the result), and commutative
    /// (the ordering of the two arguments with regards to each other does not
    /// affect the result), or else the result will be nondeterministic.
    ///
    /// For a reducer that does not require commutativity of the reducing function
    /// (but is slower), see [`ReduceAsync::reduce_async`]
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
    ///         .filter_map_async(
    ///             |x: usize| {
    ///                 let x = x.pow(3) % 100;
    ///                 (x > 50).then_some(x)
    ///             },
    ///             scope,
    ///         )
    ///         .reduce_async_commutative(|a, b| a + b)
    ///         .unwrap();
    ///    
    ///     assert_eq!(sequential_result, parallel_result);
    /// })
    /// ```
    fn reduce_async_commutative<F>(self, f: F) -> Option<Self::Item>
    where
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
        Self::Item: Send + Sync,
        F: Fn(Self::Item, Self::Item) -> Self::Item + Send + Sync,
    {
        reduce_async_commutative_inner(self, f)
    }
}

struct RangeQueue<T>(VecDeque<(T, RangeInclusive<usize>)>);

impl<T> RangeQueue<T> {
    pub fn new() -> Self {
        Self(VecDeque::new())
    }

    pub fn insert(&mut self, item: T, range: RangeInclusive<usize>) {
        let index = self
            .0
            .binary_search_by_key(&range.start, |x| x.1.start)
            .unwrap_err();
        self.0.insert(index, (item, range));
    }

    pub fn pop_pair(
        &mut self,
    ) -> ControlFlow<(), Option<((T, RangeInclusive<usize>), (T, RangeInclusive<usize>))>> {
        if self.0.len() < 2 {
            return Break(());
        }
        for i in 0..self.0.len() - 1 {
            let left = &self.0[i];
            let right = &self.0[i + 1];
            if left.1.end + 1 == right.1.start {
                let left = self.0.remove(i).unwrap();
                let right = self.0.remove(i).unwrap();
                return Continue(Some((left, right)));
            }
        }
        Continue(None)
    }
}

fn reduce_async_noncommutative_inner<I, F>(iter: I, f: F) -> Option<I::Item>
where
    I: Iterator,
    I::Item: Send + Sync,
    F: Fn(I::Item, I::Item) -> I::Item + Send + Sync,
{
    let queue = Mutex::new(RangeQueue::new());
    let work_finished = AtomicBool::new(false);
    scope(|scope| {
        (0..num_cpus().into()).for_each(|_| {
            scope.spawn(|| {
                loop {
                    let pair = queue.lock().unwrap().pop_pair();
                    match pair {
                        Break(()) => {
                            if work_finished.load(Ordering::Acquire) {
                                break;
                            }
                        }
                        Continue(pair) => {
                            if let Some(((left_item, left_range), (right_item, right_range))) = pair
                            {
                                let combined_item = f(left_item, right_item);
                                let combined_range = (left_range.start..=right_range.end).into();
                                queue.lock().unwrap().insert(combined_item, combined_range);
                            }
                        }
                    }
                    thread::yield_now();
                }
            });
        });
        iter.enumerate()
            .for_each(|(i, elem)| queue.lock().unwrap().insert(elem, (i..=i).into()));
        work_finished.store(true, Ordering::Release);
    });
    queue.into_inner().unwrap().0.pop_front().map(|x| x.0)
}

/// Extension trait to provide the `reduce_async` function
/// for iterators.
pub trait ReduceAsync: Iterator {
    /// Reduce an iterator in parallel, respecting the ordering of the
    /// input elements.
    ///
    /// The reducing function must be associative (ie. the order in which
    /// pairs are evaluated does not affect the result), but does not need
    /// to be commutative. If the reducing function is not associative, the
    /// result will be nondeterministic.
    ///
    /// For a faster reducer (but requires the reducing function to be commutative)
    /// see [`ReduceAsyncCommutative::reduce_async_commutative`]
    /// 
    /// If you require your reducing function to be both noncommutative and 
    /// nonassociative, then it is not possible to parallelize the reduction, and
    /// you must use the sequential [`Iterator::reduce`] to reduce your data.
    ///
    /// Pairs nicely when chained with the result of one
    /// or more [`OrderedThreadpool`]s.
    /// ```
    /// use threadpools::*;
    ///
    /// let str = "qwertyuiopasdfghjklzxcvbnm"
    ///     .chars()
    ///     .map(|x| x.to_string())
    ///     .cycle()
    ///     .take(10000);
    /// let reduced = str.clone().reduce_async(|a, b| a + &b).unwrap();
    /// let sequential_result = str.collect::<Vec<_>>().join("");
    ///
    /// assert_eq!(reduced, sequential_result);
    /// ```
    fn reduce_async<F>(self, f: F) -> Option<Self::Item>
    where
        Self: Sized,
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
        Self: Sized,
        Self::Item: Send + Sync,
        F: Fn(Self::Item, Self::Item) -> Self::Item + Send + Sync,
    {
        reduce_async_noncommutative_inner(self, f)
    }
}
