#![doc = include_str!("../README.md")]
#![feature(mpmc_channel)]
#![feature(new_range_api)]

use std::{
    num::NonZeroUsize,
    sync::{Arc, Condvar, Mutex},
    thread::{available_parallelism, scope},
};

pub use ordered::*;
pub use reduction::*;
pub use unordered::*;

pub mod ordered;
pub mod reduction;
pub mod unordered;

fn num_cpus() -> NonZeroUsize {
    available_parallelism().unwrap_or(NonZeroUsize::MIN)
}

#[derive(Clone)]
struct Gate<S>(Arc<(Mutex<S>, Condvar)>);

impl<S> Gate<S> {
    fn new(initial_state: S) -> Self {
        Self(Arc::new((Mutex::new(initial_state), Condvar::new())))
    }

    fn update<T>(&self, updater: impl FnOnce(&mut S) -> T) -> T {
        let mut state = self.0.0.lock().unwrap();
        let result = (updater)(&mut state);
        self.0.1.notify_all();
        result
    }

    fn wait_while(&self, condition: impl Fn(&S) -> bool) {
        let mut state = self.0.0.lock().unwrap();
        while (condition)(&state) {
            state = self.0.1.wait(state).unwrap();
        }
    }

    fn check(&self) -> S
    where
        S: Copy,
    {
        *self.0.0.lock().unwrap()
    }
}

/// Extension trait to provide the `filter_map_reduce_async_unordered` function
/// for iterators.
pub trait FilterMapReduceAsyncUnordered: Iterator {
    /// Combine multithreaded mapping, filtering, and reduction all into a single tidy function call.
    ///
    /// Makes use of the standard [`Threadpool`] to discard the additional overhead introduced by
    /// the [`OrderedThreadpool`], which is unnecessary in light of the fact that all the results
    /// end up being reduced down to a single value anyway.
    ///
    /// The reducing function must be both associative (ie. the order in which
    /// pairs are evaluated does not affect the result), and commutative
    /// (the ordering of the two arguments with regards to each other does not
    /// affect the result), or else the result will be nondeterministic.
    ///
    /// If you require the reduction to be noncommutative, then refer to
    /// [`FilterMapReduceAsync::filter_map_reduce_async`].
    ///
    /// ```
    /// use threadpools::*;
    ///
    /// let vals = 0..10000usize;
    ///
    /// let sequential_result = vals
    ///     .clone()
    ///     .filter_map(|x: usize| {
    ///         let x = x.pow(3) % 100;
    ///         (x > 50).then_some(x)
    ///     })
    ///     .reduce(|a, b| a + b)
    ///     .unwrap();
    ///
    /// let parallel_result = vals
    ///     .filter_map_reduce_async_unordered(
    ///         |x: usize| {
    ///             let x = x.pow(3) % 100;
    ///             (x > 50).then_some(x)
    ///         },
    ///         |a, b| a + b,
    ///     )
    ///     .unwrap();
    ///
    /// assert_eq!(sequential_result, parallel_result);
    /// ```
    fn filter_map_reduce_async_unordered<F, R, O>(self, f: F, r: R) -> Option<O>
    where
        Self::Item: Send + Sync,
        O: Send + Sync,
        F: Fn(Self::Item) -> Option<O> + Send + Sync,
        R: Fn(O, O) -> O + Send + Sync;
}

impl<T> FilterMapReduceAsyncUnordered for T
where
    T: Iterator + Send + Sync,
{
    fn filter_map_reduce_async_unordered<F, R, O>(self, f: F, r: R) -> Option<O>
    where
        Self::Item: Send + Sync,
        O: Send + Sync,
        F: Fn(Self::Item) -> Option<O> + Send + Sync,
        R: Fn(O, O) -> O + Send + Sync,
    {
        scope(|scope| {
            self.filter_map_multithread_async_unordered(f, scope)
                .reduce_async_commutative(r)
        })
    }
}

/// Extension trait to provide the `filter_map_reduce_async` function
/// for iterators.
pub trait FilterMapReduceAsync: Iterator {
    /// Combine multithreaded mapping, filtering, and reduction all into a single tidy function call.
    ///
    /// Makes use of the ordered [`OrderedThreadpool`] and the noncommutative reducer
    /// [`ReduceAsync::reduce_async`] to reduce elements respecting their original ordering.
    ///
    /// The reducing function must be associative (ie. the order in which
    /// pairs are evaluated does not affect the result), but does not need
    /// to be commutative. If the reducing function is not associative, the
    /// result will be nondeterministic.
    ///
    /// If noncommutativity is not needed for your reducer, then consider using
    /// [`FilterMapReduceAsyncUnordered::filter_map_reduce_async_unordered`] instead,
    /// which is more efficient.
    ///
    /// If you require the reducer to be noncommutative and nonassociative, then simply
    /// chain [`FilterMapMultithreadAsync::filter_map_multithread_async`] with
    /// the standard [`Iterator::reduce`], as a noncommutative and nonassociative reducing
    /// function cannot be parallelized.
    ///
    /// ```
    /// use threadpools::*;
    ///
    /// let chars = "qwertyuiopasdfghjklzxcvbnm"
    ///     .chars()
    ///     .cycle()
    ///     .take(10000);
    ///
    /// let sequential_result = chars
    ///     .clone()
    ///     .filter_map(|c: char| {
    ///         (!['a', 'e', 'i', 'o', 'u'].contains(&c)).then(|| c.to_string())
    ///     })
    ///     .reduce(|a, b| a + &b)
    ///     .unwrap();
    ///
    /// let parallel_result = chars
    ///     .filter_map_reduce_async(
    ///         |c: char| {
    ///             (!['a', 'e', 'i', 'o', 'u'].contains(&c)).then(|| c.to_string())
    ///         },
    ///         |a, b| a + &b,
    ///     )
    ///     .unwrap();
    ///
    /// assert_eq!(sequential_result, parallel_result);
    /// ```
    fn filter_map_reduce_async<F, R, O>(self, f: F, r: R) -> Option<O>
    where
        Self::Item: Send + Sync,
        O: Send + Sync,
        F: Fn(Self::Item) -> Option<O> + Send + Sync,
        R: Fn(O, O) -> O + Send + Sync;
}

impl<T> FilterMapReduceAsync for T
where
    T: Iterator + Send + Sync,
{
    fn filter_map_reduce_async<F, R, O>(self, f: F, r: R) -> Option<O>
    where
        Self::Item: Send + Sync,
        O: Send + Sync,
        F: Fn(Self::Item) -> Option<O> + Send + Sync,
        R: Fn(O, O) -> O + Send + Sync,
    {
        scope(|scope| self.filter_map_multithread_async(f, scope).reduce_async(r))
    }
}
