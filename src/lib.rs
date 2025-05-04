#![doc = include_str!("../README.md")]
#![feature(mpmc_channel)]
#![feature(new_range_api)]

use std::{
    num::NonZeroUsize,
    sync::{
        Arc, Condvar, Mutex,
        mpmc::{self, Receiver, Sender},
    },
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

fn channel<T>(blocking: bool) -> (Sender<T>, Receiver<T>) {
    if blocking {
        mpmc::sync_channel(0)
    } else {
        mpmc::channel()
    }
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

/// Extension trait to provide the `filter_map_reduce_async_commutative` function
/// for iterators.
pub trait FilterMapReduceAsyncCommutative: Iterator {
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
    ///     .filter_map_reduce_async_commutative(
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
    fn filter_map_reduce_async_commutative<F, R, O>(self, f: F, r: R) -> Option<O>
    where
        Self::Item: Send + Sync,
        O: Send + Sync,
        F: Fn(Self::Item) -> Option<O> + Send + Sync,
        R: Fn(O, O) -> O + Send + Sync;
}

impl<T> FilterMapReduceAsyncCommutative for T
where
    T: Iterator + Send + Sync,
{
    fn filter_map_reduce_async_commutative<F, R, O>(self, f: F, r: R) -> Option<O>
    where
        Self::Item: Send + Sync,
        O: Send + Sync,
        F: Fn(Self::Item) -> Option<O> + Send + Sync,
        R: Fn(O, O) -> O + Send + Sync,
    {
        scope(|scope| {
            self.filter_map_async_unordered(f, scope)
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
    /// [`FilterMapReduceAsyncCommutative::filter_map_reduce_async_commutative`] instead,
    /// which is more efficient.
    ///
    /// If you require the reducer to be noncommutative and nonassociative, then simply
    /// chain [`FilterMapAsync::filter_map_async`] with
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
        scope(|scope| self.filter_map_async(f, scope).reduce_async(r))
    }
}

/// Generic trait representing a thread pool.
///
/// See [`Threadpool`] and [`OrderedThreadpool`] for specific implementations.
pub trait GenericThreadpool<'scope, I, O>:
    IntoIterator<Item = O> + Extend<I> + Sized + Send + Sync
{
    type Iter<'a>: Iterator<Item = O> + 'a
    where
        Self: 'a;
    type JoinHandle;

    /// Synchronously submit a single job to the pool.
    fn submit(&self, input: I);

    /// Synchronously submit many jobs to the pool at once from
    /// an iterator or collection.
    fn submit_all<T>(&self, iter: T)
    where
        T: IntoIterator<Item = I>,
    {
        iter.into_iter().for_each(|x| self.submit(x));
    }

    /// Block the current thread until a result from the
    /// pool is available, and return it.
    fn recv(&self) -> O;

    /// Check if a result is available from the pool;
    /// if so, return it. If not, returns `None`.
    fn try_recv(&self) -> Option<O>;

    /// Iterate over the currently available results in the pool.
    ///
    /// Does not consume the pool; it only yields results until
    /// there are no jobs left to process. If more jobs are submitted
    /// to the pool afterwards, those results may be subsequently iterated
    /// over as well.
    ///
    /// It's recommended that you call [`GenericThreadpool::wait_until_finished`]
    /// before iterating, otherwise the cpu may be stuck in a busy wait while
    /// lingering jobs are still being processed.
    fn iter(&self) -> Self::Iter<'_>;

    /// Block until all producers have been exhausted, and all workers
    /// have finished processing all jobs.
    fn wait_until_finished(&self);

    /// Spawn a new producer thread, which supplies jobs
    /// to the pool from the passed iterator asynchronously
    /// on a separate thread.
    fn producer<T>(&self, iter: T) -> Self::JoinHandle
    where
        T: IntoIterator<Item = I> + Send + Sync + 'scope;

    /// Spawn a new consumer thread, which consumes and processes
    /// results from the workers asynchronously on a separate thread.
    fn consumer<F>(&self, f: F) -> Self::JoinHandle
    where
        F: Fn(O) + Sync + Send + 'scope;
}

/// Extension trait to provide the `pipe` method
/// for iterators.
pub trait Pipe<P, O> {
    /// Pipe the result of an iterator into a thread pool.
    ///
    /// Can be used multiple times to chain several thread pools together in a row.
    ///
    /// When chained together in this manner, all pools work simultaneously to process
    /// elements at once.
    ///
    /// ```
    /// use threadpools::*;
    /// use std::thread::scope;
    ///
    /// scope(|scope| {
    ///     let sequential_result = (0..10000usize)
    ///         .filter_map(|x: usize| {
    ///             let x = x.pow(3) % 100;
    ///             (x > 50).then_some(x)
    ///         })
    ///         .reduce(|a, b| a + b)
    ///         .unwrap();
    ///
    ///     let parallel_result = (0..10000usize)
    ///         .pipe(Threadpool::new(|x: usize| Some(x.pow(3)), scope))
    ///         .pipe(Threadpool::new(|x: usize| Some(x % 100), scope))
    ///         .pipe(Threadpool::new(|x: usize| (x > 50).then_some(x), scope))
    ///         .into_iter()
    ///         .reduce_async_commutative(|a, b| a + b)
    ///         .unwrap();
    ///
    ///     assert_eq!(sequential_result, parallel_result);
    /// });
    /// ```
    fn pipe(self, target: P) -> P;
}

impl<'scope, P, T, I, O> Pipe<P, O> for T
where
    T: IntoIterator<Item = I> + Sized + Send + Sync + 'scope,
    P: GenericThreadpool<'scope, I, O>,
{
    fn pipe(self, target: P) -> P {
        target.producer(self);
        target
    }
}
