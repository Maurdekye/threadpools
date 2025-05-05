//! Standard, unordered thread pool implementation.
//!
//! See [`Threadpool`] for more detailed documentation.

use std::{
    num::NonZeroUsize,
    sync::{
        Arc,
        mpmc::{IntoIter, Receiver, Sender},
        mpsc::TryRecvError,
    },
    thread::{self, Scope, ScopedJoinHandle},
};

use crate::{Gate, GenericThreadpool, channel, num_cpus};

// imports for documentation
#[allow(unused_imports)]
use crate::ordered::{FilterMapAsync, OrderedThreadpool};
#[allow(unused_imports)]
use std::thread::available_parallelism;

/// A thread pool.
///
/// Yields results as soon as possible, in arbitrary output order.
///
/// If retaining the original order of submitted jobs is desired, see [`OrderedThreadpool`].
///
/// Allows the multithreaded processing of elements, either by submitting
/// jobs one at a time via [`Threadpool::submit`], via an iterator with [`Threadpool::submit_all`],
/// or via a producer on a separate thread with [`Threadpool::producer`].
///
/// The results can be gathered either directly via [`Threadpool::recv`] or [`Threadpool::try_recv`],
/// via iteration with [`Threadpool::iter`] or the [`IntoIterator`] implementation,
/// or via a consumer on a separate thread with [`Threadpool::consumer`].
///
/// The worker function receives an input element `I`, and returns an
/// output of `Option<O>`. If it returns `Some(O)`, then the result
/// is yielded as an output. If it returns `None`, the result is
/// ignored. In this way, the [`Threadpool`] can be used as a map and a filter simultaneously.
///
/// If you desire more detailed configuration, create your [`Threadpool`] using a [`ThreadpoolBuilder`]
/// instead of the basic `new` constructor, which allows you to set various properties and flags,
/// including the number of workers, blocking properties, and an initializer function.
///
/// It's recommended that, unless you desire to produce results from the pool asynchronously,
/// you should call [`Threadpool::wait_until_finished`] before iterating over the results,
/// to ensure all work has finished being processed. Note: consuming the pool via the [`IntoIterator`]
/// implementation is guaranteed to eventually return all results before finishing, and also does
/// so asynchronously, as they become available.
///
/// If a consumer has been registered via [`Threadpool::consumer`], you will have to construct
/// your own synchronization system to confirm whether or not work has been finished processing by all consumers.
/// [`Threadpool::wait_until_finished`] only guarantees all producers and workers have finished processing.
///
/// The [`Threadpool`] is designed to be reusable, to reduce the need to repeatedly create and destroy threads over
/// long-term usage. This is the primary reason there is no synchronization machinery to check if any registered
/// consumers have finished processing; conceptually, there is no practical way to distinguish between a consumer
/// that has truly exhausted all results from the worker pool, and a consumer that is simply waiting for the next
/// element to arrive from a worker that is still undergoing processing.
pub struct Threadpool<'scope, 'env, I, O> {
    scope: &'scope Scope<'scope, 'env>,
    #[allow(unused)]
    workers: Vec<ScopedJoinHandle<'scope, ()>>,
    in_flight: Gate<usize>,
    producers_active: Gate<usize>,
    work_submission: Sender<I>,
    work_reception: Receiver<O>,
}

impl<'scope, 'env, I, O> Threadpool<'scope, 'env, I, O> {
    /// Construct a new [`Threadpool`].
    ///
    /// Provide a function that workers will use to process jobs,
    /// and a [`Scope`] that the pool will use to spawn worker threads
    /// and any producer / consumer threads.
    ///
    /// By default, the number of workers spawned is determined by the
    /// result of [`available_parallelism`].
    /// To specify the specific number of workers to spawn, and configure
    /// several additional other custom properties, including an initializer
    /// and blocking properties, construct your pool via a
    /// [`ThreadpoolBuilder`] instead.
    pub fn new<F>(f: F, scope: &'scope Scope<'scope, 'env>) -> Self
    where
        F: Fn(I) -> Option<O> + Send + Sync + 'scope,
        I: Send + Sync + 'scope,
        O: Send + Sync + 'scope,
    {
        ThreadpoolBuilder::new().build(move |x, _| f(x), scope)
    }

    fn new_inner<F, N, M>(
        f: F,
        initializer: N,
        scope: &'scope Scope<'scope, 'env>,
        num_workers: NonZeroUsize,
        blocking_submission: bool,
        blocking_workers: bool,
    ) -> Self
    where
        F: Fn(I, (&mut M, usize)) -> Option<O> + Send + Sync + 'scope,
        N: Fn(usize) -> M + Send + Sync + 'scope,
        I: Send + Sync + 'scope,
        O: Send + Sync + 'scope,
    {
        let (work_submission, inbox) = channel(blocking_submission);
        let (outbox, work_reception) = channel(blocking_workers);
        let in_flight = Gate::new(0usize);
        let producers_active = Gate::new(0);
        let f = Arc::new(f);
        let initializer = Arc::new(initializer);
        let workers = (0..num_workers.into())
            .map(|id| {
                let inbox = inbox.clone();
                let outbox = outbox.clone();
                let in_flight = in_flight.clone();
                let f = f.clone();
                let initializer = initializer.clone();
                scope.spawn(move || {
                    let mut thread_state = initializer(id);
                    for item in inbox {
                        let result = f(item, (&mut thread_state, id));
                        if let Some(result) = result {
                            outbox.send(result).unwrap();
                        }
                        in_flight.update(|x| *x = x.saturating_sub(1));
                    }
                })
            })
            .collect();
        Self {
            workers,
            work_submission,
            work_reception,
            in_flight,
            producers_active,
            scope,
        }
    }
}

impl<'scope, 'env, I, O> GenericThreadpool<'scope, I, O> for Threadpool<'scope, 'env, I, O>
where
    I: Send + Sync + 'scope,
    O: Send + Sync + 'scope,
{
    type Iter<'a>
        = ThreadpoolIter<'a, 'scope, 'env, I, O>
    where
        Self: 'a;

    type JoinHandle = ScopedJoinHandle<'scope, ()>;

    fn submit(&self, input: I) {
        self.in_flight.update(|x| *x += 1);
        self.work_submission.send(input).unwrap()
    }

    fn recv(&self) -> O {
        self.work_reception.recv().unwrap()
    }

    fn try_recv(&self) -> Option<O> {
        match self.work_reception.try_recv() {
            Ok(val) => Some(val),
            Err(TryRecvError::Empty) => None,
            Err(err) => panic!("{err}"),
        }
    }

    fn iter(&self) -> Self::Iter<'_> {
        ThreadpoolIter(self)
    }

    fn wait_until_finished(&self) {
        self.producers_active.wait_while(|x| *x > 0);
        self.in_flight.wait_while(|x| *x > 0);
    }

    fn producer<T>(&self, iter: T) -> ScopedJoinHandle<'scope, ()>
    where
        T: IntoIterator<Item = I> + Send + Sync + 'scope,
    {
        self.producers_active.update(|x| *x += 1);
        let in_flight = self.in_flight.clone();
        let work_submission = self.work_submission.clone();
        let producers_active = self.producers_active.clone();
        self.scope.spawn(move || {
            for item in iter {
                in_flight.update(|x| *x += 1);
                work_submission.send(item).unwrap();
            }
            producers_active.update(|x| *x = x.saturating_sub(1));
        })
    }

    fn consumer<F>(&self, f: F) -> ScopedJoinHandle<'scope, ()>
    where
        O: Send + Sync + 'scope,
        F: Fn(O) + Sync + Send + 'scope,
    {
        let work_reception = self.work_reception.clone();
        self.scope.spawn(move || {
            for item in work_reception {
                f(item);
            }
        })
    }
}

/// A builder for a [`Threadpool`].
///
/// Allows the custom configuration of several pool properties.
///
/// Refer to individual method implementations for more details.
///
/// ```rust
/// use std::thread::scope;
/// use std::num::NonZeroUsize;
/// use threadpools::*;
///
/// scope(|s| {
///     let pool = ThreadpoolBuilder::new()
///         .num_workers(NonZeroUsize::new(4).unwrap())
///         .blocking_submission()
///         .build(|x: i32, _| Some(x * x), s);
///
///     pool.submit_all(1..=4);
///
///     pool.wait_until_finished();
///
///     let mut results: Vec<_> = pool.iter().collect();
///     results.sort();
///
///     assert_eq!(results, vec![1, 4, 9, 16]);
/// });
/// ```
#[derive(Clone, Debug)]
pub struct ThreadpoolBuilder {
    num_workers: NonZeroUsize,
    blocking_submission: bool,
    blocking_workers: bool,
}

impl ThreadpoolBuilder {
    /// Construct a new builder with default properties.
    ///
    /// Calling [`ThreadpoolBuilder::build`] immediately on this
    /// constructs the same threadpool as calling [`Threadpool::new`].
    pub fn new() -> ThreadpoolBuilder {
        ThreadpoolBuilder {
            num_workers: num_cpus(),
            blocking_submission: false,
            blocking_workers: false,
        }
    }

    /// Set an initializer function.
    ///
    /// This function is run once per worker thread, and is used to
    /// initialize any thread-local state that the worker function may need. It is
    /// passed the thread id as an argument, and should return a value representing the
    /// thread-local state.
    pub fn initializer<I, M>(self, initializer: I) -> InitializedThreadpoolBuilder<I>
    where
        I: Fn(usize) -> M,
    {
        let ThreadpoolBuilder {
            num_workers,
            blocking_submission,
            blocking_workers,
        } = self;
        InitializedThreadpoolBuilder {
            initializer,
            num_workers,
            blocking_submission,
            blocking_workers,
        }
    }

    /// Set the specific number of worker threads to spawn.
    ///
    /// By default, the number of pool workers is the same as the
    /// value returned by [`available_parallelism`].
    pub fn num_workers(self, num_workers: NonZeroUsize) -> Self {
        Self {
            num_workers,
            ..self
        }
    }

    /// Enable blocking job submission.
    ///
    /// By default, jobs are be submitted instantly
    /// without blocking, and placed in a work queue.
    /// If enabled, newly submitted jobs will block until
    /// a worker is available to process them.
    pub fn blocking_submission(self) -> Self {
        Self {
            blocking_submission: true,
            ..self
        }
    }

    /// Enable blocking work result submission.
    ///
    /// By default, when workers finish processing a job,
    /// the result is placed in a result queue, and the worker
    /// may continue picking up new jobs immediately. If this is
    /// enabled, workers will block upon finishing their current
    /// job until the result is retrieved from the pool.
    pub fn blocking_workers(self) -> Self {
        Self {
            blocking_workers: true,
            ..self
        }
    }

    /// Construct a new [`Threadpool`] from the builder.
    ///
    /// ## Args
    ///
    /// * `f` - The function that workers will use to process jobs. It takes
    /// two arguments: the input job `I`, and a `usize` thread id.
    ///
    /// `scope` - The [`Scope`] that the pool will use to spawn worker threads
    /// and any producer / consumer threads.
    pub fn build<'scope, 'env, F, I, O>(
        self,
        f: F,
        scope: &'scope Scope<'scope, 'env>,
    ) -> Threadpool<'scope, 'env, I, O>
    where
        F: Fn(I, usize) -> Option<O> + Send + Sync + 'scope,
        I: Send + Sync + 'scope,
        O: Send + Sync + 'scope,
    {
        Threadpool::new_inner(
            move |i, (_, id)| f(i, id),
            |_| (),
            scope,
            self.num_workers,
            self.blocking_submission,
            self.blocking_workers,
        )
    }
}

/// A builder for a [`Threadpool`] with an initializer configured.
///
/// Allows the custom configuration of several pool properties.
///
/// Refer to individual method implementations for more details.
///
/// ```rust
/// use std::thread::scope;
/// use std::num::NonZeroUsize;
/// use threadpools::*;
///
/// scope(|s| {
///     let pool = ThreadpoolBuilder::new()
///         .num_workers(NonZeroUsize::new(4).unwrap())
///         .blocking_submission()
///         .build(|x: i32, _| Some(x * x), s);
///
///     pool.submit_all(1..=4);
///
///     pool.wait_until_finished();
///
///     let mut results: Vec<_> = pool.iter().collect();
///     results.sort();
///
///     assert_eq!(results, vec![1, 4, 9, 16]);
/// });
/// ```
pub struct InitializedThreadpoolBuilder<N> {
    initializer: N,
    num_workers: NonZeroUsize,
    blocking_submission: bool,
    blocking_workers: bool,
}

impl<N> InitializedThreadpoolBuilder<N> {
    /// Set a new initializer function.
    ///
    /// This function is run once per worker thread, and is used to
    /// initialize any thread-local state that the worker function may need. It is
    /// passed the thread id as an argument, and should return a value representing the
    /// thread-local state.
    ///
    /// *(why would you call this again? you already set an initializer...)*
    pub fn initializer<I, M>(self, initializer: I) -> InitializedThreadpoolBuilder<I>
    where
        I: Fn(usize) -> M,
    {
        InitializedThreadpoolBuilder {
            initializer,
            ..self
        }
    }

    /// Set the specific number of worker threads to spawn.
    ///
    /// By default, the number of pool workers is the same as the
    /// value returned by [`available_parallelism`].
    pub fn num_workers(self, num_workers: NonZeroUsize) -> Self {
        Self {
            num_workers,
            ..self
        }
    }

    /// Enable blocking job submission.
    ///
    /// By default, jobs are be submitted instantly
    /// without blocking, and placed in a work queue.
    /// If enabled, newly submitted jobs will block until
    /// a worker is available to process them.
    pub fn blocking_submission(self) -> Self {
        Self {
            blocking_submission: true,
            ..self
        }
    }

    /// Enable blocking work result submission.
    ///
    /// By default, when workers finish processing a job,
    /// the result is placed in a result queue, and the worker
    /// may continue picking up new jobs immediately. If this is
    /// enabled, workers will block upon finishing their current
    /// job until the result is retrieved from the pool.
    pub fn blocking_workers(self) -> Self {
        Self {
            blocking_workers: true,
            ..self
        }
    }

    /// Construct a new [`Threadpool`] from the builder.
    ///
    /// ## Args
    ///
    /// * `f` - The function that workers will use to process jobs. It takes
    /// two arguments: the input job `I`, and a tuple of thread metadata.
    /// The metadata tuple consists of two values itself: a mutable reference
    /// to the value returned by your initializer, and a `usize` thread id.
    ///
    /// `scope` - The [`Scope`] that the pool will use to spawn worker threads
    /// and any producer / consumer threads.
    pub fn build<'scope, 'env, F, I, O, M>(
        self,
        f: F,
        scope: &'scope Scope<'scope, 'env>,
    ) -> Threadpool<'scope, 'env, I, O>
    where
        F: Fn(I, (&mut M, usize)) -> Option<O> + Send + Sync + 'scope,
        N: Fn(usize) -> M + Send + Sync + 'scope,
        I: Send + Sync + 'scope,
        O: Send + Sync + 'scope,
    {
        Threadpool::new_inner(
            f,
            self.initializer,
            scope,
            self.num_workers,
            self.blocking_submission,
            self.blocking_workers,
        )
    }
}

/// An iterator over a [`Threadpool`].
///
/// Does not consume the pool; it only yields results until
/// there are no jobs left to process. If more jobs are submitted
/// to the pool afterwards, those results may be subsequently iterated
/// over as well.
///
/// It's recommended that you call [`Threadpool::wait_until_finished`]
/// before iterating, otherwise the cpu may be stuck in a busy wait while
/// lingering jobs are still being processed.
pub struct ThreadpoolIter<'a, 'scope, 'env, I, O>(&'a Threadpool<'scope, 'env, I, O>);

impl<'a, 'scope, 'env, I, O> Iterator for ThreadpoolIter<'a, 'scope, 'env, I, O> {
    type Item = O;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.0.work_reception.try_recv() {
                Ok(item) => return Some(item),
                Err(TryRecvError::Empty) => {
                    if self.0.in_flight.check() == 0 && self.0.producers_active.check() == 0 {
                        return None;
                    }
                }
                Err(err) => panic!("{err}"),
            }
            thread::yield_now();
        }
    }
}

impl<'scope, 'env, I, O> Extend<I> for Threadpool<'scope, 'env, I, O>
where
    I: Send + Sync + 'scope,
    O: Send + Sync + 'scope,
{
    fn extend<T: IntoIterator<Item = I>>(&mut self, iter: T) {
        self.submit_all(iter);
    }
}

impl<'scope, 'env, I, O> IntoIterator for Threadpool<'scope, 'env, I, O> {
    type Item = O;

    type IntoIter = IntoIter<O>;

    fn into_iter(self) -> Self::IntoIter {
        drop(self.work_submission);
        self.work_reception.into_iter()
    }
}

/// Extension trait to provide the `filter_map_async_unordered` function
/// for iterators.
pub trait FilterMapAsyncUnordered<'scope> {
    /// Constructs a new
    /// [`Threadpool`] and uses it to map the iterator
    /// with the passed function in parallel.
    ///
    /// Yields items in arbitrary order as they finish processing, as soon
    /// as possible. Does not preserve the original order of the input iterator.
    ///
    /// Requires a [`Scope`] be passed in order to encapsulate
    /// the lifetime of the threads that the pool spawns, which is
    /// necessary in order to serve results asynchronously as they arrive.
    ///
    /// For a version of this function that retains the original
    /// ordering of the input iterator, see
    /// [`FilterMapAsync::filter_map_async`]
    fn filter_map_async_unordered<'env, F, O>(
        self,
        f: F,
        scope: &'scope Scope<'scope, 'env>,
    ) -> impl Iterator<Item = O> + 'scope
    where
        O: Send + Sync + 'scope,
        Self: Iterator,
        Self::Item: Send + Sync + 'scope,
        F: Fn(Self::Item) -> Option<O> + Send + Sync + 'scope;
}

impl<'scope, T> FilterMapAsyncUnordered<'scope> for T
where
    T: Iterator + Send + Sync + 'scope,
{
    fn filter_map_async_unordered<'env, F, O>(
        self,
        f: F,
        scope: &'scope Scope<'scope, 'env>,
    ) -> impl Iterator<Item = O> + 'scope
    where
        O: Send + Sync + 'scope,
        Self: Iterator + 'scope,
        T::Item: Send + Sync + 'scope,
        F: Fn(T::Item) -> Option<O> + Send + Sync + 'scope,
    {
        let pool = Threadpool::new(f, scope);
        pool.producer(self);
        pool.into_iter()
    }
}
