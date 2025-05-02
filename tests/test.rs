use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread::{self, scope};
use std::time::Duration;
use threadpool::{
    FilterMapMultithread, FilterMapMultithreadAsync, FilterMapReduceAsync, OrderedThreadpool,
    ReduceAsync, Threadpool,
};

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

#[test]
fn sync_processing() {
    scope(|scope| {
        let pool = Threadpool::new(|x| is_prime(x).then_some(x), scope);
        pool.submit_all(0..1000);
        let total: usize = pool.into_iter().sum();
        assert_eq!(total, 76127);
    })
}

#[test]
fn async_processing() {
    let total = AtomicUsize::new(0);
    scope(|scope| {
        let pool = Threadpool::new(|x| is_prime(x).then_some(x), scope);
        pool.producer(0..1000);
        pool.consumer(|x| {
            total.fetch_add(x, Ordering::SeqCst);
        });
        pool.wait_until_finished();
        // unavoidable necessity to wait for the consumer to finish running;
        // this is a nonstandard usage of the consumer
        thread::sleep(Duration::from_millis(250));
        let total = total.load(Ordering::Acquire);
        assert_eq!(total, 76127);
    })
}

#[test]
fn sync_ordered_processing() {
    scope(|scope| {
        let items = 0..100;
        let pool_result: Vec<_> = OrderedThreadpool::new(|x: usize| Some(x.pow(3)), scope)
            .filter_map(items.clone())
            .collect();
        let sync_result: Vec<_> = items.map(|x| x.pow(3)).collect();
        assert_eq!(pool_result, sync_result);
    })
}

#[test]
fn async_ordered_processing() {
    scope(|scope| {
        let items = 0..100;
        let pool_result: Vec<_> = OrderedThreadpool::new(|x: usize| Some(x.pow(3)), scope)
            .filter_map_async(items.clone())
            .collect();
        let sync_result: Vec<_> = items.map(|x| x.pow(3)).collect();
        assert_eq!(pool_result, sync_result);
    })
}

#[test]
fn async_trait_processing() {
    scope(|scope| {
        let items = 0..100usize;
        let pool_result: Vec<_> = items
            .clone()
            .filter_map_multithread_async(|x| Some(x.pow(3)), scope)
            .collect();
        let sync_result: Vec<_> = items.map(|x| x.pow(3)).collect();
        assert_eq!(pool_result, sync_result);
    })
}

#[test]
fn async_no_scope_trait_processing() {
    let items = 0..100usize;
    let pool_result = items.clone().filter_map_multithread(|x| Some(x.pow(3)));
    let sync_result: Vec<_> = items.map(|x| x.pow(3)).collect();
    assert_eq!(pool_result, sync_result);
}

#[test]
fn reduction() {
    let vals = (0..10000).map(|x: usize| x.pow(3) % 100);
    let sequential_result = vals.clone().reduce(|a, b| a + b);
    let parallel_result = vals.reduce_async(|a, b| a + b);
    assert_eq!(sequential_result, parallel_result);
}

#[test]
fn map_filter_reduce() {
    scope(|scope| {
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
            .filter_map_multithread_async(
                |x: usize| {
                    let x = x.pow(3) % 100;
                    (x > 50).then_some(x)
                },
                scope,
            )
            .reduce_async(|a, b| a + b)
            .unwrap();

        assert_eq!(sequential_result, parallel_result);
    });
}

#[test]
fn map_filter_reduce_unordered() {
    scope(|scope| {
        let vals = 0..10000usize;

        let sequential_result = vals
            .clone()
            .filter_map(|x: usize| {
                let x = x.pow(3) % 100;
                (x > 50).then_some(x)
            })
            .reduce(|a, b| a + b)
            .unwrap();

        let pool = Threadpool::new(
            |x: usize| {
                let x = x.pow(3) % 100;
                (x > 50).then_some(x)
            },
            scope,
        );
        pool.producer(vals);
        let parallel_result = pool.into_iter().reduce_async(|a, b| a + b).unwrap();

        assert_eq!(sequential_result, parallel_result);
    });
}

#[test]
fn map_filter_reduce_trait() {
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
        .filter_map_reduce_async(
            |x: usize| {
                let x = x.pow(3) % 100;
                (x > 50).then_some(x)
            },
            |a, b| a + b,
        )
        .unwrap();

    assert_eq!(sequential_result, parallel_result);
}
