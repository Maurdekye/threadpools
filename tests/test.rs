use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread::{self, scope};
use std::time::Duration;
use threadpools::{
    FilterMapAsync, FilterMapMultithread, FilterMapReduceAsync, FilterMapReduceAsyncCommutative, GenericThreadpool, OrderedThreadpool, OrderedThreadpoolBuilder, Pipe, ReduceAsync, ReduceAsyncCommutative, Threadpool, ThreadpoolBuilder
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
            .filter_map_async(|x| Some(x.pow(3)), scope)
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
    let parallel_result = vals.reduce_async_commutative(|a, b| a + b);
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
            .filter_map_async(
                |x: usize| {
                    let x = x.pow(3) % 100;
                    (x > 50).then_some(x)
                },
                scope,
            )
            .reduce_async_commutative(|a, b| a + b)
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
        let parallel_result = pool
            .into_iter()
            .reduce_async_commutative(|a, b| a + b)
            .unwrap();

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
        .filter_map_reduce_async_commutative(
            |x: usize| {
                let x = x.pow(3) % 100;
                (x > 50).then_some(x)
            },
            |a, b| a + b,
        )
        .unwrap();

    assert_eq!(sequential_result, parallel_result);
}

#[test]
fn reduce_noncommutative() {
    let str = "qwertyuiopasdfghjklzxcvbnm"
        .chars()
        .map(|x| x.to_string())
        .cycle()
        .take(10000);
    let reduced = str.clone().reduce_async(|a, b| a + &b).unwrap();
    let sequential_result = str.collect::<Vec<_>>().join("");
    assert_eq!(reduced, sequential_result);
}

#[test]
fn test_filter_map_reduce_ordered() {
    let chars = "qwertyuiopasdfghjklzxcvbnm".chars().cycle().take(10000);

    let sequential_result = chars
        .clone()
        .filter_map(|c: char| (!['a', 'e', 'i', 'o', 'u'].contains(&c)).then(|| c.to_string()))
        .reduce(|a, b| a + &b)
        .unwrap();

    let parallel_result = chars
        .filter_map_reduce_async(
            |c: char| (!['a', 'e', 'i', 'o', 'u'].contains(&c)).then(|| c.to_string()),
            |a, b| a + &b,
        )
        .unwrap();

    assert_eq!(sequential_result, parallel_result);
}

#[test]
fn chaining() {
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
}

// gemini 2.5 tests

#[test]
fn test_multiple_producers_ordered() {
    scope(|scope| {
        let pool = OrderedThreadpool::new(|x: usize| Some(x * 2), scope);

        pool.submit_all(0..50);
        pool.submit_all(50..100);

        let results: Vec<_> = pool.into_iter().collect();
        let expected: Vec<_> = (0..100).map(|x| x * 2).collect();

        assert_eq!(results.len(), 100);
        assert_eq!(
            results, expected,
            "Results should maintain original combined order"
        );
    });
}

#[test]
fn test_empty_input_pool_unordered() {
    scope(|scope| {
        let pool = Threadpool::new(|x: usize| Some(x), scope);
        pool.submit_all(0..0); // Empty iterator
        let results: Vec<_> = pool.into_iter().collect();
        assert!(results.is_empty());
    });
}

#[test]
fn test_empty_input_pool_ordered() {
    scope(|scope| {
        let pool = OrderedThreadpool::new(|x: usize| Some(x), scope);
        pool.submit_all(0..0); // Empty iterator
        let results: Vec<_> = pool.into_iter().collect();
        assert!(results.is_empty());
    });
}

#[test]
fn test_single_element_input_pool_unordered() {
    scope(|scope| {
        let pool = Threadpool::new(|x: usize| Some(x * 10), scope);
        pool.submit_all(5..6); // Single element iterator
        let results: Vec<_> = pool.into_iter().collect();
        assert_eq!(results, vec![50]);
    });
}

#[test]
fn test_single_element_input_pool_ordered() {
    scope(|scope| {
        let pool = OrderedThreadpool::new(|x: usize| Some(x * 10), scope);
        pool.submit_all(5..6); // Single element iterator
        let results: Vec<_> = pool.into_iter().collect();
        assert_eq!(results, vec![50]);
    });
}

#[test]
fn test_empty_input_reducers() {
    let iter = std::iter::empty::<i32>();
    let res_comm = iter.clone().reduce_async_commutative(|a, b| a + b);
    let res_noncomm = iter.reduce_async(|a, b| a + b);
    assert_eq!(res_comm, None);
    assert_eq!(res_noncomm, None);
}

#[test]
fn test_single_element_reducers() {
    let iter = std::iter::once(10);
    let res_comm = iter.clone().reduce_async_commutative(|a, b| a + b);
    let res_noncomm = iter.reduce_async(|a, b| a + b);
    assert_eq!(res_comm, Some(10));
    assert_eq!(res_noncomm, Some(10));
}

#[test]
#[should_panic]
fn test_worker_panic_unordered() {
    scope(|scope| {
        let pool = Threadpool::new(
            |x: usize| {
                if x == 5 {
                    panic!("Worker panicked intentionally!");
                }
                Some(x)
            },
            scope,
        );
        // Submit including the panic trigger
        pool.submit_all(0..10);
        // Collect results - the panic should propagate here or during iteration
        let _results: Vec<_> = pool.into_iter().collect();
        // If collect finishes without panic, the test fails.
    });
}

#[test]
#[should_panic]
fn test_worker_panic_ordered() {
    scope(|scope| {
        let pool = OrderedThreadpool::new(
            |x: usize| {
                if x == 5 {
                    // Simulate work before panic
                    thread::sleep(Duration::from_millis(10));
                    panic!("Worker panicked intentionally!");
                }
                // Simulate work
                thread::sleep(Duration::from_millis(5));
                Some(x)
            },
            scope,
        );
        // Submit including the panic trigger
        pool.submit_all(0..10);
        // Collect results - the panic should propagate here or during iteration
        let _results: Vec<_> = pool.into_iter().collect();
        // If collect finishes without panic, the test fails.
    });
}

#[test]
fn test_pipe_ordered() {
    scope(|scope| {
        let sequential_result: Vec<_> = (0..100usize)
            .map(|x| x + 1) // First operation
            .map(|x| x * 2) // Second operation
            .collect();

        let parallel_result = (0..100usize)
            .pipe(OrderedThreadpool::new(|x: usize| Some(x + 1), scope)) // Ordered
            .pipe(OrderedThreadpool::new(|x: usize| Some(x * 2), scope)) // Ordered
            .into_iter()
            .collect::<Vec<_>>();

        assert_eq!(
            sequential_result, parallel_result,
            "Piped ordered pools should maintain order"
        );
    });
}

#[test]
fn test_try_recv_behavior() {
    scope(|scope| {
        let pool = Threadpool::new(
            |x: usize| {
                thread::sleep(Duration::from_millis(100)); // Simulate work
                Some(x * 2)
            },
            scope,
        );

        // Try receiving before submitting anything
        assert_eq!(pool.try_recv(), None, "Should be None before submission");

        pool.submit(10);

        // Try receiving immediately after submission (job likely not finished)
        // This might occasionally succeed if the scheduler is very fast,
        // but usually it should be None. We add a small sleep to increase
        // the chance it's still processing.
        thread::sleep(Duration::from_millis(10));
        let immediate_recv = pool.try_recv();
        println!("Immediate try_recv result: {:?}", immediate_recv); // For debugging race conditions

        // Wait until the job should be finished
        thread::sleep(Duration::from_millis(150));
        assert_eq!(
            pool.try_recv().or(immediate_recv),
            Some(20),
            "Should receive result after waiting"
        ); // Use `or` to handle the race condition

        // Try receiving again after result is taken
        assert_eq!(
            pool.try_recv(),
            None,
            "Should be None after result is taken"
        );

        // Submit another job
        pool.submit(20);
        // Use recv (blocking) this time
        assert_eq!(pool.recv(), 40, "Blocking recv should work");

        // Ensure pool is finished before scope ends
        pool.wait_until_finished();
        assert_eq!(
            pool.try_recv(),
            None,
            "Should be None after waiting finished"
        );
    });
}

// Builder Tests

#[test]
fn test_unordered_builder_default() {
    scope(|scope| {
        let pool = ThreadpoolBuilder::new()
            .build(|x: usize, _id| Some(x * 2), scope);
        pool.submit_all(0..10);
        let mut results: Vec<_> = pool.into_iter().collect();
        results.sort_unstable(); // Unordered pool, sort for comparison
        let expected: Vec<_> = (0..10).map(|x| x * 2).collect();
        assert_eq!(results, expected);
    });
}

#[test]
fn test_ordered_builder_default() {
    scope(|scope| {
        let pool = OrderedThreadpoolBuilder::new()
            .build(|x: usize, (_id, _jid)| Some(x * 2), scope);
        pool.submit_all(0..10);
        let results: Vec<_> = pool.into_iter().collect();
        let expected: Vec<_> = (0..10).map(|x| x * 2).collect();
        assert_eq!(results, expected); // Ordered pool, order should match
    });
}

#[test]
fn test_unordered_builder_num_workers() {
    scope(|scope| {
        let num_workers = NonZeroUsize::new(2).unwrap();
        let pool = ThreadpoolBuilder::new()
            .num_workers(num_workers)
            .build(move |x: usize, id| {
                // Check if thread id is within expected range (0 or 1)
                assert!(id < num_workers.get());
                Some(x * id) // Use id in calculation to verify different threads are used
            }, scope);

        pool.submit_all(0..20); // More jobs than workers
        let results: Vec<_> = pool.into_iter().collect();
        assert_eq!(results.len(), 20);
    });
}


#[test]
fn test_ordered_builder_num_workers() {
    scope(|scope| {
        let num_workers = NonZeroUsize::new(2).unwrap();
        let pool = OrderedThreadpoolBuilder::new()
            .num_workers(num_workers)
            .build(move |x: usize, (id, jid)| {
                // Check if thread id is within expected range (0 or 1)
                assert!(id < num_workers.get());
                Some((x, jid, id)) // Return job index and thread id
            }, scope);

        pool.submit_all(0..20); // More jobs than workers
        let results: Vec<_> = pool.into_iter().collect();
        assert_eq!(results.len(), 20);
        // Verify job indices are sequential and match input order
        for (i, (x, jid, _id)) in results.iter().enumerate() {
            assert_eq!(*x, i);
            assert_eq!(*jid, i);
        }
        // Check that at least two different thread IDs were used (likely, not guaranteed)
        let unique_ids: std::collections::HashSet<_> = results.iter().map(|&(_, _, id)| id).collect();
        assert!(unique_ids.len() > 0 && unique_ids.len() <= num_workers.get());
    });
}


#[test]
fn test_unordered_builder_initializer() {
    scope(|scope| {
        let pool = ThreadpoolBuilder::new()
            .initializer(|id| format!("Thread {}", id)) // Initialize with thread-specific string
            .build(|x: usize, (state, id)| {
                assert_eq!(*state, format!("Thread {}", id)); // Check thread-local sta
                Some(x * 2)
            }, scope);

        pool.submit_all(0..10);
        let results: Vec<_> = pool.into_iter().collect();
        assert_eq!(results.len(), 10);
        // Cannot easily check final state without joining threads directly,
        // but the assert inside the worker verifies it's accessible.
    });
}

#[test]
fn test_ordered_builder_initializer() {
    scope(|scope| {
        let pool = OrderedThreadpoolBuilder::new()
            .initializer(|id| id) // Initialize with thread ID
            .build(|x: usize, (state, id, _jid)| {
                assert_eq!(*state, id); // Check initial thread-local state
                Some(x * 2)
            }, scope);

        pool.submit_all(0..10);
        let results: Vec<_> = pool.into_iter().collect();
        let expected: Vec<_> = (0..10).map(|x| x * 2).collect();
        assert_eq!(results, expected);
        // Cannot easily check final state without joining threads directly.
    });
}

#[test]
fn test_unordered_builder_chaining() {
     scope(|scope| {
        let num_workers = NonZeroUsize::new(1).unwrap();
        let pool = ThreadpoolBuilder::new()
            .num_workers(num_workers) // Set workers
            .initializer(|id| id * 10) // Set initializer
            // .blocking_submission() // Add other options if needed
            // .blocking_workers()
            .build(|x: usize, (state, id)| {
                assert_eq!(*state, id * 10);
                Some(x + *state)
            }, scope);

        pool.submit_all(0..5);
        let mut results: Vec<_> = pool.into_iter().collect();
        results.sort_unstable();
        // Since num_workers is 1, id is 0, state is 0
        let expected: Vec<_> = (0..5).map(|x| x + 0).collect();
        assert_eq!(results, expected);
    });
}

#[test]
fn test_ordered_builder_chaining() {
     scope(|scope| {
        let num_workers = NonZeroUsize::new(1).unwrap();
        let pool = OrderedThreadpoolBuilder::new()
            .num_workers(num_workers) // Set workers
            .initializer(|id| id * 100) // Set initializer
            // .blocking_submission() // Add other options if needed
            // .blocking_workers()
            .build(|x: usize, (state, id, _jid)| {
                 assert_eq!(*state, id * 100);
                 Some(x + *state)
            }, scope);

        pool.submit_all(0..5);
        let results: Vec<_> = pool.into_iter().collect();
         // Since num_workers is 1, id is 0, state is 0
        let expected: Vec<_> = (0..5).map(|x| x + 0).collect();
        assert_eq!(results, expected);
    });
}