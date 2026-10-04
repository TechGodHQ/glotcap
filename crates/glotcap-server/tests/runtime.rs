use glotcap_server::{AppState, Config, SyntheticFactory, tool};
use serde_json::json;
use std::{
    future::Future,
    sync::{Arc, Barrier},
    task::{Context, Poll, Waker},
    time::Duration,
};
async fn start(s: &AppState) -> String {
    tool(s, "start_session", json!({"format":"pcm_s16le_16000_mono"}))
        .await
        .unwrap()["session_id"]
        .as_str()
        .unwrap()
        .into()
}
async fn terminal(s: &AppState, id: &str) -> serde_json::Value {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let v = tool(s, "read_events", json!({"session_id":id,"after":0}))
                .await
                .unwrap();
            if ["completed", "cancelled", "failed"].contains(&v["status"].as_str().unwrap()) {
                return v;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap()
}
async fn blocked_session() -> (AppState, Arc<SyntheticFactory>) {
    let factory = Arc::new(SyntheticFactory::blocked());
    let state = AppState::new(Config::default(), factory.clone());
    let id = start(&state).await;
    tool(
        &state,
        "append_audio",
        json!({"session_id":id,"sequence":0,"data_base64":"AAA="}),
    )
    .await
    .unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while factory.calls() == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(state.workers(), 1);
    assert_eq!(factory.live(), 1);
    (state, factory)
}

#[tokio::test(flavor = "current_thread")]
async fn concurrent_shutdown_callers_wait_for_verified_teardown() {
    let (state, factory) = blocked_session().await;
    let clone = state.clone();
    let mut first = Box::pin(state.shutdown());
    let mut second = Box::pin(clone.shutdown());
    let mut cx = Context::from_waker(Waker::noop());
    // No yield between polls: the live blocked worker cannot run its cancellation yet.
    assert_eq!(first.as_mut().poll(&mut cx), Poll::Pending);
    assert_eq!(state.workers(), 1);
    assert_eq!(factory.live(), 1);
    assert_eq!(
        second.as_mut().poll(&mut cx),
        Poll::Pending,
        "a concurrent caller must not return before the first caller joins"
    );
    tokio::time::timeout(Duration::from_secs(2), async {
        first.await;
        assert_eq!(state.workers(), 0);
        assert_eq!(factory.live(), 0);
        second.await;
        assert_eq!(state.workers(), 0);
        assert_eq!(factory.live(), 0);
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn dropped_shutdown_future_keeps_join_available_for_retry() {
    let (state, factory) = blocked_session().await;
    let mut first = Box::pin(state.shutdown());
    let mut cx = Context::from_waker(Waker::noop());
    assert_eq!(first.as_mut().poll(&mut cx), Poll::Pending);
    drop(first);
    let mut retry = Box::pin(state.shutdown());
    assert_eq!(state.workers(), 1);
    assert_eq!(factory.live(), 1);
    assert_eq!(
        retry.as_mut().poll(&mut cx),
        Poll::Pending,
        "dropping shutdown must not detach the worker's only join handle"
    );
    tokio::time::timeout(Duration::from_secs(2), retry)
        .await
        .unwrap();
    assert_eq!(state.workers(), 0);
    assert_eq!(factory.live(), 0);
    state.shutdown().await;
    assert_eq!(state.workers(), 0);
    assert_eq!(factory.live(), 0);
}

#[tokio::test]
async fn bounded_queue_cancel_bypasses_blocked_provider_and_cleanup() {
    let factory = Arc::new(SyntheticFactory::blocked());
    let s = AppState::new(
        Config {
            queue: 1,
            sessions: 1,
            ..Config::default()
        },
        factory.clone(),
    );
    let id = start(&s).await;
    tool(
        &s,
        "append_audio",
        json!({"session_id":id,"sequence":0,"data_base64":"AAA="}),
    )
    .await
    .unwrap();
    while factory.calls() == 0 {
        tokio::task::yield_now().await;
    }
    tool(
        &s,
        "append_audio",
        json!({"session_id":id,"sequence":1,"data_base64":"AAA="}),
    )
    .await
    .unwrap();
    assert_eq!(
        tool(
            &s,
            "append_audio",
            json!({"session_id":id,"sequence":2,"data_base64":"AAA="})
        )
        .await
        .unwrap_err(),
        "backpressure"
    );
    assert_eq!(
        tool(
            &s,
            "start_session",
            json!({"format":"pcm_s16le_16000_mono"})
        )
        .await
        .unwrap_err(),
        "session_limit"
    );
    let finish = tokio::time::timeout(
        Duration::from_millis(200),
        tool(&s, "finish_session", json!({"session_id":id})),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(finish["status"], "draining");
    for _ in 0..100 {
        tokio::task::yield_now().await;
    }
    assert_eq!(
        factory.calls(),
        1,
        "control wake must not restart provider work"
    );
    tokio::time::timeout(
        Duration::from_millis(200),
        tool(&s, "cancel_session", json!({"session_id":id})),
    )
    .await
    .unwrap()
    .unwrap();
    let v = terminal(&s, &id).await;
    assert_eq!(v["status"], "cancelled");
    assert_eq!(v["events"].as_array().unwrap().len(), 1);
    assert_eq!(factory.live(), 0);
    assert_eq!(s.workers(), 0);
    s.shutdown().await;
}
#[tokio::test]
async fn drain_final_terminal_and_exact_replay() {
    let s = AppState::synthetic(Config::default());
    let id = start(&s).await;
    let a = json!({"session_id":id,"sequence":0,"data_base64":"AAA="});
    let receipt = tool(&s, "append_audio", a.clone()).await.unwrap();
    tool(
        &s,
        "append_audio",
        json!({"session_id":id,"sequence":1,"data_base64":"AAA="}),
    )
    .await
    .unwrap();
    assert_eq!(tool(&s, "append_audio", a.clone()).await.unwrap(), receipt);
    tool(&s, "finish_session", json!({"session_id":id}))
        .await
        .unwrap();
    let v = terminal(&s, &id).await;
    let e = v["events"].as_array().unwrap();
    assert_eq!(e[e.len() - 2]["kind"], "transcript_final");
    assert_eq!(e[e.len() - 1]["kind"], "completed");
    assert!(
        e[e.len() - 2]["text"]
            .as_str()
            .unwrap()
            .starts_with("SYNTHETIC")
    );
    assert_eq!(tool(&s, "append_audio", a).await.unwrap(), receipt);
    assert_eq!(s.workers(), 0);
    s.shutdown().await;
}
#[tokio::test]
async fn lag_expiry_atomic_replay_live_and_disconnect_not_cancel() {
    let s = AppState::synthetic(Config {
        events: 3,
        ..Config::default()
    });
    let id = start(&s).await;
    let mut observer = s.subscribe(&id, 0).unwrap();
    for sequence in 0..6 {
        tool(
            &s,
            "append_audio",
            json!({"session_id":id,"sequence":sequence,"data_base64":"AAA="}),
        )
        .await
        .unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if let Ok(page) =
                    tool(&s, "read_events", json!({"session_id":id,"after":sequence})).await
                    && page["next_cursor"].as_u64().unwrap() > sequence
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }
    assert_eq!(observer.next().await.unwrap().unwrap_err(), "observer_lag");
    assert_eq!(s.subscribe(&id, 0).err().unwrap(), "cursor_expired");
    let v = tool(&s, "read_events", json!({"session_id":id,"after":3}))
        .await
        .unwrap();
    assert_eq!(v["events"].as_array().unwrap().len(), 3);
    let mut observer = s.subscribe(&id, 5).unwrap();
    assert_eq!(observer.next().await.unwrap().unwrap()["event_id"], 6);
    tool(&s, "finish_session", json!({"session_id":id}))
        .await
        .unwrap();
    assert_eq!(
        observer.next().await.unwrap().unwrap()["kind"],
        "transcript_final"
    );
    assert_eq!(observer.next().await.unwrap().unwrap()["kind"], "completed");
    assert!(observer.next().await.is_none());
    let id2 = start(&s).await;
    drop(s.subscribe(&id2, 0).unwrap());
    assert_eq!(
        tool(&s, "read_events", json!({"session_id":id2,"after":0}))
            .await
            .unwrap()["status"],
        "open"
    );
    s.shutdown().await;
    assert_eq!(s.workers(), 0);
}
#[tokio::test]
async fn failure_releases_provider_without_final_and_shutdown_closes_start() {
    let factory = Arc::new(SyntheticFactory::failing());
    let s = AppState::new(Config::default(), factory.clone());
    let id = start(&s).await;
    tool(
        &s,
        "append_audio",
        json!({"session_id":id,"sequence":0,"data_base64":"AAA="}),
    )
    .await
    .unwrap();
    let v = terminal(&s, &id).await;
    assert_eq!(v["status"], "failed");
    assert_eq!(v["events"].as_array().unwrap().len(), 1);
    assert_eq!(factory.live(), 0);
    assert_eq!(s.workers(), 0);
    assert_eq!(
        tool(&s, "finish_session", json!({"session_id":id}))
            .await
            .unwrap_err(),
        "terminal_conflict"
    );
    s.shutdown().await;
    assert_eq!(
        tool(
            &s,
            "start_session",
            json!({"format":"pcm_s16le_16000_mono"})
        )
        .await
        .unwrap_err(),
        "shutdown"
    );
}
#[tokio::test]
async fn observer_bound_validation_and_failed_ingress_does_not_consume_sequence() {
    let s = AppState::synthetic(Config {
        observers: 1,
        max_bytes: 2,
        ..Config::default()
    });
    let id = start(&s).await;
    let observer = s.subscribe(&id, 0).unwrap();
    assert_eq!(s.subscribe(&id, 0).err().unwrap(), "observer_limit");
    drop(observer);
    let _observer = s.subscribe(&id, 0).unwrap();
    for (sequence, data, error) in [
        (1, "AAA=", "sequence_gap"),
        (0, "!", "invalid_base64"),
        (0, "AA==", "invalid_pcm"),
    ] {
        assert_eq!(
            tool(
                &s,
                "append_audio",
                json!({"session_id":id,"sequence":sequence,"data_base64":data})
            )
            .await
            .unwrap_err(),
            error
        );
    }
    tool(
        &s,
        "append_audio",
        json!({"session_id":id,"sequence":0,"data_base64":"AAA="}),
    )
    .await
    .unwrap();
    assert_eq!(
        tool(
            &s,
            "append_audio",
            json!({"session_id":id,"sequence":0,"data_base64":"AQE="})
        )
        .await
        .unwrap_err(),
        "replay_conflict"
    );
    assert_eq!(
        tool(
            &s,
            "append_audio",
            json!({"session_id":id,"sequence":1,"data_base64":"AAA="})
        )
        .await
        .unwrap_err(),
        "duration_limit"
    );
    s.shutdown().await;
    assert_eq!(s.workers(), 0);
}
#[tokio::test]
async fn dropping_last_host_cancels_blocked_workers() {
    let factory = Arc::new(SyntheticFactory::blocked());
    let s = AppState::new(Config::default(), factory.clone());
    let id = start(&s).await;
    tool(
        &s,
        "append_audio",
        json!({"session_id":id,"sequence":0,"data_base64":"AAA="}),
    )
    .await
    .unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        while factory.calls() == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    drop(s);
    tokio::time::timeout(Duration::from_millis(100), async {
        while factory.live() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("last host drop must not leak blocked workers");
}
#[tokio::test]
async fn cancelling_one_session_does_not_cancel_another_blocked_worker() {
    let factory = Arc::new(SyntheticFactory::blocked());
    let s = AppState::new(
        Config {
            sessions: 2,
            ..Config::default()
        },
        factory.clone(),
    );
    let a = start(&s).await;
    let b = start(&s).await;
    for id in [&a, &b] {
        tool(
            &s,
            "append_audio",
            json!({"session_id":id,"sequence":0,"data_base64":"AAA="}),
        )
        .await
        .unwrap();
    }
    tokio::time::timeout(Duration::from_secs(1), async {
        while factory.calls() < 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    tool(&s, "cancel_session", json!({"session_id":a}))
        .await
        .unwrap();
    assert_eq!(terminal(&s, &a).await["status"], "cancelled");
    assert_eq!(factory.live(), 1);
    assert_eq!(
        tool(&s, "read_events", json!({"session_id":b,"after":0}))
            .await
            .unwrap()["status"],
        "open"
    );
    tool(
        &s,
        "append_audio",
        json!({"session_id":b,"sequence":1,"data_base64":"AAA="}),
    )
    .await
    .unwrap();
    s.shutdown().await;
    assert_eq!(factory.live(), 0);
    assert_eq!(s.workers(), 0);
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cross_thread_finish_cancel_race_has_one_terminal() {
    let factory = Arc::new(SyntheticFactory::default());
    let state = AppState::new(Config::default(), factory.clone());
    let runtime = tokio::runtime::Handle::current();
    for _ in 0..32 {
        let id = start(&state).await;
        tool(
            &state,
            "append_audio",
            json!({"session_id":id,"sequence":0,"data_base64":"AAA="}),
        )
        .await
        .unwrap();
        // Separate OS threads rendezvous immediately before polling the non-suspending
        // tool futures. Unlike join!, this exercises real session-lock contention.
        let barrier = Barrier::new(2);
        let (finish, cancel) = std::thread::scope(|scope| {
            let f = scope.spawn(|| {
                barrier.wait();
                runtime.block_on(tool(&state, "finish_session", json!({"session_id":id})))
            });
            let c = scope.spawn(|| {
                barrier.wait();
                runtime.block_on(tool(&state, "cancel_session", json!({"session_id":id})))
            });
            (f.join().unwrap(), c.join().unwrap())
        });
        assert!(finish.is_ok() || finish == Err("terminal_conflict"));
        assert!(cancel.is_ok() || cancel == Err("terminal_conflict"));
        assert!(finish.is_ok() || cancel.is_ok());
        let page = terminal(&state, &id).await;
        let events = page["events"].as_array().unwrap();
        assert_eq!(
            events
                .iter()
                .filter(
                    |e| ["completed", "cancelled", "failed"].contains(&e["kind"].as_str().unwrap())
                )
                .count(),
            1
        );
        match page["status"].as_str().unwrap() {
            "completed" => {
                assert!(finish.is_ok());
                assert_eq!(cancel, Err("terminal_conflict"));
                assert_eq!(events[events.len() - 2]["kind"], "transcript_final");
                assert_eq!(events.last().unwrap()["kind"], "completed");
                assert_eq!(
                    events
                        .iter()
                        .filter(|e| e["kind"] == "transcript_final")
                        .count(),
                    1
                );
            }
            "cancelled" => {
                assert!(cancel.is_ok());
                assert!(!events.iter().any(|e| e["kind"] == "transcript_final"));
                assert_eq!(events.last().unwrap()["kind"], "cancelled");
            }
            status => panic!("unexpected terminal status: {status}"),
        }
        assert_eq!(state.workers(), 0);
        assert_eq!(factory.live(), 0);
    }
    state.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn finish_cancel_races_have_one_terminal() {
    let s = AppState::synthetic(Config::default());
    for _ in 0..20 {
        let id = start(&s).await;
        tool(
            &s,
            "append_audio",
            json!({"session_id":id,"sequence":0,"data_base64":"AAA="}),
        )
        .await
        .unwrap();
        let args = json!({"session_id":id});
        let (f, c) = tokio::join!(
            tool(&s, "finish_session", args.clone()),
            tool(&s, "cancel_session", args)
        );
        f.unwrap();
        assert!(c.is_ok() || c == Err("terminal_conflict"));
        let v = terminal(&s, &id).await;
        assert_eq!(
            v["events"]
                .as_array()
                .unwrap()
                .iter()
                .filter(
                    |e| ["completed", "cancelled", "failed"].contains(&e["kind"].as_str().unwrap())
                )
                .count(),
            1
        );
    }
    s.shutdown().await;
}
