use glotcap::{Limits, Session, Status};
#[test]
fn sequence_receipts_and_bounded_replay() {
    let mut s = Session::new(Limits {
        replay: 2,
        max_bytes: 8,
        events: 3,
    });
    assert_eq!(s.append(1, vec![0, 0]), Err("sequence_gap"));
    assert_eq!(s.append(0, vec![0]), Err("invalid_pcm"));
    let receipt = s.append(0, vec![0, 0]).unwrap();
    s.append(1, vec![1, 1]).unwrap();
    assert_eq!(s.append(0, vec![0, 0]).unwrap(), receipt);
    assert_eq!(s.append(0, vec![2, 2]), Err("replay_conflict"));
    s.append(2, vec![2, 2]).unwrap();
    assert_eq!(s.append(0, vec![0, 0]), Err("replay_expired"));
    assert_eq!(s.append(3, vec![0; 4]), Err("duration_limit"));
    s.finish().unwrap();
    assert_eq!(s.status, Status::Draining);
    assert_eq!(s.append(2, vec![2, 2]).unwrap().next_sequence, 3);
    assert_eq!(s.append(3, vec![0, 0]), Err("ingress_closed"));
}
#[test]
fn terminal_order_late_callbacks_and_cursor_expiry() {
    let mut s = Session::new(Limits {
        replay: 2,
        max_bytes: 8,
        events: 3,
    });
    s.partial("SYNTHETIC 1".into()).unwrap();
    s.partial("SYNTHETIC 2".into()).unwrap();
    s.finish().unwrap();
    s.complete("SYNTHETIC final".into()).unwrap();
    assert_eq!(s.read(0, 3), Err("cursor_expired"));
    let events = s.read(1, 3).unwrap();
    assert_eq!(events[1].kind, "transcript_final");
    assert_eq!(events[2].kind, "completed");
    assert!(s.partial("late".into()).is_err());
    assert!(s.complete("late".into()).is_err());
    assert!(s.cancel().is_err());
    assert_eq!(s.read(99, 3), Err("cursor_ahead"));
}
#[test]
fn transcript_payloads_are_bounded_without_consuming_event_ids() {
    let mut s = Session::new(Limits::default());
    assert_eq!(s.partial("x".repeat(4097)), Err("transcript_limit"));
    assert_eq!(s.cursor(), 0);
    s.finish().unwrap();
    assert_eq!(s.complete("x".repeat(4097)), Err("transcript_limit"));
    assert_eq!(s.status, Status::Draining);
    assert_eq!(s.cursor(), 0);
}
#[test]
fn cancel_is_idempotent_and_never_finalizes() {
    let mut s = Session::new(Limits::default());
    s.finish().unwrap();
    s.cancel().unwrap();
    s.cancel().unwrap();
    assert_eq!(s.read(0, 10).unwrap().len(), 1);
    assert_eq!(s.status, Status::Cancelled);
    assert!(s.finish().is_err());
    assert!(s.partial("late".into()).is_err());
}
