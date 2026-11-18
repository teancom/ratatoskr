use super::*;

#[test]
fn fresh_record_is_healthy_with_zero_counters() {
    // Property: a brand-new `UpstreamHealth` reports Healthy state,
    // zero consecutive failures, no recorded events. This is the
    // invariant every handle starts with.
    let h = UpstreamHealth::new();
    let snap = h.snapshot();
    assert_eq!(snap.state, HealthState::Healthy);
    assert_eq!(snap.consecutive_failures, 0);
    assert_eq!(snap.last_failure_kind, FailureKind::None);
    assert!(snap.last_success_at.is_none());
    assert!(snap.last_failure_at.is_none());
}

#[test]
fn record_failure_transport_level_increments_counter() {
    // Property: transport-level failures increment the counter
    // and `record_failure` returns the new value so the caller
    // can compare against the threshold without a second load.
    let h = UpstreamHealth::new();
    assert_eq!(h.record_failure(FailureKind::CallTimeout), Some(1));
    assert_eq!(h.record_failure(FailureKind::TransportClosed), Some(2));
    assert_eq!(h.record_failure(FailureKind::TransportSend), Some(3));
    assert_eq!(h.record_failure(FailureKind::RmcpTimeout), Some(4));
    assert_eq!(h.consecutive_failures(), 4);
}

#[test]
fn record_failure_peer_mcp_error_does_not_increment_counter() {
    // Property: peer-returned errors (`-32602` invalid_params, etc.)
    // are tool-shape noise, not upstream sickness. They're recorded
    // (so observability can show them) but MUST NOT push the counter
    // toward the Suspect threshold — otherwise a broken Lua script
    // that calls the same tool with wrong args repeatedly would
    // trigger a pointless respawn.
    let h = UpstreamHealth::new();
    assert_eq!(h.record_failure(FailureKind::PeerMcpError), None);
    assert_eq!(h.record_failure(FailureKind::PeerMcpError), None);
    assert_eq!(h.record_failure(FailureKind::PeerMcpError), None);
    assert_eq!(h.consecutive_failures(), 0);

    // But `last_failure_kind` is updated, so the supervisor's
    // snapshot reflects the most recent peer error for debugging.
    assert_eq!(h.snapshot().last_failure_kind, FailureKind::PeerMcpError);
}

#[test]
fn record_failure_auth_expired_does_not_increment_counter_but_stamps_kind() {
    // Property: an AuthExpired failure is recorded for observability
    // (last_failure_kind so list-servers can explain the NeedsLogin
    // state) but does NOT bump consecutive_failures — re-dialing
    // can't fix a dead refresh token, so it must not drive the
    // transient-flap respawn machinery.
    let h = UpstreamHealth::new();
    assert_eq!(h.record_failure(FailureKind::AuthExpired), None);
    assert_eq!(h.consecutive_failures(), 0);
    assert_eq!(h.snapshot().last_failure_kind, FailureKind::AuthExpired);
}

#[test]
fn enter_needs_login_fires_edge_only_once() {
    // Property: the FIRST transition into NeedsLogin returns true (the
    // edge); staying in NeedsLogin returns false. This is what stops a
    // chatty agent's repeated failing calls from spamming the user
    // with identical re-auth notifications.
    let h = UpstreamHealth::new();
    assert_eq!(h.state(), HealthState::Healthy);

    assert!(
        h.enter_needs_login(),
        "first transition Healthy -> NeedsLogin must report the edge"
    );
    assert_eq!(h.state(), HealthState::NeedsLogin);

    assert!(
        !h.enter_needs_login(),
        "already NeedsLogin -> no edge, must return false (notification dedup)"
    );
    assert!(!h.enter_needs_login(), "still NeedsLogin -> still no edge");
    assert_eq!(h.state(), HealthState::NeedsLogin);
}

#[test]
fn enter_needs_login_re_edges_after_recovery() {
    // Property: if the upstream recovers (e.g. reconnect sets it back
    // to Healthy) and then loses auth again, that's a NEW edge — the
    // user should be notified again, because it's a fresh incident.
    let h = UpstreamHealth::new();
    assert!(h.enter_needs_login());
    // Simulate a successful reconnect resetting state.
    h.set_state(HealthState::Healthy);
    assert!(
        h.enter_needs_login(),
        "a fresh loss of auth after recovery is a new edge"
    );
}

#[test]
fn record_success_resets_counter() {
    // Property: a single success zeroes the consecutive-failure
    // counter regardless of how many prior failures accumulated.
    let h = UpstreamHealth::new();
    h.record_failure(FailureKind::CallTimeout);
    h.record_failure(FailureKind::CallTimeout);
    h.record_failure(FailureKind::CallTimeout);
    assert_eq!(h.consecutive_failures(), 3);

    h.record_success();
    assert_eq!(h.consecutive_failures(), 0);
    assert!(
        h.snapshot().last_success_at.is_some(),
        "success timestamp must be set after record_success"
    );
}

#[test]
fn record_success_auto_promotes_suspect_to_healthy() {
    // Property: when the state is Suspect (set by the supervisor
    // after K consecutive transport failures), a successful call
    // promotes back to Healthy. The counter zero happens
    // unconditionally; the state flip is conditional on being in
    // Suspect so we don't accidentally overwrite a supervisor
    // transition to Unhealthy.
    let h = UpstreamHealth::new();
    h.set_state(HealthState::Suspect);
    h.record_success();
    assert_eq!(h.state(), HealthState::Healthy);
}

#[test]
fn record_success_does_not_promote_from_unhealthy() {
    // Property: a racing success against the supervisor's
    // transition to Unhealthy must NOT flip state back to
    // Healthy. The supervisor's transition is authoritative; the
    // handle-level auto-promotion only applies from Suspect.
    //
    // Why this matters: without this guard, the following race
    // is possible:
    //   - T0: supervisor decides "K failures, go to Unhealthy"
    //   - T0: a straggler call that was in-flight before the
    //         transition returns successfully
    //   - T1: supervisor writes Unhealthy
    //   - T2: straggler's record_success runs and silently
    //         demotes Unhealthy back to Healthy
    // The compare_exchange(Suspect→Healthy) in record_success
    // makes sure the straggler is a no-op on non-Suspect states.
    for initial in [
        HealthState::Unhealthy,
        HealthState::Respawning,
        HealthState::Dead,
    ] {
        let h = UpstreamHealth::new();
        h.set_state(initial);
        h.record_success();
        assert_eq!(
            h.state(),
            initial,
            "record_success must not promote out of {initial:?}"
        );
    }
}

#[test]
fn record_success_keeps_healthy_as_healthy() {
    // Property: a success when already Healthy is a no-op on
    // state. (The counter reset still happens; that's covered by
    // record_success_resets_counter above.) Pins that the
    // compare_exchange isn't accidentally widened to stomp on
    // non-Suspect states.
    let h = UpstreamHealth::new();
    h.record_success();
    assert_eq!(h.state(), HealthState::Healthy);
}

#[test]
fn health_state_ordering_matches_expected_semantics() {
    // Property: the numeric order Healthy < Suspect < Unhealthy <
    // Respawning < Dead is load-bearing for
    // `HealthState::gates_calls` and for any future predicate that
    // wants to ask "is this worse than Suspect?" via `>=`.
    assert!(HealthState::Healthy < HealthState::Suspect);
    assert!(HealthState::Suspect < HealthState::Unhealthy);
    assert!(HealthState::Unhealthy < HealthState::Respawning);
    assert!(HealthState::Respawning < HealthState::Dead);
    // NeedsLogin is appended after Dead. Its placement at the end
    // keeps the documented stable ordering of the pre-existing
    // variants untouched; gating is driven by `gates_calls()`, not
    // this order, so the position is cosmetic.
    assert!(HealthState::Dead < HealthState::NeedsLogin);
}

#[test]
fn gates_calls_is_true_for_unhealthy_and_worse() {
    // Property: `gates_calls` reports true exactly when `call_tool`
    // should short-circuit with TransientUnavailable. Healthy and
    // Suspect still dispatch — Suspect because the supervisor is
    // probing and we don't want to block legitimate traffic while
    // it decides.
    assert!(!HealthState::Healthy.gates_calls());
    assert!(!HealthState::Suspect.gates_calls());
    assert!(HealthState::Unhealthy.gates_calls());
    assert!(HealthState::Respawning.gates_calls());
    assert!(HealthState::Dead.gates_calls());
    // NeedsLogin gates too: a call would just 401 again. The agent
    // gets a clear "down" signal rather than a hang or a confusing
    // auth error on every attempt.
    assert!(HealthState::NeedsLogin.gates_calls());
}

#[test]
fn failure_kind_classification_matches_transport_level_predicate() {
    // Property: `FailureKind::is_transport_level` agrees with
    // `UpstreamError::is_transport_level` on the overlapping set.
    // Both must stay in sync because `call_tool` classifies the
    // incoming error once and derives both the FailureKind (for
    // the health record) and the caller-facing
    // `UpstreamError::is_transport_level` (for the agent).
    assert!(FailureKind::CallTimeout.is_transport_level());
    assert!(FailureKind::TransportClosed.is_transport_level());
    assert!(FailureKind::TransportSend.is_transport_level());
    assert!(FailureKind::RmcpTimeout.is_transport_level());

    assert!(!FailureKind::None.is_transport_level());
    assert!(!FailureKind::PeerMcpError.is_transport_level());
    assert!(!FailureKind::UnexpectedResponse.is_transport_level());
    // Side-channel sickness signal — NOT transport-level here
    // because it routes through `consecutive_self_reported_errors`
    // instead of `consecutive_failures`. See the doc on
    // `is_transport_level` for the rationale.
    assert!(!FailureKind::UpstreamSelfReportedError.is_transport_level());
    // AuthExpired is an operator-precondition signal, NOT a transient
    // transport flap — re-dialing won't help, only a human `login`
    // will. It must not feed `consecutive_failures`.
    assert!(!FailureKind::AuthExpired.is_transport_level());
}

#[test]
fn set_state_then_state_roundtrips_every_variant() {
    // Property: every HealthState variant survives the
    // encode/decode round-trip through the AtomicU8.
    let h = UpstreamHealth::new();
    for s in [
        HealthState::Healthy,
        HealthState::Suspect,
        HealthState::Unhealthy,
        HealthState::Respawning,
        HealthState::Dead,
        HealthState::NeedsLogin,
    ] {
        h.set_state(s);
        assert_eq!(h.state(), s);
    }
}

#[test]
fn failure_kind_roundtrips_every_variant() {
    // Property: every FailureKind variant survives encode/decode
    // via the last_failure_kind atomic (covered implicitly by
    // record_failure + snapshot).
    let h = UpstreamHealth::new();
    for k in [
        FailureKind::None,
        FailureKind::CallTimeout,
        FailureKind::TransportClosed,
        FailureKind::RmcpTimeout,
        FailureKind::TransportSend,
        FailureKind::PeerMcpError,
        FailureKind::UnexpectedResponse,
        FailureKind::UpstreamSelfReportedError,
        FailureKind::AuthExpired,
    ] {
        // Skip None: record_failure(None) isn't a real use case,
        // and record_failure doesn't accept a way to clear the
        // last_failure_kind without going through a real failure.
        if k == FailureKind::None {
            continue;
        }
        h.record_failure(k);
        assert_eq!(h.snapshot().last_failure_kind, k);
    }
}

#[test]
fn reset_counter_zeroes_without_touching_state() {
    // Property: `reset_counter` is a narrow operation — it zeroes
    // the consecutive counter and nothing else. The supervisor
    // uses it after a successful respawn (future case, when we
    // reuse a record across replace); today handles are replaced
    // along with their records, but the method exists so the
    // reset has a single name rather than a raw atomic store.
    let h = UpstreamHealth::new();
    h.set_state(HealthState::Unhealthy);
    h.record_failure(FailureKind::CallTimeout);
    h.record_failure(FailureKind::CallTimeout);
    assert_eq!(h.consecutive_failures(), 2);

    h.reset_counter();
    assert_eq!(h.consecutive_failures(), 0);
    // State untouched — reset_counter is narrow by design.
    assert_eq!(h.state(), HealthState::Unhealthy);
}

#[test]
fn snapshot_eventually_reflects_recorded_events() {
    // Property: after a sequence of record_* calls, the snapshot
    // reflects every field. Not a cross-field atomicity claim
    // (the module docs are explicit about that); just that the
    // individual loads pick up the stored values.
    let h = UpstreamHealth::new();
    h.set_state(HealthState::Suspect);
    h.record_failure(FailureKind::CallTimeout);
    h.record_failure(FailureKind::TransportClosed);

    let snap = h.snapshot();
    assert_eq!(snap.state, HealthState::Suspect);
    assert_eq!(snap.consecutive_failures, 2);
    assert_eq!(snap.last_failure_kind, FailureKind::TransportClosed);
    assert!(snap.last_failure_at.is_some());
    assert!(snap.last_success_at.is_none());
}

#[test]
fn consecutive_counter_saturates_rather_than_wrapping() {
    // Property: a pathological failure burst against a
    // consecutive-failure counter close to u32::MAX must saturate
    // (stay at MAX), not wrap to 0. A wrap would be catastrophic:
    // a wrapped counter reads as "Healthy, no failures" just as
    // the upstream enters its worst state.
    //
    // We can't realistically drive u32::MAX failures in a test, but
    // we can pre-load the counter near the ceiling and make sure
    // `record_failure` returns the saturated value.
    let h = UpstreamHealth::new();
    // Push the counter to MAX - 1 directly via the atomic (this
    // is a test-only shortcut; production callers go through
    // record_failure).
    h.consecutive_failures
        .store(u32::MAX - 1, Ordering::Release);
    // First increment: lands on MAX, record_failure returns MAX.
    assert_eq!(h.record_failure(FailureKind::CallTimeout), Some(u32::MAX));
    // Further increments preserve the stored counter and return value
    // at the ceiling.
    assert_eq!(h.record_failure(FailureKind::CallTimeout), Some(u32::MAX));
    assert_eq!(h.consecutive_failures(), u32::MAX);
}

// ---- record_self_reported_error / decay tests ----

#[test]
fn record_self_reported_error_starts_at_one_increments_to_two() {
    // Property: fresh record + one self-reported hit yields counter
    // = 1; a second hit within SELF_REPORT_DECAY yields = 2. This
    // is the in-streak case; decay test below covers the other.
    let h = UpstreamHealth::new();

    assert_eq!(h.consecutive_self_reported_errors(), 0);
    let after_first = h.record_self_reported_error();
    assert_eq!(after_first, 1);
    assert_eq!(h.consecutive_self_reported_errors(), 1);

    let after_second = h.record_self_reported_error();
    assert_eq!(after_second, 2);
    assert_eq!(h.consecutive_self_reported_errors(), 2);
}

#[test]
fn self_reported_streak_decays_after_window() {
    // Property: a hit older than SELF_REPORT_DECAY does NOT compound
    // with a fresh hit — the fresh one starts a new streak at 1
    // instead of bumping the stale streak to 2. Without this, an
    // upstream that emitted one transient Error a week ago would
    // need only `threshold - 1` fresh hits to trigger demotion,
    // which is too eager — we want demotion only on sustained sick.
    let h = UpstreamHealth::new();

    // Land an initial hit, then backdate it to look like it
    // happened well outside the decay window.
    h.record_self_reported_error();
    assert_eq!(h.consecutive_self_reported_errors(), 1);

    let backdated_micros = now_micros()
        .saturating_sub((SELF_REPORT_DECAY.as_micros() as i64).saturating_add(1_000_000));
    h.last_self_reported_micros
        .store(backdated_micros, Ordering::Release);

    let after_fresh = h.record_self_reported_error();
    assert_eq!(
        after_fresh,
        1,
        "stale streak (> {} s old) must decay; fresh hit starts at 1",
        SELF_REPORT_DECAY.as_secs()
    );
    assert_eq!(h.consecutive_self_reported_errors(), 1);
}

#[test]
fn record_success_does_not_clear_self_reported_counter() {
    // Property mirrors the regression guard in upstream_handler's
    // test module, but lives here too because this is the unit
    // boundary that owns the invariant. The dash-mcp bug was a
    // record_success call zeroing this counter — that flow lives
    // entirely inside UpstreamHealth, so the contract belongs in
    // the module's own tests as well.
    let h = UpstreamHealth::new();
    h.record_self_reported_error();
    h.record_self_reported_error();
    assert_eq!(h.consecutive_self_reported_errors(), 2);

    h.record_success();

    assert_eq!(
        h.consecutive_self_reported_errors(),
        2,
        "record_success must NOT touch the self-reported counter"
    );
    assert_eq!(
        h.consecutive_failures(),
        0,
        "transport-level counter is independent and was already 0"
    );
}

#[test]
fn reset_counter_zeroes_both_counters() {
    // Property: the supervisor's reset_counter (used for the
    // future "respawn reuses the same record" case) zeros both
    // counters, not just the transport-level one. Otherwise a
    // respawned upstream would inherit a stale self-reported
    // streak from its predecessor.
    let h = UpstreamHealth::new();
    h.record_failure(FailureKind::CallTimeout);
    h.record_failure(FailureKind::CallTimeout);
    h.record_self_reported_error();
    h.record_self_reported_error();
    h.record_self_reported_error();
    assert_eq!(h.consecutive_failures(), 2);
    assert_eq!(h.consecutive_self_reported_errors(), 3);

    h.reset_counter();
    assert_eq!(h.consecutive_failures(), 0);
    assert_eq!(h.consecutive_self_reported_errors(), 0);
}

#[test]
fn record_success_does_not_promote_suspect_to_healthy_while_self_reported_streak_outstanding() {
    // Property: when the side channel is still complaining
    // (consecutive_self_reported_errors > 0), record_success on
    // Suspect must NOT auto-promote to Healthy. This is what stops
    // the Healthy ↔ Suspect oscillation: every misleading-success
    // call_tool's record_success would CAS-promote, undoing the
    // demotion the supervisor had just made.
    let h = UpstreamHealth::new();
    // Establish the situation: self-reported streak outstanding,
    // upstream sitting in Suspect (as the supervisor would have
    // driven it on the previous tick).
    h.record_self_reported_error();
    h.record_self_reported_error();
    h.set_state(HealthState::Suspect);

    // Misleading-success arrives: rmcp returned Ok(_), so
    // record_success runs. Counter survives (already covered
    // elsewhere); the new invariant is that state stays Suspect.
    h.record_success();

    assert_eq!(h.state(), HealthState::Suspect);
    assert_eq!(h.consecutive_self_reported_errors(), 2);
}

#[test]
fn record_success_still_promotes_suspect_to_healthy_when_self_reported_clean() {
    // Property: the gate added by the test above must not break
    // the legitimate Suspect → Healthy promotion path. If the
    // side channel is clean (counter = 0), a real success still
    // promotes — that's the transport-flap-recovery case
    // (consecutive_failures was the demoter; a real call succeeds
    // and recovers).
    let h = UpstreamHealth::new();
    h.record_failure(FailureKind::CallTimeout);
    h.record_failure(FailureKind::CallTimeout);
    h.set_state(HealthState::Suspect);
    assert_eq!(h.consecutive_self_reported_errors(), 0);

    h.record_success();

    assert_eq!(h.state(), HealthState::Healthy);
    assert_eq!(h.consecutive_failures(), 0);
}

#[test]
fn snapshot_surfaces_self_reported_counter() {
    // Property: HealthSnapshot is the operator-facing view; the
    // new counter must be visible there too, not just via the
    // dedicated reader.
    let h = UpstreamHealth::new();
    h.record_self_reported_error();
    h.record_self_reported_error();

    let snap = h.snapshot();
    assert_eq!(snap.consecutive_self_reported_errors, 2);
    assert_eq!(
        snap.last_failure_kind,
        FailureKind::UpstreamSelfReportedError
    );
}

#[test]
fn consecutive_failures_saturate_in_storage_and_return_value() {
    // Property: reaching u32::MAX keeps both the logged value and the
    // stored health counter at MAX instead of wrapping to zero.
    let h = UpstreamHealth::new();
    h.consecutive_failures
        .store(u32::MAX - 1, Ordering::Release);

    assert_eq!(h.record_failure(FailureKind::CallTimeout), Some(u32::MAX));
    assert_eq!(h.consecutive_failures(), u32::MAX);
    assert_eq!(h.record_failure(FailureKind::CallTimeout), Some(u32::MAX));
    assert_eq!(h.consecutive_failures(), u32::MAX);
}

#[test]
fn self_reported_errors_saturate_and_refresh_decay_timestamp() {
    // Property: a fresh self-reported signal at the counter ceiling is
    // retained as MAX, and its timestamp starts the next decay interval.
    let h = UpstreamHealth::new();
    h.consecutive_self_reported_errors
        .store(u32::MAX - 1, Ordering::Release);
    h.last_self_reported_micros
        .store(now_micros().saturating_sub(1_000_000), Ordering::Release);

    assert_eq!(h.record_self_reported_error(), u32::MAX);
    assert_eq!(h.consecutive_self_reported_errors(), u32::MAX);
    let recorded_at = h.last_self_reported_micros.load(Ordering::Acquire);
    assert!(recorded_at > 0);
    assert!(now_micros().saturating_sub(recorded_at) < SELF_REPORT_DECAY.as_micros() as i64);

    assert_eq!(h.record_self_reported_error(), u32::MAX);
    assert_eq!(h.consecutive_self_reported_errors(), u32::MAX);
}

#[test]
fn concurrent_counter_updates_saturate_near_max() {
    // Property: racing increments at the boundary cannot wrap either
    // streak counter or return a value below the saturated maximum.
    const THREADS: usize = 8;
    const INCREMENTS: usize = 32;

    let h = UpstreamHealth::new();
    h.consecutive_failures
        .store(u32::MAX - 1, Ordering::Release);
    h.consecutive_self_reported_errors
        .store(u32::MAX - 1, Ordering::Release);
    h.last_self_reported_micros
        .store(now_micros(), Ordering::Release);

    std::thread::scope(|scope| {
        let mut joins = Vec::new();
        for _ in 0..THREADS {
            let h = &h;
            joins.push(scope.spawn(move || {
                for _ in 0..INCREMENTS {
                    assert_eq!(h.record_failure(FailureKind::CallTimeout), Some(u32::MAX));
                    assert_eq!(h.record_self_reported_error(), u32::MAX);
                }
            }));
        }
        for join in joins {
            join.join().expect("counter update thread must complete");
        }
    });

    assert_eq!(h.consecutive_failures(), u32::MAX);
    assert_eq!(h.consecutive_self_reported_errors(), u32::MAX);
}
