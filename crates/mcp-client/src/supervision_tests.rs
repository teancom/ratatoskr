use super::*;

#[test]
fn respawn_backoff_cap_must_not_exceed_one_hour() {
    let hour = Duration::from_secs(3600);
    let settings = SupervisionSettings {
        respawn_backoff_base: hour,
        respawn_backoff_cap: hour,
        ..Default::default()
    };
    settings.validate().expect("exactly one hour is allowed");
    for attempt in [0, 1, 2, 33, 34, 35, u32::MAX] {
        assert_eq!(settings.respawn_delay(attempt), hour);
    }
    for cap in [
        hour + Duration::from_nanos(1),
        hour + Duration::from_millis(1),
        Duration::MAX,
    ] {
        let settings = SupervisionSettings {
            respawn_backoff_cap: cap,
            ..Default::default()
        };
        assert_eq!(
            settings.validate(),
            Err(SupervisionError::BackoffCapExceedsOneHour),
            "cap {cap:?}"
        );
    }
}

#[test]
fn default_settings_are_self_consistent() {
    // Property: the built-in defaults pass validation. Regression
    // guard: if someone changes a default to a value that breaks
    // a cross-field invariant (e.g. active_ping_timeout >= interval),
    // this test catches it before the supervisor tries to run.
    let s = SupervisionSettings::default();
    s.validate().expect("defaults must validate");
}

#[test]
fn merge_partial_with_empty_partial_is_identity() {
    // Property: merging an all-None partial is a no-op.
    let base = SupervisionSettings::default();
    let partial = PartialSupervisionSettings::default();
    assert_eq!(base.merge_partial(&partial), base);
}

#[test]
fn merge_partial_overlays_only_present_fields() {
    // Property: only Some fields overlay; None fields leave the
    // base unchanged. Demonstrated via a partial that overrides
    // two fields and leaves the rest untouched.
    let base = SupervisionSettings::default();
    let partial = PartialSupervisionSettings {
        call_timeout_ms: Some(60_000),
        unhealthy_threshold: Some(10),
        ..Default::default()
    };
    let merged = base.merge_partial(&partial);

    assert_eq!(merged.call_timeout, Duration::from_secs(60));
    assert_eq!(merged.unhealthy_threshold, 10);
    // Everything else is untouched.
    assert_eq!(merged.active_ping_interval, base.active_ping_interval);
    assert_eq!(merged.active_ping_timeout, base.active_ping_timeout);
    assert_eq!(merged.respawn_max_attempts, base.respawn_max_attempts);
    assert_eq!(merged.respawn_backoff_base, base.respawn_backoff_base);
    assert_eq!(merged.respawn_backoff_cap, base.respawn_backoff_cap);
    assert_eq!(merged.dead_rearm_interval, base.dead_rearm_interval);
}

#[test]
fn merge_partial_overlays_all_fields_when_all_present() {
    // Property: a fully-populated partial replaces every field.
    // Pins that no field is accidentally dropped from the merge.
    let base = SupervisionSettings::default();
    let partial = PartialSupervisionSettings {
        call_timeout_ms: Some(10_000),
        unhealthy_threshold: Some(2),
        active_ping_interval_ms: Some(2_000),
        active_ping_timeout_ms: Some(500),
        respawn_max_attempts: Some(3),
        respawn_backoff_base_ms: Some(200),
        respawn_backoff_cap_ms: Some(5_000),
        dead_rearm_interval_ms: Some(30_000),
    };
    let merged = base.merge_partial(&partial);
    assert_eq!(merged.call_timeout, Duration::from_secs(10));
    assert_eq!(merged.unhealthy_threshold, 2);
    assert_eq!(merged.active_ping_interval, Duration::from_secs(2));
    assert_eq!(merged.active_ping_timeout, Duration::from_millis(500));
    assert_eq!(merged.respawn_max_attempts, 3);
    assert_eq!(merged.respawn_backoff_base, Duration::from_millis(200));
    assert_eq!(merged.respawn_backoff_cap, Duration::from_secs(5));
    assert_eq!(merged.dead_rearm_interval, Duration::from_secs(30));
}

#[test]
fn validate_rejects_zero_unhealthy_threshold() {
    let s = SupervisionSettings {
        unhealthy_threshold: 0,
        ..SupervisionSettings::default()
    };
    assert_eq!(s.validate(), Err(SupervisionError::ZeroUnhealthyThreshold));
}

#[test]
fn validate_rejects_ping_timeout_ge_interval() {
    // Property: a ping timeout that's >= the ping interval would
    // cause unfinished pings to pile up — catch at validate.
    let s = SupervisionSettings {
        active_ping_interval: Duration::from_secs(3),
        active_ping_timeout: Duration::from_secs(3), // equal, not less
        ..SupervisionSettings::default()
    };
    match s.validate() {
        Err(SupervisionError::PingTimeoutExceedsInterval { .. }) => (),
        other => panic!("expected PingTimeoutExceedsInterval, got {other:?}"),
    }

    let s2 = SupervisionSettings {
        active_ping_interval: Duration::from_secs(3),
        active_ping_timeout: Duration::from_secs(5), // greater
        ..SupervisionSettings::default()
    };
    assert!(s2.validate().is_err());
}

#[test]
fn validate_rejects_backoff_base_exceeding_cap() {
    let s = SupervisionSettings {
        respawn_backoff_base: Duration::from_secs(100),
        respawn_backoff_cap: Duration::from_secs(30),
        ..SupervisionSettings::default()
    };
    match s.validate() {
        Err(SupervisionError::BackoffBaseExceedsCap { .. }) => (),
        other => panic!("expected BackoffBaseExceedsCap, got {other:?}"),
    }
}

#[test]
fn validate_rejects_zero_call_timeout() {
    let s = SupervisionSettings {
        call_timeout: Duration::ZERO,
        ..SupervisionSettings::default()
    };
    assert_eq!(s.validate(), Err(SupervisionError::ZeroCallTimeout));
}

#[test]
fn respawn_delay_grows_exponentially_and_caps() {
    // Property: the backoff sequence doubles each attempt until
    // it hits the cap, then stays there. This pins the exponential
    // shape and the cap-clamp together.
    let s = SupervisionSettings {
        respawn_backoff_base: Duration::from_millis(100),
        respawn_backoff_cap: Duration::from_millis(800),
        ..SupervisionSettings::default()
    };
    assert_eq!(s.respawn_delay(1), Duration::from_millis(100));
    assert_eq!(s.respawn_delay(2), Duration::from_millis(200));
    assert_eq!(s.respawn_delay(3), Duration::from_millis(400));
    assert_eq!(s.respawn_delay(4), Duration::from_millis(800));
    // Capped from here on.
    assert_eq!(s.respawn_delay(5), Duration::from_millis(800));
    assert_eq!(s.respawn_delay(100), Duration::from_millis(800));
}

#[test]
fn respawn_delay_attempt_zero_returns_base() {
    // Property: attempt 0 is defensively mapped to base. The
    // supervisor never calls with attempt=0 in production, but
    // a test or future caller that does won't get a zero-duration
    // delay that would fire instantly.
    let s = SupervisionSettings::default();
    assert_eq!(s.respawn_delay(0), s.respawn_backoff_base);
}

#[test]
fn respawn_delay_is_monotonic_up_to_cap() {
    // Property: delay(n) <= delay(n+1) for all n. Regression
    // guard for future refactors of the backoff formula.
    let s = SupervisionSettings::default();
    for n in 1u32..=15 {
        let a = s.respawn_delay(n);
        let b = s.respawn_delay(n + 1);
        assert!(
            a <= b,
            "backoff must be monotonic: delay({n})={a:?} > delay({})={b:?}",
            n + 1
        );
        assert!(b <= s.respawn_backoff_cap, "backoff must never exceed cap");
    }
}

#[test]
fn partial_settings_serde_roundtrip_is_camel_case() {
    // Property: PartialSupervisionSettings serializes to the
    // camelCase shape the config file uses, and round-trips back.
    // This pins the config-surface compat — if someone renames a
    // field and the JSON key silently changes, operators' configs
    // break.
    let partial = PartialSupervisionSettings {
        call_timeout_ms: Some(30_000),
        unhealthy_threshold: Some(5),
        active_ping_interval_ms: None,
        active_ping_timeout_ms: None,
        respawn_max_attempts: None,
        respawn_backoff_base_ms: None,
        respawn_backoff_cap_ms: None,
        dead_rearm_interval_ms: None,
    };
    let json = serde_json::to_string(&partial).expect("serialize");
    // None fields are skipped; present fields use camelCase.
    assert!(json.contains("\"callTimeoutMs\":30000"));
    assert!(json.contains("\"unhealthyThreshold\":5"));
    assert!(!json.contains("activePingIntervalMs"));

    let parsed: PartialSupervisionSettings = serde_json::from_str(&json).expect("deserialize");
    assert_eq!(parsed, partial);
}

#[test]
fn partial_deserializes_with_only_the_fields_present() {
    // Property: parsing a config with only a subset of fields
    // leaves the unspecified ones as None — the operator can
    // override exactly what they want without having to echo
    // every default.
    let json = r#"{"callTimeoutMs": 60000}"#;
    let parsed: PartialSupervisionSettings =
        serde_json::from_str(json).expect("partial should parse");
    assert_eq!(parsed.call_timeout_ms, Some(60_000));
    assert!(parsed.unhealthy_threshold.is_none());
    assert!(parsed.active_ping_interval_ms.is_none());
}

#[test]
fn partial_deserializes_empty_object_as_all_none() {
    // Property: `{}` is a valid partial with zero overrides.
    // Needed so an operator can legally write
    // `upstreamSupervision: {}` as a "please use all defaults"
    // marker without serde rejecting it.
    let json = "{}";
    let parsed: PartialSupervisionSettings =
        serde_json::from_str(json).expect("empty object must deserialize");
    assert_eq!(parsed, PartialSupervisionSettings::default());
}
