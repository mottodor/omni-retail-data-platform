"""Unit tests for the deterministic fault-injection policy."""

from mock_api import faults

SEED = 42


def test_probability_is_deterministic_and_bounded() -> None:
    params = {"page": 1, "page_size": 10}
    first = faults.fault_probability(SEED, "/api/v1/fx-rates", params)
    assert first == faults.fault_probability(SEED, "/api/v1/fx-rates", params)
    assert 0.0 <= first < 1.0


def test_fingerprint_ignores_fault_parameters() -> None:
    base = {"page": 1, "date": "2026-09-10"}
    with_fault_params = {"page": 1, "date": "2026-09-10", "fault": "429", "fault_rate": 0.5}
    assert faults.fingerprint(SEED, "/api/v1/fx-rates", base) == faults.fingerprint(
        SEED, "/api/v1/fx-rates", with_fault_params
    )


def test_should_fault_disabled_without_mode_or_rate() -> None:
    seen: set[str] = set()
    assert not faults.should_fault(SEED, "/p", {"a": 1}, None, 1.0, seen)
    assert not faults.should_fault(SEED, "/p", {"a": 1}, "429", 0.0, seen)
    assert not seen


def test_should_fault_fires_at_most_once_per_fingerprint() -> None:
    seen: set[str] = set()
    # rate 1.0: every fingerprint is eligible, but only the first time
    assert faults.should_fault(SEED, "/p", {"a": 1}, "500", 1.0, seen)
    assert not faults.should_fault(SEED, "/p", {"a": 1}, "500", 1.0, seen)
    assert seen


def test_should_fault_respects_the_rate_threshold() -> None:
    seen: set[str] = set()
    params = {"page": 2}
    probability = faults.fault_probability(SEED, "/p", params)
    if probability < 0.5:
        assert faults.should_fault(SEED, "/p", params, "429", 0.5, seen)
        assert not faults.should_fault(SEED, "/p", params, "429", 0.5, seen)
    else:
        assert not faults.should_fault(SEED, "/p", params, "429", 0.5, seen)
        assert not seen


def test_fingerprints_differ_across_requests() -> None:
    # distinct pages must be distinct fingerprints so pagination is not
    # globally blocked by a single injected fault
    assert faults.fingerprint(SEED, "/p", {"page": 1}) != faults.fingerprint(
        SEED, "/p", {"page": 2}
    )
