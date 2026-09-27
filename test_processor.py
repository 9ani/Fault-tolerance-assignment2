import pytest

import processor
from processor import TRANSACTIONS, new_state, record, run_baseline, run_fault_tolerant, run_with_exceptions


@pytest.fixture(autouse=True)
def setup(tmp_path, monkeypatch):
    monkeypatch.setattr(processor, "LOG_DIR", tmp_path)
    monkeypatch.setattr(processor, "BACKOFF", 0)


def test_baseline_stops_at_first_failure():
    r = run_baseline()
    assert r["attempted"] == 2
    assert r["successful"] == 1
    assert r["failed"] == 14
    assert r["processed_amount"] == 12000
    assert r["lost_amount"] == 425000


def test_one_failure_does_not_stop_processing():
    r = run_with_exceptions()
    assert r["successful"] == 8
    assert [f["tx"] for f in r["failures"]] == ["T002", "T004", "T006", "T008", "T010", "T012", "T015"]


def test_retry_policy_matches_rules():
    tries = {a["tx"]: a["attempt"] for a in run_fault_tolerant()["attempts"]}
    assert tries["T001"] == 1
    assert tries["T002"] == 2
    assert tries["T004"] == 3
    assert tries["T006"] == 3


def test_rollback_and_checkpoints():
    r = run_fault_tolerant()
    assert r["successful"] == 13
    assert r["retries"] == 11
    assert r["rollbacks"] == 2
    assert r["ledger_total"] == r["processed_amount"] == 287000
    assert "T006" not in r["done"] and "T012" not in r["done"]
    saved = [(len(c["state"]["done"]), c["state"]["total"]) for c in r["checkpoints"]]
    assert saved == [(5, 103000), (10, 214000), (13, 287000)]


def test_record_is_idempotent():
    state = new_state()
    record(TRANSACTIONS[0], 1, state)
    record(TRANSACTIONS[0], 1, state)
    assert state == {"done": ["T001"], "total": 12000}


def test_invalid_transaction_is_rejected_without_retry(monkeypatch):
    monkeypatch.setattr(processor, "TRANSACTIONS", TRANSACTIONS + [("T016", -500, None)])
    r = run_fault_tolerant()
    assert r["successful"] == 13
    assert r["rollbacks"] == 2
    assert r["attempts"][-1]["action"] == "reject"


def test_logs_are_not_duplicated_on_rerun():
    run_fault_tolerant()
    run_fault_tolerant()
    lines = (processor.LOG_DIR / "fault_tolerant.log").read_text(encoding="utf-8").splitlines()
    assert sum("ROLLBACK" in line for line in lines) == 2
