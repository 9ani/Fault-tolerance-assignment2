import csv
import json
import logging
import time
from copy import deepcopy
from datetime import datetime
from pathlib import Path

TRANSACTIONS = [
    ("T001", 12000, None),
    ("T002", 25000, "Network"),
    ("T003", 8000, None),
    ("T004", 45000, "Timeout"),
    ("T005", 13000, None),
    ("T006", 70000, "Database"),
    ("T007", 9000, None),
    ("T008", 31000, "Network"),
    ("T009", 15000, None),
    ("T010", 50000, "Timeout"),
    ("T011", 6000, None),
    ("T012", 80000, "Database"),
    ("T013", 11000, None),
    ("T014", 22000, None),
    ("T015", 40000, "Network"),
]

FAILED_ATTEMPTS = {"Network": 1, "Timeout": 2, "Database": 3}
MAX_RETRIES = 2
CHECKPOINT_EVERY = 5
BACKOFF = 0.1
LOG_DIR = Path("logs")


class PaymentError(Exception):
    kind = "unknown"


class NetworkError(PaymentError):
    kind = "transient"


class GatewayTimeout(PaymentError):
    kind = "transient"


class DatabaseError(PaymentError):
    kind = "persistent"


class ValidationError(PaymentError):
    kind = "invalid"


def make_logger(name):
    LOG_DIR.mkdir(exist_ok=True)
    log = logging.getLogger(name)
    log.setLevel(logging.INFO)
    for old in log.handlers[:]:
        old.close()
        log.removeHandler(old)
    handler = logging.FileHandler(LOG_DIR / f"{name}.log", mode="w", encoding="utf-8")
    handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)-8s | %(message)s"))
    log.addHandler(handler)
    return log


def now():
    return datetime.now().strftime("%H:%M:%S.%f")[:-3]


def new_state():
    return {"done": [], "total": 0}


def fails(tx, attempt, failure):
    return tx[2] == failure and attempt <= FAILED_ATTEMPTS[failure]


def validate(tx):
    tx_id, amount, failure = tx
    if amount <= 0 or failure not in (None, *FAILED_ATTEMPTS):
        raise ValidationError(f"{tx_id}: invalid amount or failure type")


def process(tx, attempt):
    if fails(tx, attempt, "Network"):
        raise NetworkError(f"{tx[0]}: connection to bank gateway lost")
    if fails(tx, attempt, "Timeout"):
        raise GatewayTimeout(f"{tx[0]}: bank gateway did not answer in time")


def record(tx, attempt, state):
    tx_id, amount, _ = tx
    if tx_id in state["done"]:
        return
    state["total"] += amount
    if fails(tx, attempt, "Database"):
        raise DatabaseError(f"{tx_id}: payment record was not saved")
    state["done"].append(tx_id)


def handle(tx, attempt, state):
    validate(tx)
    process(tx, attempt)
    record(tx, attempt, state)


def amount_of(ids):
    return sum(amount for tx_id, amount, _ in TRANSACTIONS if tx_id in ids)


def summary(attempted, state):
    ok = len(state["done"])
    processed = amount_of(state["done"])
    return {
        "attempted": attempted,
        "successful": ok,
        "failed": len(TRANSACTIONS) - ok,
        "processed_amount": processed,
        "lost_amount": sum(t[1] for t in TRANSACTIONS) - processed,
        "ledger_total": state["total"],
        "completion_rate": round(ok / len(TRANSACTIONS) * 100, 2),
        "done": state["done"],
    }


def run_baseline():
    log = make_logger("baseline")
    state = new_state()
    attempted = 0
    try:
        for tx in TRANSACTIONS:
            attempted += 1
            handle(tx, 1, state)
            log.info("tx=%s amount=%d status=OK", tx[0], tx[1])
    except PaymentError as e:
        log.critical("tx=%s error=%s msg='%s' -> processor stopped", tx[0], type(e).__name__, e)
    return summary(attempted, state)


def run_with_exceptions():
    log = make_logger("exceptions")
    state = new_state()
    failures = []
    for tx in TRANSACTIONS:
        try:
            handle(tx, 1, state)
            log.info("tx=%s amount=%d status=OK", tx[0], tx[1])
        except PaymentError as e:
            name = type(e).__name__
            log.error("tx=%s failure=%s error=%s kind=%s action=skip msg='%s'", tx[0], tx[2], name, e.kind, e)
            failures.append({"tx": tx[0], "failure": tx[2], "error": name, "kind": e.kind, "status": "FAILED"})
    result = summary(len(TRANSACTIONS), state)
    result["failures"] = failures
    return result


def add_attempt(attempts, tx, attempt, failure, action, result):
    attempts.append({
        "step": len(attempts) + 1,
        "time": now(),
        "tx": tx[0],
        "attempt": attempt,
        "failure": failure,
        "action": action,
        "result": result,
    })


def try_with_retry(tx, state, log, attempts):
    for attempt in range(1, MAX_RETRIES + 2):
        try:
            handle(tx, attempt, state)
        except ValidationError as e:
            add_attempt(attempts, tx, attempt, "ValidationError", "reject", "fail")
            log.error("tx=%s rejected, no retry: %s", tx[0], e)
            return "rejected"
        except PaymentError as e:
            name = type(e).__name__
            if attempt > MAX_RETRIES:
                add_attempt(attempts, tx, attempt, name, "give up", "fail")
                log.error("tx=%s attempt=%d error=%s retries exhausted", tx[0], attempt, name)
                return "failed"
            add_attempt(attempts, tx, attempt, name, "retry", "fail")
            log.warning("tx=%s attempt=%d error=%s -> retry", tx[0], attempt, name)
            time.sleep(BACKOFF * 2 ** (attempt - 1))
            continue
        add_attempt(attempts, tx, attempt, "-", "commit", "success")
        log.info("tx=%s attempt=%d amount=%d status=OK", tx[0], attempt, tx[1])
        return "ok"


def save_checkpoint(checkpoints, state, log, reason):
    cp = {"name": f"CP{len(checkpoints) + 1}", "time": now(), "reason": reason, "state": deepcopy(state)}
    checkpoints.append(cp)
    (LOG_DIR / "checkpoints.json").write_text(json.dumps(checkpoints, indent=2))
    log.info("CHECKPOINT %s successful=%d total=%d reason=%s", cp["name"], len(state["done"]), state["total"], reason)


def rollback(checkpoints, state, log, tx):
    target = checkpoints[-1] if checkpoints else {"name": "START", "state": new_state()}
    restored = deepcopy(target["state"])
    dropped = [i for i in state["done"] if i not in restored["done"]]
    log.error("ROLLBACK tx=%s to=%s ledger %d -> %d dropped=%s",
              tx[0], target["name"], state["total"], restored["total"], dropped)
    return restored, target["name"]


def run_fault_tolerant():
    log = make_logger("fault_tolerant")
    state = new_state()
    checkpoints = []
    attempts = []
    rollbacks = 0
    for tx in TRANSACTIONS:
        status = try_with_retry(tx, state, log, attempts)
        if status == "ok" and len(state["done"]) % CHECKPOINT_EVERY == 0:
            save_checkpoint(checkpoints, state, log, f"{CHECKPOINT_EVERY} successful")
        if status != "failed":
            continue
        state, cp_name = rollback(checkpoints, state, log, tx)
        attempts[-1]["action"] = f"rollback to {cp_name}"
        attempts[-1]["result"] = "rolled back"
        rollbacks += 1
    if not checkpoints or checkpoints[-1]["state"] != state:
        save_checkpoint(checkpoints, state, log, "end of batch")

    with open(LOG_DIR / "retry_log.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=attempts[0].keys())
        writer.writeheader()
        writer.writerows(attempts)

    result = summary(len(TRANSACTIONS), state)
    result["retries"] = sum(1 for a in attempts if a["action"] == "retry")
    result["rollbacks"] = rollbacks
    result["checkpoints"] = checkpoints
    result["attempts"] = attempts
    return result


def compare(base, ft):
    faulty = [t[0] for t in TRANSACTIONS if t[2]]
    recovered = [i for i in faulty if i in ft["done"]]
    return {
        "extra_successful": ft["successful"] - base["successful"],
        "completion_gain_pp": round(ft["completion_rate"] - base["completion_rate"], 2),
        "recovered_faulty": f"{len(recovered)}/{len(faulty)}",
        "recovery_rate_pct": round(len(recovered) / len(faulty) * 100, 2),
        "loss_reduction_count_pct": round((base["failed"] - ft["failed"]) / base["failed"] * 100, 2),
        "loss_reduction_amount_pct": round((base["lost_amount"] - ft["lost_amount"]) / base["lost_amount"] * 100, 2),
    }


def main():
    base = run_baseline()
    exc = run_with_exceptions()
    ft = run_fault_tolerant()
    diff = compare(base, ft)
    results = {"baseline": base, "exceptions": exc, "fault_tolerant": ft, "comparison": diff}
    (LOG_DIR / "results.json").write_text(json.dumps(results, indent=2))

    print(f"{'metric':<18}{'baseline':>11}{'exceptions':>12}{'fault-tol':>11}")
    for key in ["attempted", "successful", "failed", "processed_amount", "lost_amount", "ledger_total", "completion_rate"]:
        print(f"{key:<18}{base[key]:>11}{exc[key]:>12}{ft[key]:>11}")
    print(f"retries={ft['retries']} rollbacks={ft['rollbacks']}")
    for cp in ft["checkpoints"]:
        print(f"{cp['name']}: successful={len(cp['state']['done'])} total={cp['state']['total']} ({cp['reason']})")
    for key, value in diff.items():
        print(f"{key}: {value}")


if __name__ == "__main__":
    main()
