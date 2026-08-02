from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from generate import Transaction


def valid_transaction_kwargs(**overrides):
    base = {
    "txn_id": '8b4e86d3-fbad-4450-98d4-3d7d0e48313e',
    "timestamp": datetime.fromisoformat('2026-08-02T12:04:42.341331+05:30'),
    "sender_upi": 'user0472@okbizaxis',
    "sender_state": 'Telangana',
    "sender_device_id": 'user0472_0',
    "receiver_upi": 'user0354@oksbi',
    "receiver_type": 'P2P',
    "receiver_category": None,
    "amount": 1343.38,
    "status": 'SUCCESS',
    "is_fraud": False,
    "fraud_pattern": None,
    }
    base.update(overrides)
    return base

#checks if data OP is same as input as expected
def test_transaction_accepts_valid_event():
    txn = Transaction(**valid_transaction_kwargs())
    assert txn.txn_id == '8b4e86d3-fbad-4450-98d4-3d7d0e48313e'
    assert txn.amount == 1343.38
    assert txn.is_fraud is False

#checks if ValidationError is raised when amt <=0 as per pydantic data contract
def test_amount():
    with pytest.raises(ValidationError):
        Transaction(**valid_transaction_kwargs(amount=-5))


# timestamp format check

def test_transaction_rejects_naive_timestamp(): 
    with pytest.raises(ValidationError):
        Transaction(**valid_transaction_kwargs(timestamp=datetime(2026, 8, 2, 12, 4, 42))) # noqa: DTZ001 — intentionally naive, testing rejection
        # no tzinfo passed => naive => must_be_aware should reject it


def test_transaction_accepts_aware_timestamp():
    txn = Transaction(**valid_transaction_kwargs(
        timestamp=datetime(2026, 8, 2, 12, 4, 42, tzinfo=timezone.utc)
    ))
    assert txn.timestamp.tzinfo is not None

# all merchent should have receiver_category
def test_transaction_rejects_p2m_without_category():
    with pytest.raises(ValidationError):
        Transaction(**valid_transaction_kwargs(receiver_type='P2M',receiver_category=None))  


## Fraud event generators tests

from generate import FRAUD_GENERATORS, build_merchants, build_users


#tests where event generated bt FRAUD_GENERATORS are frauds
@pytest.mark.parametrize("fraud_fn", FRAUD_GENERATORS)
def test_fraud_generators_label_correctly(fraud_fn):
    users = build_users(10)
    merchants = build_merchants(5)
    events = fraud_fn(users, merchants)
    for event in events:
        assert event.is_fraud is True
        assert event.fraud_pattern is not None

# check the results for randomness with fixed seed

import random


def test_same_seed_reproducible():
    random.seed(42)
    users1 = build_users(10)

    random.seed(42)
    users2 = build_users(10)

    assert users1 == users2


# ruff check