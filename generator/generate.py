"""Synthetic UPI transaction generator.

Builds a small seeded population of users/merchants, then streams
transaction events (mostly legitimate, occasionally fraudulent) to
date/hour-partitioned JSONL files.

Usage:
    python generate.py --rate 5 --fraud-rate 0.02 --output-dir data/stream
"""

import argparse
import math
import random
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path

IST = timezone(timedelta(hours=5, minutes=30))

NUM_USERS = 500
NUM_MERCHANTS = 80

INDIAN_STATES = [
    "Maharashtra", "Karnataka", "Delhi", "Tamil Nadu", "West Bengal",
    "Gujarat", "Uttar Pradesh", "Telangana", "Rajasthan", "Kerala",
    "Punjab", "Haryana", "Madhya Pradesh", "Bihar", "Odisha",
]

BANK_HANDLES = ["oksbi", "okhdfcbank", "okicici", "okaxis", "ybl", "paytm", "okbizaxis"]

MERCHANT_CATEGORIES = [
    "grocery", "food_delivery", "fuel", "travel", "entertainment",
    "utilities", "ecommerce", "healthcare", "education", "electronics",
    "fashion", "pharmacy",
]


@dataclass
class User:
    user_id: str
    upi: str
    state: str
    devices: list = field(default_factory=list)
    typical_spend: float = 500.0


@dataclass
class Merchant:
    merchant_id: str
    upi: str
    category: str


def build_users(n: int) -> list:
    users = []
    for i in range(n):
        bank = random.choice(BANK_HANDLES)
        num_devices = random.randint(1, 3)
        users.append(User(
            user_id=f"user{i:04d}",
            upi=f"user{i:04d}@{bank}",
            state=random.choice(INDIAN_STATES),
            devices=[f"user{i:04d}_{j}" for j in range(num_devices)],
            typical_spend=round(random.lognormvariate(6.2, 0.7), 2),  # right-skewed spend per user, median ~e^6.2≈490
        ))
    return users


def build_merchants(n: int) -> list:
    merchants = []
    for i in range(n):
        bank = random.choice(BANK_HANDLES)
        merchants.append(Merchant(
            merchant_id=f"merchant{i:03d}",
            upi=f"merchant{i:03d}@{bank}",
            category=random.choice(MERCHANT_CATEGORIES),
        ))
    return merchants


STATUS_CHOICES = ["SUCCESS", "FAILED", "REVERSED"]
STATUS_WEIGHTS = [0.95, 0.04, 0.01]

def random_status() -> str:
    # generate k=1 random status based on weighted choices
    return random.choices(STATUS_CHOICES, weights=STATUS_WEIGHTS, k=1)[0]

def stranger_upi() -> str:
    bank = random.choice(BANK_HANDLES)
    i=random.randint(1,1000)
    return f"stranger{i:04d}@{bank}"

def new_device_id() -> str:
    import string
    ni="".join(random.choices(string.ascii_lowercase,k=4))
    i=random.randint(1,1000)
    j=random.randint(1,10)
    return f"{ni}{i:04d}_{j}"

from typing import Optional
from pydantic import BaseModel, field_validator, ConfigDict,model_validator
import re

class Transaction(BaseModel):
    model_config = ConfigDict(strict=True)  # no silent coercion

    txn_id: str
    timestamp: datetime
    sender_upi: str
    sender_state: str
    sender_device_id: str
    receiver_upi: str
    receiver_type: str
    receiver_category: Optional[str] = None
    amount: float
    status: str
    is_fraud: bool
    fraud_pattern: Optional[str] = None

    @field_validator("txn_id", "sender_upi", "sender_state", "sender_device_id",
                      "receiver_upi", "receiver_type", "status")
    @classmethod
    def not_null(cls, v):
        if v is None or v == "":
            raise ValueError("Value cannot be empty or whitespace")
        return v

    @field_validator("sender_upi", "receiver_upi")
    @classmethod
    def upi_format(cls, v: str) -> str:
        reg = re.compile(r"^[A-Za-z0-9]+@[A-Za-z]+$")
        if not reg.fullmatch(v):
            raise ValueError(f"Invalid UPI format: {v!r}")
        return v

    @field_validator("amount")
    @classmethod
    def amount_validate(cls, v: float) -> float:
        if v<=0:
            raise ValueError(f"Invalid amount: negative or zero")
        return v

    @field_validator("timestamp")
    @classmethod
    def must_be_aware(cls, v):
        if v.tzinfo is None:
            raise ValueError("timestamp must be timezone-aware")
        return v

    @model_validator(mode="after")
    def p2m_needs_category(self):
        if self.receiver_type == "P2M" and not self.receiver_category:
            raise ValueError("P2M transaction requires receiver_category")
        return self


def make_event(sender, receiver_upi, receiver_type, receiver_category,
               amount, device_id, status, is_fraud=False, fraud_pattern=None,
               timestamp=None) -> Transaction:
    ts = timestamp or datetime.now(IST)

    return Transaction(
        txn_id=str(uuid.uuid4()),
        timestamp=ts,
        sender_upi=sender.upi,
        sender_state=sender.state,
        sender_device_id=device_id,
        receiver_upi=receiver_upi,
        receiver_type=receiver_type,
        receiver_category=receiver_category,
        amount=round(amount, 2),
        status=status,
        is_fraud=is_fraud,
        fraud_pattern=fraud_pattern,
    )


def pick_receiver(sender, users, merchants):
    """60% P2M / 40% P2P, matching legitimate traffic mix."""
    if random.random() < 0.6:
        merchant = random.choice(merchants)
        return merchant.upi, "P2M", merchant.category
    other = random.choice(users)
    while other.user_id == sender.user_id:
        other = random.choice(users)
    return other.upi, "P2P", None


def legit_transaction(users, merchants) -> list:
    sender = random.choice(users)
    receiver_upi, receiver_type, category = pick_receiver(sender, users, merchants)
    amount = random.lognormvariate(math.log(max(sender.typical_spend, 10)), 0.5)
    return [make_event(
        sender, receiver_upi, receiver_type, category,
        amount, random.choice(sender.devices), random_status(),
    )]


def fraud_rapid_fire_burst(users, merchants) -> list:
    """Many small transfers to brand-new payees in quick succession."""
    sender = random.choice(users)
    device = random.choice(sender.devices)
    now = datetime.now(IST)
    events = []
    for i in range(random.randint(6, 12)): # burst transactions 6-12 to unknown of ntb upi
        ts = now + timedelta(seconds=i * random.uniform(1, 4))
        events.append(make_event(
            sender, stranger_upi(), "P2P", None,
            random.uniform(50, 500), device, "SUCCESS",
            is_fraud=True, fraud_pattern="rapid_fire_burst", timestamp=ts,
        ))
    return events


def fraud_odd_hour_high_value(users, merchants) -> list:
    """A single high-value transaction at an unusual hour (1-4 AM)."""
    sender = random.choice(users)
    receiver_upi, receiver_type, category = pick_receiver(sender, users, merchants)
    odd_hour_ts = datetime.now(IST).replace(
        hour=random.randint(1, 4), minute=random.randint(0, 59), second=random.randint(0, 59),
    )
    amount = sender.typical_spend * random.uniform(8, 15) # 8x to 15x typical spends in odd hours
    return [make_event(
        sender, receiver_upi, receiver_type, category,
        amount, random.choice(sender.devices), random_status(),
        is_fraud=True, fraud_pattern="odd_hour_high_value", timestamp=odd_hour_ts,
    )]


def fraud_amount_anomaly(users, merchants) -> list:
    """A single transaction 20-50x the sender's typical spend."""
    sender = random.choice(users)
    receiver_upi, receiver_type, category = pick_receiver(sender, users, merchants)
    amount = sender.typical_spend * random.uniform(20, 50)
    return [make_event(
        sender, receiver_upi, receiver_type, category,
        amount, random.choice(sender.devices), random_status(),
        is_fraud=True, fraud_pattern="amount_anomaly",
    )]


def fraud_unseen_device_drain(users, merchants) -> list:
    """Several high-value transfers from a device the user has never used."""
    sender = random.choice(users)
    device = new_device_id()  # not in sender.devices
    now = datetime.now(IST)
    events = []
    for i in range(random.randint(3, 6)):
        ts = now + timedelta(seconds=i * random.uniform(2, 6))
        amount = sender.typical_spend * random.uniform(5, 15)
        events.append(make_event(
            sender, stranger_upi(), "P2P", None,
            amount, device, "SUCCESS",
            is_fraud=True, fraud_pattern="unseen_device_drain", timestamp=ts,
        ))
    return events


FRAUD_GENERATORS = [
    fraud_rapid_fire_burst,
    fraud_odd_hour_high_value,
    fraud_amount_anomaly,
    fraud_unseen_device_drain,
]


def next_events(users, merchants, fraud_rate) -> list:
    if random.random() < fraud_rate:
        generator = random.choice(FRAUD_GENERATORS)
        return generator(users, merchants)
    return legit_transaction(users, merchants)

#################kafka############

def make_producer(bootstrap_servers: str) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: v.encode("utf-8"),
        enable_idempotence=True,
    )

def publish_event(producer: KafkaProducer, event: Transaction, topic: str) -> None:
    producer.send(topic, key=event.sender_upi, value=event.model_dump_json())


def write_event(event: Transaction, output_dir: Path) -> Path:
    ts = event.timestamp
    path = output_dir / ts.strftime("%Y-%m-%d") / f"{ts.strftime('%H')}.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(event.model_dump_json() + "\n")
    return path


def main():

    # python generate.py --output-dir .\.local\generator_sample --rate 5.0 --fraud-rate .1 --count 100 --seed 42
    parser = argparse.ArgumentParser(description="Generate synthetic UPI transaction events.")
    parser.add_argument("--output-dir", default="data/stream", help="root directory for partitioned JSONL output")
    parser.add_argument("--rate", type=float, default=5.0, help="events per second")
    parser.add_argument("--fraud-rate", type=float, default=0.02, help="probability of a fraud incident per generated batch")
    parser.add_argument("--count", type=int, default=None, help="stop after N events (default: run until Ctrl+C)")
    parser.add_argument("--seed", type=int, default=42,help="baseline for random operations . same seed will generate same population everytime" )
    args = parser.parse_args()

    random.seed(args.seed)
    output_dir = Path(args.output_dir)

    users = build_users(NUM_USERS)
    merchants = build_merchants(NUM_MERCHANTS)

    delay = 1.0 / args.rate if args.rate > 0 else 0
    written = 0
    try:
        while args.count is None or written < args.count:
            for event in next_events(users, merchants, args.fraud_rate):
                path = write_event(event, output_dir)
                written += 1
                tag = f"FRAUD:{event.fraud_pattern}" if event.is_fraud else "legit"
                print(f"[{written}] {event.txn_id} {tag} -> {path}")
                if delay:
                    time.sleep(delay)
                if args.count is not None and written >= args.count:
                    break
    except KeyboardInterrupt:
        pass
    print(f"stopped after {written} events")


if __name__ == "__main__":
    main()
