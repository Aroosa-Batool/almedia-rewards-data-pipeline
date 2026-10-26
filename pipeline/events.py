"""Event contract + synthetic producer.

The contract (Pydantic model) is the single place where "what is a valid event"
is defined. Producers, the API and the stream consumer all use it.
"""
from __future__ import annotations

import json
import random
import uuid
from datetime import datetime, timedelta, timezone
from typing import Literal

from pydantic import BaseModel, Field, field_validator, model_validator

EventType = Literal["offer_click", "offer_complete", "reward_paid"]
COUNTRIES = ["DE", "US", "GB", "FR", "BR", "IN", "PK"]
ADVERTISERS = ["RaidQuest", "CoinMaster", "FarmLand", "ShopNow", "BankPlus"]


class RewardEvent(BaseModel):
    event_id: str = Field(min_length=8)
    event_type: EventType
    user_id: str = Field(min_length=1)
    offer_id: str = Field(min_length=1)
    advertiser: str
    country: str = Field(min_length=2, max_length=2)
    reward_cents: int = Field(default=0, ge=0, le=100_000)
    occurred_at: datetime

    @field_validator("country")
    @classmethod
    def upper_country(cls, v: str) -> str:
        return v.upper()

    @field_validator("occurred_at")
    @classmethod
    def not_in_future(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            v = v.replace(tzinfo=timezone.utc)
        if v > datetime.now(timezone.utc) + timedelta(minutes=5):
            raise ValueError("occurred_at is in the future (clock skew > 5 min)")
        return v

    @model_validator(mode="after")
    def reward_only_when_paid(self) -> "RewardEvent":
        if self.event_type == "reward_paid" and self.reward_cents == 0:
            raise ValueError("reward_paid must carry reward_cents > 0")
        return self

    def partition(self, partitions: int = 4) -> int:
        # Same user -> same partition -> per-user ordering is preserved (like Kafka keys).
        return hash_key(self.user_id) % partitions


def hash_key(key: str) -> int:
    # Stable across processes (Python's hash() is salted per process).
    h = 0
    for ch in key:
        h = (h * 31 + ord(ch)) & 0xFFFFFFFF
    return h


def generate(n: int, seed: int | None = None, bad_ratio: float = 0.03, dup_ratio: float = 0.05) -> list[dict]:
    """Realistic messy traffic: funnels per user, duplicates (retries), late and invalid events."""
    rng = random.Random(seed)
    now = datetime.now(timezone.utc)
    out: list[dict] = []
    while len(out) < n:
        user = f"u{rng.randint(1, 400):04d}"
        offer = f"o{rng.randint(1, 40):03d}"
        adv = ADVERTISERS[hash_key(offer) % len(ADVERTISERS)]
        country = rng.choice(COUNTRIES)
        t = now - timedelta(minutes=rng.randint(0, 60 * 24 * 7))
        funnel = ["offer_click"]
        if rng.random() < 0.35:
            funnel.append("offer_complete")
            if rng.random() < 0.9:
                funnel.append("reward_paid")
        reward = rng.choice([50, 120, 300, 750, 2000])
        for step, etype in enumerate(funnel):
            ev = {
                "event_id": str(uuid.UUID(int=rng.getrandbits(128))),
                "event_type": etype,
                "user_id": user,
                "offer_id": offer,
                "advertiser": adv,
                "country": country,
                "reward_cents": reward if etype == "reward_paid" else 0,
                "occurred_at": (t + timedelta(minutes=step * rng.randint(1, 90))).isoformat(),
            }
            if rng.random() < bad_ratio:
                ev[rng.choice(["reward_cents", "country", "event_type"])] = rng.choice([-5, "", "offer_view"])
            out.append(ev)
            if rng.random() < dup_ratio:
                out.append(dict(ev))  # producer retry -> exact duplicate
    rng.shuffle(out)  # arrival order != event order
    return out[:n]


if __name__ == "__main__":
    for e in generate(5, seed=1):
        print(json.dumps(e))
