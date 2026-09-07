from __future__ import annotations

import math
from dataclasses import dataclass


@dataclass
class WelfordState:
    n: int = 0
    mean: float = 0.0
    M2: float = 0.0

    def update(self, value: float) -> None:
        self.n += 1
        delta = value - self.mean
        self.mean += delta / self.n
        delta2 = value - self.mean
        self.M2 += delta * delta2

    @property
    def variance(self) -> float:
        if self.n < 2:
            return 0.0
        return self.M2 / (self.n - 1)

    @property
    def std(self) -> float:
        return math.sqrt(self.variance)

    def z_score(self, value: float) -> float:
        if self.std == 0:
            return 0.0
        return (value - self.mean) / self.std

    def to_dict(self) -> dict:
        return {"n": self.n, "mean": self.mean, "M2": self.M2}

    @classmethod
    def from_dict(cls, data: dict) -> "WelfordState":
        return cls(n=data["n"], mean=data["mean"], M2=data["M2"])
