from datetime import datetime
from typing import Literal, Optional

from pydantic import BaseModel, Field, field_validator


class TransactionEvent(BaseModel):
    transaction_id: str
    merchant_id: str
    merchant_name: str
    amount: float
    currency: str = "USD"
    status: Literal["success", "failed", "declined"]
    payment_method: Literal["card", "bank_transfer", "wallet"]
    fraud_score: float
    processor_latency_ms: int = Field(gt=0)
    timestamp: datetime

    @field_validator("amount")
    @classmethod
    def validate_amount(cls, v: float) -> float:
        if v <= 0:
            raise ValueError("amount must be greater than 0")
        return v

    @field_validator("fraud_score")
    @classmethod
    def validate_fraud_score(cls, v: float) -> float:
        if not (0.0 <= v <= 1.0):
            raise ValueError("fraud_score must be between 0.0 and 1.0")
        return v


class AlertRecord(BaseModel):
    alert_id: str
    merchant_id: str
    merchant_name: str
    metric: Literal["success_rate", "volume", "fraud_score", "latency"]
    current_value: float
    baseline_value: float
    z_score: float
    description: str
    fired_at: datetime
    resolved_at: Optional[datetime] = None
