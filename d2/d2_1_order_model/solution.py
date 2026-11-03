"""D2.1 Core: frozen, validated Order model.

Implementation for this task; run its adjacent pytest suite from the project root.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from d1.d1_1_csv_ingestion.solution import ValidationError, parse_amount
from d1.d1_2_dates.solution import normalise_ts
from d1.d1_3_text_cleaning.solution import validate_email


@dataclass(frozen=True)
class Order:
    order_id: int
    email: str
    amount: Decimal
    currency: str
    ordered_at: datetime

    def __post_init__(self):
        if not isinstance(self.order_id, int) or isinstance(self.order_id, bool) or self.order_id <= 0:
            raise ValidationError('order_id must be a positive integer')
        validate_email(self.email)
        if not isinstance(self.amount, Decimal):
            raise ValidationError('amount must be Decimal')
        parse_amount(self.amount, positive=True)
        if not isinstance(self.currency, str) or not re.fullmatch('[A-Z]{3}', self.currency):
            raise ValidationError('currency must have three uppercase letters')
        if not isinstance(self.ordered_at, datetime) or self.ordered_at.utcoffset() is None:
            raise ValidationError('ordered_at must be timezone-aware')
        object.__setattr__(self, 'ordered_at', self.ordered_at.astimezone(timezone.utc))

    @classmethod
    def from_row(cls, row: dict, source_tz='UTC'):
        try:
            return cls(int(row['order_id']), row['email'], parse_amount(row['amount'], positive=True),
                       row['currency'], datetime.fromisoformat(normalise_ts(row['ordered_at'], source_tz)))
        except (KeyError, TypeError, ValueError) as exc:
            raise ValidationError(f'invalid order: {exc}') from exc

    def to_row(self):
        return dict(order_id=str(self.order_id), email=self.email, amount=str(self.amount),
                    currency=self.currency, ordered_at=self.ordered_at.isoformat())
