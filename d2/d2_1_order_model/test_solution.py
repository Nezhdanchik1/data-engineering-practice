from dataclasses import FrozenInstanceError
from datetime import datetime, timezone, timedelta
from decimal import Decimal
import pytest
from d2.d2_1_order_model.solution import Order
from d1.d1_1_csv_ingestion.solution import ValidationError

BASE = dict(order_id=1, email='aru@example.com', amount=Decimal('1.20'), currency='USD', ordered_at=datetime(2026,10,1,tzinfo=timezone.utc))

def test_round_trip_and_frozen():
    order = Order(**BASE)
    assert Order.from_row(order.to_row()) == order
    with pytest.raises(FrozenInstanceError):
        order.currency = 'EUR'

def test_offset_normalised():
    order = Order(**{**BASE, 'ordered_at': datetime(2026,10,1,2,tzinfo=timezone(timedelta(hours=2)))})
    assert order.ordered_at.hour == 0 and order.ordered_at.tzinfo == timezone.utc
    assert Order.from_row(order.to_row()) == order

@pytest.mark.parametrize('column,value', [('amount', Decimal('0')), ('amount', Decimal('-1')), ('amount', Decimal('NaN')), ('amount', 1.2), ('currency', 'usd'), ('currency', 'US'), ('ordered_at', datetime(2026,10,1)), ('order_id', 0), ('email', 'bad')])
def test_validation(column, value):
    with pytest.raises(ValidationError):
        Order(**{**BASE, column: value})

def test_from_row_missing_and_invalid_date():
    with pytest.raises(ValidationError):
        Order.from_row({})
    with pytest.raises(ValidationError, match='2026-13-01'):
        Order.from_row({**Order(**BASE).to_row(), 'ordered_at': '2026-13-01'})
