"""
Unit tests for utils/financial_calcs.py

Run from the repo root with:  pytest -q
These prove the gold-layer math before it ever touches Spark — the kind of
testing discipline the JD asks for ("data quality monitoring", "best practice").
"""
import sys
import os
import math

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "utils"))
import financial_calcs as fc  # noqa: E402


def test_monthly_payment_known_value():
    # $400k home, 20% down ($320k loan), 6% APR, 30yr.
    # Known amortized P&I is ~$1,918.56.
    payment = fc.monthly_mortgage_payment(400_000, 6.0)
    assert math.isclose(payment, 1918.56, abs_tol=1.0)


def test_zero_rate_is_straight_line():
    # At 0% interest, payment is simply principal / number of months.
    payment = fc.monthly_mortgage_payment(360_000, 0.0, down_payment_pct=0)
    assert math.isclose(payment, 360_000 / 360, abs_tol=0.01)


def test_affordability_index_threshold():
    # Income that exactly covers the payment should give index ~1.0.
    payment = fc.monthly_mortgage_payment(400_000, 6.0)
    income = payment * 12
    assert math.isclose(fc.affordability_index(income, 400_000, 6.0),
                        1.0, abs_tol=0.001)


def test_price_to_income_stress():
    assert fc.price_to_income_ratio(450_000, 90_000) == 5.0


def test_months_of_supply_sellers_market():
    # 1,200 active listings, 400 avg monthly sales -> 3.0 months (seller's).
    assert fc.months_of_supply(1200, 400) == 3.0


def test_guards_against_divide_by_zero():
    assert fc.price_to_income_ratio(300_000, 0) == float("inf")
    assert fc.months_of_supply(500, 0) == float("inf")
    assert fc.rent_to_price_ratio(24_000, 0) == 0.0
