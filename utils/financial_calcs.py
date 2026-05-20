"""
Financial calculation helpers for the gold-layer affordability marts.

These are pure functions (no Spark dependency) so they can be unit-tested
with pytest in isolation. The Spark notebooks wrap them in UDFs or apply
them to aggregated pandas frames at the metro/county grain.
"""


def monthly_mortgage_payment(home_price: float,
                             annual_rate_pct: float,
                             down_payment_pct: float = 20.0,
                             term_years: int = 30) -> float:
    """
    Principal & interest portion of a monthly mortgage payment.

    Uses the standard amortizing-loan formula:
        M = P * [ r(1+r)^n ] / [ (1+r)^n - 1 ]
    where P = loan principal, r = monthly rate, n = number of payments.

    Note: P&I only. Excludes taxes, insurance, PMI — documented assumption.
    """
    loan = home_price * (1 - down_payment_pct / 100.0)
    monthly_rate = (annual_rate_pct / 100.0) / 12.0
    n = term_years * 12

    if monthly_rate == 0:                 # guard against zero-rate edge case
        return loan / n
    factor = (1 + monthly_rate) ** n
    return loan * (monthly_rate * factor) / (factor - 1)


def affordability_index(median_household_income: float,
                        home_price: float,
                        annual_rate_pct: float,
                        down_payment_pct: float = 20.0,
                        term_years: int = 30) -> float:
    """
    Ratio of monthly income to required monthly P&I payment.

    >= 1.0  : median household can afford the median home
    <  1.0  : affordability stress (lower is worse)
    """
    payment = monthly_mortgage_payment(
        home_price, annual_rate_pct, down_payment_pct, term_years)
    if payment <= 0:
        return float("inf")
    return (median_household_income / 12.0) / payment


def price_to_income_ratio(home_price: float,
                          median_household_income: float) -> float:
    """
    Median home price divided by median annual household income.
    Historically, > 4.0 is considered stressed; > 5.0 severely so.
    """
    if median_household_income <= 0:
        return float("inf")
    return home_price / median_household_income


def rent_to_price_ratio(annual_rent: float, home_price: float) -> float:
    """
    Gross annual rent as a fraction of home price (investor 'yield' proxy).
    The '1% rule' corresponds to a monthly ratio of 0.01 (12% annual).
    """
    if home_price <= 0:
        return 0.0
    return annual_rent / home_price


def months_of_supply(active_listings: float,
                     trailing_3mo_sales: float) -> float:
    """
    Active inventory divided by the trailing-3-month average monthly sales.
    < 4 = seller's market, 4-6 = balanced, > 6 = buyer's market.

    Pass trailing_3mo_sales as the AVERAGE monthly sales over 3 months,
    not the 3-month total.
    """
    if trailing_3mo_sales <= 0:
        return float("inf")
    return active_listings / trailing_3mo_sales
