"""Deterministic minute-frequency local-provider acceptance strategy."""

from jqdata import *

SECURITIES = [
    "000001.XSHE",
    "000002.XSHE",
    "000063.XSHE",
    "000333.XSHE",
    "000651.XSHE",
]


def initialize(context):
    set_benchmark("000300.XSHG")
    g.invested = False
    run_daily(open_positions, time="09:31")


def open_positions(context):
    if g.invested:
        return
    target_value = context.portfolio.total_value * 0.18
    for security in SECURITIES:
        order_target_value(security, target_value)
    g.invested = True
