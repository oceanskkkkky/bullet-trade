"""Deterministic multi-security benchmark for local backend parity/performance."""

from jqdata import *

SECURITIES = [
    "000001.XSHE",
    "000002.XSHE",
    "000063.XSHE",
    "000333.XSHE",
    "000651.XSHE",
    "000858.XSHE",
    "002230.XSHE",
    "002415.XSHE",
    "002594.XSHE",
    "300059.XSHE",
    "300750.XSHE",
    "600000.XSHG",
    "600009.XSHG",
    "600036.XSHG",
    "600276.XSHG",
    "600519.XSHG",
    "601318.XSHG",
    "601398.XSHG",
    "601888.XSHG",
    "603259.XSHG",
]


def initialize(context):
    set_benchmark("000300.XSHG")
    g.invested = False
    run_daily(open_positions, time="open")


def open_positions(context):
    if g.invested:
        return
    target_value = context.portfolio.total_value * 0.045
    for security in SECURITIES:
        order_target_value(security, target_value)
    g.invested = True
