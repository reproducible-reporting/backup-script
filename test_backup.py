"""Unit tests for the backup sript."""

from datetime import datetime

from backup import grandfatherson


def test_grandfatherson_none():
    dts = [
        datetime(2022, 5, 5, 0, 0, 0),
        datetime(2022, 5, 4, 0, 0, 0),
        datetime(2022, 5, 2, 0, 0, 0),
        datetime(2022, 5, 1, 0, 0, 0),
    ]
    assert grandfatherson(dts) == (dts[:1], dts[1:])


def test_grandfatherson_tenminutely():
    dts = [
        datetime(2022, 5, 5, 17, 30, 0),
        datetime(2022, 5, 5, 17, 25, 0),
        datetime(2022, 5, 5, 17, 20, 0),
        datetime(2022, 5, 5, 16, 55, 0),
        datetime(2022, 5, 5, 16, 50, 0),
        datetime(2022, 5, 5, 16, 40, 0),
        datetime(2022, 5, 5, 16, 30, 0),
    ]
    keep_dts = [dts[0], dts[2], dts[4], dts[5]]
    prune_dts = [dts[1], dts[3], dts[6]]
    assert grandfatherson(dts, tenminutely=3) == (keep_dts, prune_dts)


def test_grandfatherson_hourly():
    dts = [
        datetime(2022, 5, 8, 0, 0, 0),
        datetime(2022, 5, 5, 17, 0, 0),
        datetime(2022, 5, 5, 16, 30, 0),
        datetime(2022, 5, 5, 16, 0, 0),
        datetime(2022, 5, 5, 15, 0, 0),
    ]
    keep_dts = [dts[0], dts[1], dts[3]]
    prune_dts = [dts[2], dts[4]]
    assert grandfatherson(dts, hourly=3) == (keep_dts, prune_dts)


def test_grandfatherson_daily1():
    dts = [
        datetime(2022, 5, 5, 0, 0, 0),
        datetime(2022, 5, 4, 0, 0, 0),
        datetime(2022, 5, 2, 0, 0, 0),
        datetime(2022, 5, 1, 0, 0, 0),
    ]
    assert grandfatherson(dts, daily=3) == (dts[:-1], dts[-1:])


def test_grandfatherson_daily2():
    dts = [
        datetime(2022, 5, 5, 10, 0, 0),
        datetime(2022, 5, 4, 10, 0, 0),
        datetime(2022, 5, 4, 9, 0, 0),
        datetime(2022, 5, 2, 10, 0, 0),
        datetime(2022, 5, 1, 10, 0, 0),
    ]
    keep_dts = [dts[0], dts[2], dts[3]]
    prune_dts = [dts[1], dts[4]]
    assert grandfatherson(dts, daily=3) == (keep_dts, prune_dts)


def test_grandfatherson_daily_too_few():
    dts = [
        datetime(2022, 5, 5, 0, 0, 0),
        datetime(2022, 5, 4, 0, 0, 0),
    ]
    assert grandfatherson(dts, daily=3) == (dts[:2], [])


def test_grandfatherson_daily_too_few_oldest1():
    dts = [
        datetime(2022, 5, 5, 10, 0, 0),
        datetime(2022, 5, 4, 10, 0, 0),
        datetime(2022, 5, 4, 9, 0, 0),
    ]
    keep_dts = [dts[0], dts[2]]
    prune_dts = [dts[1]]
    assert grandfatherson(dts, daily=3) == (keep_dts, prune_dts)


def test_grandfatherson_daily_too_few_oldest2():
    dts = [
        datetime(2022, 5, 5, 10, 0, 0),
        datetime(2022, 5, 5, 8, 0, 0),
    ]
    assert grandfatherson(dts, daily=3) == (dts, [])


def test_grandfatherson_monthly():
    dts = [
        datetime(2022, 5, 5, 0, 0, 0),
        datetime(2022, 5, 4, 0, 0, 0),
        datetime(2022, 5, 2, 0, 0, 0),
        datetime(2022, 5, 1, 0, 0, 0),
        datetime(2022, 4, 20, 0, 0, 0),
        datetime(2022, 4, 10, 0, 0, 0),
        datetime(2022, 3, 8, 0, 0, 0),
        datetime(2022, 3, 7, 0, 0, 0),
        datetime(2022, 2, 8, 0, 0, 0),
    ]
    keep_dts = [dts[0], dts[3], dts[5], dts[7]]
    prune_dts = [dts[1], dts[2], dts[4], dts[6], dts[8]]
    assert grandfatherson(dts, monthly=3) == (keep_dts, prune_dts)


def test_grandfatherson_weekly():
    dts = [
        datetime(2022, 6, 13, 0, 0, 0),
        datetime(2022, 6, 12, 0, 0, 0),
        datetime(2022, 6, 11, 0, 0, 0),
        datetime(2022, 6, 7, 0, 0, 0),
        datetime(2022, 6, 6, 0, 0, 0),
        datetime(2022, 6, 5, 0, 0, 0),
        datetime(2022, 5, 29, 0, 0, 0),
        datetime(2022, 5, 30, 0, 0, 0),
    ]
    keep_dts, prune_dts = grandfatherson(dts, weekly=3)
    assert keep_dts == [dts[0], dts[4], dts[7]]
    assert prune_dts == [dts[1], dts[2], dts[3], dts[5], dts[6]]
