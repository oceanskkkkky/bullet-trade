"""缓存目录与磁盘读写回归测试。"""

import os
from pathlib import Path

import pandas as pd

from bullet_trade.data.cache import CacheManager


def test_relative_cache_dir_persists_dataframe_and_reuses_it(tmp_path, monkeypatch):
    """相对目录在 Windows 下可被标准库和 pandas 一致地使用。"""
    monkeypatch.chdir(tmp_path)
    cache = CacheManager("tushare", cache_dir=".\\.bullet-trade\\cache")
    source = pd.DataFrame({"close": [10.0]}, index=pd.to_datetime(["2025-04-09"]))
    fetch_calls = 0

    def fetch(_kwargs):
        nonlocal fetch_calls
        fetch_calls += 1
        return source

    first = cache.cached_call(
        "get_price",
        {"security": "000001.XSHE", "start_date": "2025-04-09", "end_date": "2025-04-09"},
        fetch,
        result_type="df",
    )
    second = cache.cached_call(
        "get_price",
        {"security": "000001.XSHE", "start_date": "2025-04-09", "end_date": "2025-04-09"},
        fetch,
        result_type="df",
    )

    assert fetch_calls == 1
    pd.testing.assert_frame_equal(first, second)
    assert list(Path(cache.cache_dir).rglob("*.meta.json"))


def test_cache_dir_expands_tilde_before_dataframe_io(tmp_path, monkeypatch):
    """用户目录写法不能在 os.makedirs 与 pandas IO 间产生两套路径。"""
    fake_home = tmp_path / "home"
    monkeypatch.setenv("HOME", str(fake_home))
    monkeypatch.setenv("USERPROFILE", str(fake_home))

    cache = CacheManager("tushare", cache_dir="~/.bullet-trade/cache")

    assert os.path.normpath(cache.cache_dir) == os.path.normpath(
        str(fake_home / ".bullet-trade" / "cache")
    )
