# 按名称直接访问数据提供者

适用场景：策略需要调用某个数据源的特有接口（如聚宽 `get_is_st`、Tushare 特有字段），又不想扩展统一数据 API。通过 `get_data_provider("jqdata")` 可直接拿到对应实例并调用其原生/特有方法，不会影响全局默认数据源。

## 快速使用

```python
from bullet_trade.data.api import get_data_provider,set_data_provider

#默认数据源
ts = set_data_provider('tushare')
print(get_security_info('000001.XSHE').display_name)

# 不改变默认数据源，单独拿到 JQData 实例
jq = get_data_provider("jqdata")
# 调用特有接口（若 provider 未封装，回退到 SDK）
is_st = jq.get_extras('is_st', ['000001.XSHE', '000018.XSHE'], start_date='2013-12-01', end_date='2013-12-03')
stocks = jq.get_index_stocks("399101.XSHE", date="2025-11-25")
```

要点：
- **不修改默认数据源**：`get_data_provider("jqdata")` 只返回实例，默认数据源仍保持原值。
- **实例缓存**：同名 provider 重复获取返回同一实例，避免重复初始化/认证。
- **获取即认证**：若需要账号（如 JQData/Tushare），在获取时即尝试认证；缺少凭证会抛出清晰错误。

## SDK 回退规则（仅限同一数据源）

当 provider 未实现某方法时，会按以下顺序回退到“同一数据源”的 SDK/客户端；不会跨数据源切换。

| provider_name | 回退目标（按优先级） | 备注 |
| --- | --- | --- |
| jqdata | `jqdatasdk` 模块 | 直接调用 SDK |
| tushare | `tushare.pro_api()` 客户端 → `tushare` 模块 | 需提前配置 `TUSHARE_TOKEN` |
| qmt / miniqmt | `xtquant.xtdata` 模块 | 需安装 miniQMT/xtquant |
| remote_qmt | 无回退 | 仅抛出缺失提示 |
| local | 无网络回退；可通过 `.backend` 访问稳定的 `LocalDataBackend` | 保持离线回测可复现性 |

回退失败会抛出类似错误：`jqdata 未实现 special_method，已尝试回退到同名 provider 的 SDK/客户端: jqdatasdk`。

## 可移植性提示

- 直接调用特有接口会降低“跨数据源”可移植性，迁移到其他数据源时需自行兼容或降级。
- 建议仅在必要时使用直连，能统一的接口优先走 `bullet_trade.data.api`。

## 示例策略

`tests/strategies/strategy_small_cap_direct_provider_access.py` 演示：
- 默认数据源设为 QMT，对照 JQData 价格；
- 通过 `get_data_provider("jqdata")` 获取指数成分、市值前 10；
- 缺少账户或依赖时自动 `pytest.skip`，避免阻塞回归。
