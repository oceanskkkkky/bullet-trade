# LocalDataProvider（Parquet / DuckDB）

`LocalDataProvider` 是稳定的聚宽兼容门面，可从 Parquet 事实源或不可变 DuckDB generation 读取股票、ETF、指数、财务和公司行为数据。策略查询阶段不访问网络，也不会在 DuckDB 缺分片时静默回退到 Parquet。

## 安装与配置

```bash
pip install -e ".[local]"
```

在项目根目录的 `.env` 中配置：

```env
DEFAULT_DATA_PROVIDER=local
LOCAL_DATA_BACKEND=parquet
LOCAL_DATA_PATH=./data/parquet
LOCAL_DATA_STRICT=true
```

默认路径就是 `./data/parquet`，相对于 BulletTrade 项目根目录解析。在 Windows 上可以继续使用正斜杠，也可以写成 `..\market-data\parquet`。不要把机器盘符写进代码。

也可在 Python 中显式切换：

```python
from bullet_trade.data.api import get_price, set_data_provider

set_data_provider("local", path="./data/parquet")
bars = get_price(
    "000001.XSHE",
    start_date="2025-01-01",
    end_date="2025-01-31",
    fields=["open", "close", "volume", "money"],
)
```

`parquet`、`local_parquet` 和 `local-parquet` 都是 `local` 的兼容别名。

## DuckDB 高性能模式

推荐使用 Python 3.10 和 DuckDB 1.5.4。仓库提供的 Conda 参考环境为：

```powershell
conda create -n bullet-trade-duckdb-py310 python=3.10 pip -y
conda run -n bullet-trade-duckdb-py310 python -m pip install -e ".[local,dev]"
```

先做只读预检，再构建全物化 generation：

```powershell
bullet-trade data build-duckdb --source ./data/parquet --output ./data/duckdb --dry-run
bullet-trade data build-duckdb --source ./data/parquet --output ./data/duckdb `
  --temp-directory ./data/duckdb-temp --threads 8 --memory-limit 16GB `
  --max-temp-size 600GB --source-file-batch-size 128 `
  --domains meta,daily,finance,actions,minute `
  --minute-quarterly-datasets stock_1m
```

构建器在 `generations/<id>/` 中写入新的不可变 shard，校验成功后才原子更新 `current-manifest.json`。失败或空间不足不会改变当前 generation。Windows 回测进程可继续持有旧 shard；不要手工覆盖已打开的 `.duckdb` 文件。

`--domains` 与 `--datasets` 可用于按领域或单数据集 pilot；`--minute-years 2025` 限定年份，`--minute-quarterly-datasets stock_1m` 只把股票 1m 细分到季度，其余分钟表按年度。构建默认 `--resume`，每个已校验时间分片都会写入 `build-state.json`；中断后使用完全相同的源、分片和构建参数即可续建。`--no-publish` 用于 pilot 验证。

增量构建使用 `--base-manifest <上一代 manifest.json>`。新 generation 会继承未被本次 `--domains/--datasets` 选中的不可变 shard：同一磁盘优先创建硬链接，跨磁盘安全复制；被选中的领域或分钟数据集会整体替换，因此不会把新旧分片混合。只迁移已有 generation、不扫描源数据时可组合使用 `--inherit-only --base-manifest ...`。每次构建完成后仍应以新 manifest 做接口与数据校验，再决定是否发布。

失败的 `.staging-*` 不会被运行时读取。旧 generation 默认只列出、不删除：

```powershell
bullet-trade data cleanup-duckdb --output ./data/duckdb --keep-generations 2
bullet-trade data cleanup-duckdb --output ./data/duckdb --keep-generations 2 --apply
```

使用 `--apply` 前必须关闭引用候选 generation 的 Windows 回测进程，并核对它不在 `current-manifest.json` 引用链上。大数据可以把输出根和 temp 放到不同外置盘；路径由命令或环境提供，代码和示例配置不依赖固定盘符。完整实测与资源边界见 [LOCAL_DUCKDB_BENCHMARK.md](LOCAL_DUCKDB_BENCHMARK.md)。

运行配置：

```env
DEFAULT_DATA_PROVIDER=local
LOCAL_DATA_BACKEND=duckdb
LOCAL_DATA_PATH=./data/duckdb/current-manifest.json
LOCAL_DATA_QUERY_MODE=batch
LOCAL_DATA_REQUIRE_CORPORATE_ACTIONS=true
LOCAL_DATA_REQUIRE_FUNDAMENTALS=false
LOCAL_DATA_DUCKDB_THREADS=8
LOCAL_DATA_DUCKDB_MEMORY_LIMIT=12GB
LOCAL_DATA_DUCKDB_TEMP_DIRECTORY=./data/duckdb-temp
LOCAL_DATA_DUCKDB_MAX_TEMP_SIZE=100GB
BT_BACKTEST_DATA_SESSION=true
BT_BACKTEST_DATA_SESSION_PRICE_BLOCKS=true
BT_BACKTEST_DATA_SESSION_MAX_BYTES=2147483648
BT_BACKTEST_DATA_SESSION_MIN_FREE_BYTES=2147483648
```

`LOCAL_DATA_QUERY_MODE=scalar` 是显式诊断/回滚路径；它仍读取同一 DuckDB generation，但禁用 Provider 的多证券批量入口。切回 Parquet 也必须显式修改 `LOCAL_DATA_BACKEND` 和路径。

引擎会在首个模拟交易日之前调用本地回测预检，验证交易日、股票日线、所选分钟频率、初始持仓证券、manifest 分片和日期覆盖。公司行动默认是必需数据；纯价格研究可显式设置 `LOCAL_DATA_REQUIRE_CORPORATE_ACTIONS=false`。策略需要财务查询时设置 `LOCAL_DATA_REQUIRE_FUNDAMENTALS=true`，使四张财务表也在模拟前校验。缺失项会给出重建或导入命令，DuckDB 模式不会回退扫描 Parquet。

策略无需修改数据 API。加载上述 `.env` 后照常运行：

```powershell
bullet-trade --env-file .env backtest ./strategies/my_strategy.py `
  --start 2025-01-01 --end 2025-12-31 --output ./backtest_results/local-duckdb
```

`BT_BACKTEST_DATA_SESSION_PRICE_BLOCKS=true` 开启 generation 隔离的 Arrow/Pandas 行情块缓存。`MAX_BYTES` 是当前进程上限，`MIN_FREE_BYTES` 是系统剩余内存保护线；并发回测时必须按进程数分摊，而不是每个进程使用整机预算。

## 外部数据目录

Provider 不要求数据必须位于仓库内。可以把外部目录直接作为只读数据源，无需复制：

```python
set_data_provider("local", path=r"..\market-data\parquet")
```

外部目录必须遵循当前数据集契约，核心文件包括：

```text
parquet/
├── <股票目录>/
│   ├── stock_basic_data.parquet
│   ├── stock_daily.parquet
│   └── stock_{1,5,15,30,60}min/<TS_CODE>.parquet
├── <ETF目录>/
│   ├── etf_basic_data.parquet
│   ├── etf_daily.parquet
│   └── etf_{1,5,15,30,60}min/<TS_CODE>.parquet
├── <指数目录>/
│   ├── index_basic.parquet
│   ├── index_daily/<TS_CODE>.parquet
│   └── index_{1,5,15,30,60}min/<TS_CODE>.parquet
└── <财务目录>/
    ├── income_cleaned.parquet
    ├── balancesheet_cleaned.parquet
    ├── cashflow_cleaned.parquet
    └── fina_indicator_cleaned.parquet
```

父目录名称可以不同；Provider 通过稳定文件名和行情子目录名发现数据。证券代码在存储层使用 Tushare 格式（如 `000001.SZ`），策略层仍使用聚宽格式（如 `000001.XSHE`）。

## 已支持能力

- 股票、ETF、指数的日线和 `1m/5m/15m/30m/60m` 行情。
- 股票增强日线优先使用 `stock_daily.parquet`；更早区间自动由 `daily_adj_*.parquet` 只读补齐，不会覆盖现代区间的完整字段。
- `pre`、`post` 和不复权查询；复权使用本地 `adj_factor`。
- 日线 `volume` 从手转换为股、`money` 从千元转换为元；分钟数据保持本地的股/元口径。
- 基于本地指数文件的交易日历、历史指数成分和权重。
- 股票、ETF、指数基础信息。
- `get_extras("is_st", ...)`。
- 聚宽 `query(...)` 的常用估值、利润表、资产负债表、现金流量表和财务指标字段；支持代码过滤、常用比较过滤、排序和 limit。
- `get_fundamentals` 按 `ann_date <= date` 选择当时已公告的最新报表，避免公告日未来数据；`statDate` 支持 `2024`、`2024q1` 或可解析日期。
- 存在规范 `corporate_actions.parquet` 或 DuckDB actions shard 时，`get_split_dividend()` 支持股票现金/送转和 ETF/基金派息。
- 多证券行情、当前快照、参考因子、证券信息和公司行为具有稳定批量后端契约；Parquet 为正确的 scalar fallback，DuckDB 为 native batch。

业绩预告和业绩快报没有一一对应的聚宽 `query(...)` 表，可通过稳定后端契约读取：

```python
import pandas as pd
from bullet_trade.data.api import get_data_provider

local = get_data_provider("local")
forecast = local.backend.read_financial_table(
    "forecast",
    ["ts_code", "type", "p_change_min", "p_change_max"],
    as_of=pd.Timestamp("2025-04-30"),
    securities=["000001.SZ"],
)
```

同一接口也支持 `express`；返回结果仍按公告日做时点过滤。

可以用诊断接口确认实际路径和后端：

```python
from bullet_trade.data.api import get_data_provider

print(get_data_provider().diagnostics())
```

## 公司行为准备

`adj_factor` 只能用于价格复权，不能还原现金入账、送转股资格、税额和实施状态。使用 Tushare `dividend`（股票）和 `fund_div`（基金/ETF）显式准备事件：

```powershell
bullet-trade data import-corporate-actions --root ./data/parquet `
  --output ./data/parquet/corporate_actions.parquet `
  --start 2020-01-01 --end 2025-12-31 `
  --checkpoint ./data/parquet/.corporate-actions-import.json `
  --checkpoint-every 25
```

命令默认读取 `TUSHARE_TOKEN`。证券类型来自本地 catalog，不按代码前缀猜测；导入会限速、保留报告期及公告/登记/支付/上市日期，以报告期和执行日期构造稳定 `source_event_id`，再用 payload hash 幂等合并并原子发布。公告日修订不会生成第二次派息，不同报告期即使共用除权日也会保留。默认从兼容 checkpoint 续传，每 25 个证券保存规范事件和完成集合；成功发布后自动清理临时状态。`--no-resume` 可显式重新开始。权限不足、接口限流、缺除权日或未实施/取消方案会报告为错误或拒绝行，默认终端只展示前 20 条拒绝示例。回测查询不会调用 Tushare。

如果数据集缺失，`get_split_dividend()` 会抛出可操作的 `LocalDataCapabilityError`，不会返回一个看似可信的空结果。

## Local 与 JQData 能力边界

| 接口 | Local | JQData | 主要差异 |
|---|---|---|---|
| `auth` | 本地 schema/manifest 校验 | 账号和网络认证 | Local 不访问网络 |
| `get_price` / 策略层 `history` | 支持 | 支持 | 签名一致；Local 忽略仅对 JQData price engine 有意义的 `prefer_engine/force_no_engine` |
| `get_bars` | 支持 | 支持 | Local 支持股票、基金、指数的 1d 与已物化 `1m/5m/15m/30m/60m` |
| `get_trade_days` / `get_trade_day` | 支持 | 支持 | Local 以本地指数日线交易日历覆盖为准 |
| `get_all_securities` / `get_security_info` | 支持 | 支持 | Local 以本地 catalog 和 generation 覆盖为准 |
| `get_index_stocks` / `get_index_weights` | 支持 | 支持 | Local 使用最近一个不晚于查询日的本地指数快照 |
| `get_fundamentals` / `get_fundamentals_continuously` | 有限支持 | 支持 | Local 仅执行已注册估值、利润、资产负债、现金流和财务指标字段及确定性 SQL 表达式子集 |
| `get_extras` | 有限支持 | 支持 | Local 当前仅支持 `is_st` |
| `get_split_dividend` | 条件支持 | 支持 | Local 必须先准备规范公司行为数据；另有后端级批量接口 |
| `get_ticks` / `get_current_tick` / 实时订阅 | 明确不支持 | 视接口和权限 | Local 抛 `LocalDataUnsupportedOperationError`，不伪装为实时数据 |
| 行业/概念、基金详情、融资融券、期货、龙虎榜、限售股 | 不支持 | 视接口和权限 | 尚无对应的本地规范数据集 |
| 未显式声明的 SDK 方法 | 不回退 | 可回退到 `jqdatasdk` | Local 禁止隐式联网和不稳定的动态能力 |
| `preflight_backtest` / `capabilities` / `diagnostics` | Local 特有 | 无 | 用于覆盖预检、generation 固定、批量能力及查询统计 |

可用 `get_data_provider().capabilities()` 和 `.diagnostics()` 在回测前检查实际能力、manifest identity、native/fallback、查询次数和缓存统计。

按参数名、顺序、参数类型（位置/关键字）和默认值比较，Local 与 `JQDataProvider` 的 14 个共享公开方法契约一致；Python 类型注解和返回类型可以更具体，但不改变调用方式。需要注意：方法“存在”不代表能力等价，例如 Local 的 tick/订阅方法保留兼容签名，却会明确抛出离线能力错误。

2026-07-19 的真实数据严格对比覆盖了交易日/交易日归属、股票/基金/指数目录与证券信息、三类资产行情、单/多证券三种复权、long/panel 返回形状、`get_bars`、`is_st`、指数成分/权重、估值、利润表和连续估值共 22 组调用。Parquet 与 DuckDB 的值、顺序、索引和 dtype 全部一致；公司行为和完整分钟 generation 在发布前执行同一严格门禁。

## 明确限制

- 价格复权可用；
- 缺少公司行为数据时，不能精确模拟现金分红和送转持仓；预检或查询会明确失败。

期货、期权、tick、行业/概念成分和实时行情不在当前 Parquet 数据集范围内。基本面适配器只执行已声明支持的表达式；无法安全映射的表、字段或过滤操作符会抛出清晰错误，不会忽略条件。

## 设计与扩展

实现分成两层：

- `LocalDataProvider`：稳定的聚宽兼容门面，负责代码、单位、复权和返回形状。
- `LocalDataBackend`：存储无关契约；`ParquetDataBackend` 负责文件发现、谓词下推和 schema 校验。

这个结构借鉴 EasyXT 的数据源抽象与本地批量读取思路，但统一了查询签名、频率和复权语义，并禁止静默网络回退。`ParquetDataBackend` 和 `DuckDBDataBackend` 实现同一契约，策略和 `set_data_provider("local")` 不需要改动。

DuckDB 运行期只读取 manifest 声明的物化表，并复用只读连接。Provider 按资产/频率分组执行参数化 SQL；回测引擎批量化当前快照、订单、持仓估值和公司行为。`BacktestDataSession` 可使用 generation 隔离的 Arrow block 与有界 LRU，超过内存/空闲内存保护时会正确降级并记录原因。

## 故障排查

- `LocalDataMissingShardError`：manifest 没有请求的资产/频率/年份；重建对应 shard，不会回退扫描 Parquet。
- `LocalDataIncompatibleGenerationError`：schema/storage version 不兼容；使用当前代码重新物化，不要手改 manifest 版本。
- 构建 OOM：先减少 `--threads`，再给构建进程合理提高 `--memory-limit`；大量单文件可降低 `--source-file-batch-size`。参考机旧版日线 precedence 构建曾需要 4 线程/20 GB；当前分钟矩阵在固定 12 GB 下以 8 线程最快，正式混合构建建议 8 线程/16 GB，并保留 2 GB 以上给 Python/操作系统。
- Windows 文件占用：不要覆盖已打开数据库；发布新 generation 后让新回测读取新指针，旧进程继续使用旧 shard。
- 性能未达预期：确认 `LOCAL_DATA_QUERY_MODE=batch`、`diagnostics()["fully_materialized"]` 为真，并检查后端查询数。20 证券热调用应接近一次行情 SQL 加一次共享交易日历 SQL。
