## Context

仓库已经形成两层本地数据结构：`LocalDataProvider` 负责聚宽代码格式、单位、复权与返回形状，`LocalDataBackend` 负责存储读取；当前只有 `ParquetDataBackend`。该分层与 EasyXT 的统一数据门面、DuckDB 批量查询思路一致，但本项目还额外要求稳定签名、严格 schema 校验和禁止静默网络回退。

当前接口能力如下。这里的“有限”表示方法存在且在声明范围内可用，不表示覆盖聚宽 SDK 的全部字段或资产类别。

| 接口组 | JQDataProvider | LocalDataProvider 当前状态 |
|---|---|---|
| `auth`、`get_trade_days`、`get_trade_day` | 支持 | 支持；`auth` 执行本地依赖和 schema 校验 |
| `get_price`、`get_bars` | 支持 | 支持股票、ETF、指数日线和 1/5/15/30/60 分钟线，支持前/后/不复权；`prefer_engine`、`force_no_engine` 仅为签名兼容 |
| `get_all_securities`、`get_security_info` | 支持 | 支持股票、基金/ETF/LOF、指数，以本地基础表覆盖范围为准 |
| `get_index_stocks`、`get_index_weights` | 支持 | 支持，以本地历史成分/权重快照为准 |
| `get_fundamentals`、`get_fundamentals_continuously` | SDK 查询能力 | 有限支持估值、利润表、资产负债表、现金流量表和财务指标的已适配字段/操作符，并按公告日做时点过滤 |
| `get_extras` | 支持多类扩展数据 | 仅支持 `is_st` |
| `get_split_dividend` | 支持股票、基金/ETF/LOF及部分货币基金事件 | **当前不可用**；方法固定返回 `[]`，因为本地湖只有 `adj_factor`，没有事件明细 |
| `get_ticks`、`get_current_tick`、扩展的 `get_live_current` | 支持/代理 SDK | 不支持 |
| 行业/概念接口 | 支持 | 不支持 |
| 基金详情、融资融券、期货、龙虎榜、限售股 | 支持 | 不支持 |
| tick/市场订阅与取消订阅 | 继承基类空操作 | 继承基类空操作，不应声明为可用能力 |

`get_split_dividend()` 的既有标准输出是 `security`、`date`、`security_type`、`scale_factor`、`bonus_pre_tax`、`per_base`。单靠 `adj_factor` 只能做价格复权，不能可靠还原现金分红金额、送股与转增构成，因此必须新增事件级数据。

Tushare 可作为显式数据准备来源：股票使用 `dividend`，基金/ETF 使用 `fund_div`。股票需要 `ex_date`、`div_proc`、`cash_div_tax`（缺失时参考 `cash_div`）、`stk_bo_rate`、`stk_co_rate`，并保留 `end_date`、`ann_date`、`imp_ann_date`、`record_date`、`pay_date`、`div_listdate`、`base_date` 和 `base_share` 用于去重与审计。基金至少需要 `ex_date`、`div_proc` 和 `div_cash`，并保留接口返回的公告、登记与支付日期。证券类型必须从本地证券目录解析，不能沿用“代码以 5 开头即基金”的启发式判断。

## Goals / Non-Goals

**Goals:**

- 在不改变策略公共接口的前提下，明确 Local 与 JQData 的能力边界。
- 使 `get_split_dividend()` 基于可审计、可重复导入的本地事件数据工作。
- 支持把当前 Parquet 湖构建为 DuckDB，并通过配置切换后端。
- 保留 Parquet 的开放、便携数据源地位，同时利用 DuckDB 处理重复扫描、跨表关联和时点财务查询。
- 给出可复制的本地回测配置、预检、运行与结果溯源流程。

**Non-Goals:**

- 本变更不追求一次性实现聚宽的 tick、实时订阅、行业概念、期货及全部金融数据接口。
- 不让回测进程按需访问 Tushare/JQData；网络同步是独立、显式的数据准备动作。
- 不把 DuckDB 设计成多用户 OLTP 服务或远程数据库。
- 不保证不同数据供应商在原始数据修订、停牌填充或复权因子定义不同的情况下逐位相同。

## Decisions

### 1. 保持 Provider 门面稳定，能力由显式元数据描述

继续由 `LocalDataProvider` 暴露现有 `DataProvider` 签名，新增只读的结构化 `capabilities()` 信息，并让 `diagnostics()` 汇总后端名称、schema 版本、数据覆盖与能力状态。能力项包含 `status`、支持资产/频率/字段、所需数据集和限制。

不支持的方法改为抛出 `LocalDataQueryError` 的具体子类，而不是固定空值或继承空操作。无匹配记录仍可返回空集合；“没有数据集”和“查询结果为空”必须可区分。

替代方案是让每个后端直接实现完整 `DataProvider`。这会重复代码格式、单位与返回形状逻辑，并使后端切换成为破坏性变更，因此不采用。

### 2. 在后端契约中增加公司行为请求

新增不可变 `CorporateActionRequest`，包含标准证券代码、开始/结束日期和可选资产类型；`LocalDataBackend.read_corporate_actions()` 返回统一 DataFrame。Provider 只负责将其转为既有列表字典口径。

规范表 `corporate_actions` 至少包含：

| 字段 | 语义 |
|---|---|
| `event_id` | 稳定的来源事件标识或规范自然键哈希 |
| `security`、`security_type` | Tushare 格式代码及规范资产类别 |
| `event_date` | 可执行除权除息日，通常为 `ex_date` |
| `scale_factor` | 每份旧持仓对应的新持仓倍数 |
| `bonus_pre_tax`、`per_base` | 税前现金分红及其基数；股票按每 10 股，基金按每 1 份 |
| `announcement_date`、`implementation_date`、`record_date`、`pay_date`、`listing_date` | 时点、安全性和审计信息 |
| `status`、`source`、`source_updated_at`、`payload_hash` | 生效过滤、增量同步和修订追踪 |

股票转换规则为：`per_base=10`，`bonus_pre_tax=cash_per_share*10`，`scale_factor=1+stk_bo_rate+stk_co_rate`；只有合并字段存在而拆分字段均缺失时才使用 `stk_div` 兜底，避免重复计算。基金规则为：`per_base=1`、`bonus_pre_tax=div_cash`，普通现金分红的 `scale_factor=1`；若未来源数据提供基金拆分比例，则单独映射并测试。

导入先写源暂存表，再按来源自然键执行幂等合并。Tushare 没有统一事件 ID 时，使用来源、代码、报告期、公告/实施公告日、除权日组成自然键，以载荷哈希识别修订；同一经济事件多版本只暴露最新有效实施版本。拒绝记录及原因写入导入报告，不吞掉 API 或解析异常。

替代方案是从 `adj_factor` 的跳变反推事件。该方法无法分解现金与送转，也无法满足分红入账，故仅把复权因子作为对账信号，不作为事件来源。

### 3. Parquet 是事实源，DuckDB 是可重建后端

新增 `DuckDBDataBackend`，实现与 Parquet 相同的 `LocalDataBackend` 方法。Provider 以参数绑定 SQL 查询，尽量下推证券、日期和字段过滤；连接以只读方式打开，写入只发生在独立构建命令。

构建器支持两种模式：

- `views`：创建指向 Parquet 的 DuckDB 视图。几乎不复制行数据，保留 Parquet 的分区/共享优势；数据库和 Parquet 必须一起移动或重建视图。
- `materialized`：通过 `CREATE TABLE AS SELECT` 物化规范表。会额外占用近似一份数据空间，但重复关联、排序、窗口计算、财务时点查询和大量小文件场景通常更稳定，也便于事务式增量合并。

数据库包含 `bt_manifest` 和各数据集统计，记录 schema 版本、构建模式、源根目录、文件指纹、行数及日期范围。默认指纹由相对路径、大小、修改时间和 Parquet 元数据组成；需要归档级验证时可启用流式强哈希。构建到目标同目录的临时 `.duckdb`，校验完成后原子替换；Windows 下替换前关闭所有构建连接，失败时保留旧库。

替代方案是放弃 Parquet、直接让下载器写 DuckDB。这样会降低与 Pandas/Arrow/其他工具的互操作性，并使现有数据全部迁移，因此采用“Parquet 事实源 + 可重建 DuckDB”的混合模式。

### 4. 配置兼容和相对路径解析

保留以下配置：

```env
DEFAULT_DATA_PROVIDER=local
LOCAL_DATA_BACKEND=parquet
LOCAL_DATA_PATH=./data/parquet
LOCAL_DATA_STRICT=true
```

DuckDB 使用 `LOCAL_DATA_BACKEND=duckdb`，`LOCAL_DATA_PATH` 指向相对或绝对 `.duckdb` 文件。相对路径继续由统一解析器按项目根目录解析，使用 `pathlib.Path`，不拼接盘符或 POSIX 专用分隔符。程序化配置保持 `set_data_provider("local", backend="duckdb", path="./data/local.duckdb")`。

没有为 DuckDB 新增第二套 Provider 名称，因为后端是存储选择而不是策略数据源选择。兼容别名可继续映射到 `local`。

### 5. 预检先于回测，运行记录绑定数据快照

标准流程为：准备/更新 Parquet → 可选导入公司行为 → 可选构建 DuckDB → 运行本地数据预检 → 启动现有回测 CLI。预检读取策略声明或运行配置所需能力，并验证依赖、schema、日期和证券覆盖；首期无法静态推断的动态调用允许用户显式声明 `required_data_capabilities`。

最小运行示例保持现有 CLI：

```powershell
$env:DEFAULT_DATA_PROVIDER = "local"
$env:LOCAL_DATA_BACKEND = "parquet"
$env:LOCAL_DATA_PATH = ".\data\parquet"
bullet-trade backtest strategies\demo_strategy.py --start 2024-01-01 --end 2024-06-30
```

每次回测报告记录后端、解析后的非敏感路径、manifest/schema 版本和源指纹，使相同策略结果可追溯。Parquet 与 DuckDB 使用同一小型固定数据集执行烟雾测试，并比较订单数、期末持仓、现金和净值关键点。

## Risks / Trade-offs

- [Tushare 权限、限频或历史字段修订导致导入不完整] → 分日期/证券批次同步、记录水位和拒绝项，支持重试并在覆盖不足时阻止依赖该数据的回测。
- [基金类型靠代码推断会误判深市 ETF/LOF] → 以 `fund_basic`/本地证券目录为准，仅在目录缺失时明确报错。
- [公司行为重复或修订造成重复入账] → 来源自然键、载荷哈希、有效状态过滤和重复窗口导入测试共同约束。
- [DuckDB 与 Parquet 返回 dtype 或空值细节不同] → 在后端边界规范化 dtype/排序，并建立同源契约测试。
- [物化 DuckDB 占用额外磁盘且可能过期] → manifest 显示源指纹与构建时间，预检检测过期；允许选择零复制 views 模式。
- [外部视图保存的 Parquet 路径移动后失效] → 预检验证所有目标文件，并提供快速重建视图命令。
- [Windows 上目标数据库正被回测进程占用，无法原子替换] → 构建到新文件并在替换失败时保留两者，提示关闭读连接后重试。
- [EasyXT 式多套连接/表名约定扩散] → 只保留一个后端契约、一个 schema 注册表和一个连接生命周期实现，不复制其硬编码路径或静默空结果模式。

## Migration Plan

1. 先增加能力元数据、类型化错误与公司行为后端契约，保持 Parquet 现有查询通过。
2. 实现规范公司行为 schema、Parquet 读取和 Tushare 显式导入；将固定空列表替换为真实查询或缺数据错误。
3. 实现 DuckDB 构建器、manifest 与后端，并用同一 fixture 做 Parquet/DuckDB 契约测试。
4. 增加预检、CLI 文档和双后端最小回测，更新示例环境文件。
5. 默认仍为 Parquet；用户只有在显式构建数据库并设置 `LOCAL_DATA_BACKEND=duckdb` 后才迁移。

回滚时把 `LOCAL_DATA_BACKEND` 改回 `parquet` 即可；Parquet 源从未被构建器修改。若公司行为导入异常，可移走新增数据集并让预检明确阻止相关策略，不影响不依赖该接口的历史行情回测。

## Open Questions

- 首版 Tushare `fund_div` 是否覆盖所有目标 ETF/LOF，还是需要增加交易所或其他供应商补充源？
- materialized 模式首版是否实现增量 `MERGE`，还是先采用更简单可靠的全量原子重建？建议首版全量重建，数据规模确认后再增加增量刷新。
- 能力需求由策略显式声明还是由回测加载阶段自动采集？建议先支持显式声明与运行时错误，后续再做静态分析。
