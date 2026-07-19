## Context

仓库已经形成两层本地数据结构：`LocalDataProvider` 负责聚宽代码格式、单位、复权、停牌填充与返回形状，`LocalDataBackend` 负责存储读取；当前只有 `ParquetDataBackend`。该分层可以容纳 DuckDB，但现有 `BarRequest` 和 Provider 实现以单证券为中心，多证券 `get_price()` 会逐证券读取。

参考数据根约 145.2 GiB、67,106 个 Parquet 文件，其中股票、ETF、指数行情约占 139.5 GiB。当前参考机上，单股票一年 6 字段热查询约 0.084 秒，20 股票一年 2 字段热查询约 1.538 秒，显示逐证券调度接近线性放大。回测引擎还会逐持仓更新收盘价、逐订单解析执行价、逐持仓查询公司行为；`BacktestDataSession` 已有行情块缓存，但默认关闭且主要覆盖单证券 `count` 窗口。因此，只把 Parquet 换成 DuckDB 不能达到端到端最大提速，必须同时批量化后端和引擎热路径。

参考机有 10 核/16 线程、31.8 GiB 内存。仓库所在 D 盘当前剩余空间不足以安全保留完整 Parquet、两代物化库和构建临时文件，但其他数据盘空间充足。数据库根、临时目录、线程和内存都必须由配置提供，设计不得硬编码盘符。

`/tool/格式转换器最新.exe` 是未签名、无版本元数据的 PyInstaller/Tkinter 程序，内含 PyArrow 与 DuckDB 1.5.3。没有源码或自动化接口可以证明其规范表、去重、物理排序、原子发布和校验行为，因此只能作为隔离小样本试验对象，不能成为生产构建链的必要依赖。

## Goals / Non-Goals

**Goals:**

- 不改变策略层 `get_price()`、`history()`、`get_current_data()`、财务和公司行为接口。
- 为高性能本地回测提供所有运行期数据均来自 DuckDB 物化表的模式，不在查询期读取或回退到 Parquet。
- 使用非破坏性批量后端契约、引擎批量访问和 Arrow 数据会话预加载消除 N+1 查询。
- 将 67,000 多个源文件合并为有界的规范表，并按领域、资产、频率和时间生成不可变分片。
- 用版本化 manifest 原子发布新 generation，保证 Windows 文件锁环境下的并发只读、回滚和结果复现。
- 先证明 Parquet、DuckDB 标量路径和 DuckDB 批量路径语义等价，再以查询层和端到端基准验收性能。

**Non-Goals:**

- 不移除 Parquet 后端；它继续作为事实源、正确性基线和显式回滚路径。
- 不让回测进程按需访问 Tushare/JQData，也不允许 DuckDB 模式静默读取 Parquet。
- 不把 DuckDB 设计成多用户 OLTP 服务或在活动回测期间原地更新其已打开的数据文件。
- 不一次性补齐聚宽 tick、实时订阅、行业概念、期货及全部金融数据接口。
- 不以牺牲未来数据隔离、复权、停牌、单位、顺序或组合结果换取速度。

## Decisions

### 1. 稳定 Provider 门面，显式报告能力和查询路径

`LocalDataProvider` 继续暴露现有 `DataProvider` 签名，负责所有聚宽兼容语义。新增结构化 `capabilities()`，并让 `diagnostics()` 汇总后端、schema/manifest generation、覆盖范围、批量能力、实际查询模式和统计。

不支持的方法抛出类型化本地数据错误。“数据集缺失”“分片覆盖不足”和“查询结果为空”必须可区分；DuckDB 物化模式缺分片时不得回退到 Parquet。

替代方案是让 DuckDB 直接实现完整 Provider。该方案会复制代码格式、复权和返回形状逻辑，使后端切换成为破坏性变更，因此不采用。

### 2. 以带默认回退的批量契约扩展 LocalDataBackend

新增同质批量请求模型，至少包含证券集合、资产类型、频率、开始/结束时间、字段和可选 per-security count。后端返回按 `time, code` 排序的规范长表。批量能力覆盖：

- `read_bars_batch`
- `read_current_bars`
- `read_reference_factors_batch`
- `read_security_info_batch`
- `read_corporate_actions_batch`

这些方法在 `LocalDataBackend` 上是非抽象方法，默认正确地循环现有标量方法。`DuckDBDataBackend` 声明 `native` 并用有界数量的参数化 SQL 覆盖；`ParquetDataBackend` 可以先使用 fallback，之后独立优化。Provider 按资产/频率/公共参数分组，并在 Provider 边界一次性完成代码、单位、复权、停牌和 panel/长表转换。

`count=N` 不允许退化为无界全表读取。日线先通过交易日历计算下界；其他场景使用按证券分区的窗口函数或有界时间范围。大证券集合通过列表参数、Arrow 临时关系或等价安全方式传入，不能拼接未经处理的 SQL。

此设计保持旧后端可用，同时提供显式标量模式用于 A/B、诊断和回滚。

### 3. 公司行为采用规范事件表与批量日历

新增 `CorporateActionRequest` 和规范表 `corporate_actions`。最少字段为 `event_id`、证券、类别、生效日、`scale_factor`、`bonus_pre_tax`、`per_base`、报告期结束日、公告/实施/登记/支付/上市日期、状态、来源、更新时间和载荷哈希。Tushare 没有统一事件主键时，稳定身份由来源、证券、报告期和执行日期组成；公告日和状态文本不参与身份，避免同一方案的修订公告重复入账，同时允许不同报告期的方案共用一个除权日。

Tushare 股票使用 `dividend`，基金/ETF 使用 `fund_div`。股票输出按每 10 股，基金按每 1 份；证券类型来自本地目录而不是代码前缀。导入先暂存再幂等合并，取消或未实施记录不成为可执行事件，异常和拒绝行必须报告。

公共 `get_split_dividend()` 保持单证券签名；引擎每天对全部持仓执行一次批量事件查询，并按 manifest generation 缓存事件日历。`adj_factor` 只用于复权对账，不能代替事件数据。

### 4. 高性能模式全量物化，但采用不可变分片而不是单体库

高性能运行期的所有已支持数据均在 DuckDB 物理表中，Parquet 仅作为构建输入。建议 generation 包含：

| 分片 | 主要内容 | 建议物理顺序 |
|---|---|---|
| `meta_<id>.duckdb` | 证券、交易日、数据清单、指数元数据 | 业务主键 |
| `daily_<id>.duckdb` | 股票、ETF、指数日线 | `trade_date, ts_code` |
| `finance_<id>.duckdb` | 估值、利润表、资产负债表、现金流和指标 | `ts_code, ann_date, end_date` |
| `actions_<id>.duckdb` | 公司行为 | `event_date, security` |
| `minute_<asset>_<freq>_<period>_<id>.duckdb` | 分钟行情 | `ts_code, trade_time`，按年份或更小时间范围分片 |

分片是逻辑建议而非硬编码文件名；实际表名和 sort key 来自唯一 schema 注册表。日线按日期优先，适合逐日回测、截面查询和通过日历下界实现的 count 窗口。分钟数据按时间分片并在片内按证券/时间组织，避免一次构建或打开全部 139+ GiB 行情。若基准证明单一物理顺序不能满足关键 workload，可以只为较小的日线热点表增加第二种有序副本；不得未经基准复制所有分钟数据。

DuckDB 自动 zonemap 的效果依赖数据有序性，构建器必须记录物理排序并用 `EXPLAIN ANALYZE` 验证裁剪。对高度选择性的点查询，可在基准证明收益高于加载/空间成本后增加 ART 索引；不能默认依赖 ART 改善 JOIN 或聚合。

单个回测进程为所需 generation 分片建立长期只读连接或连接集合，复用 catalog 和 buffer cache。多进程只能读同一 generation；写入只发生在独立构建进程。

### 5. 版本化 manifest 是发布和复现边界

构建器写入新的不可变分片和 generation manifest，包含 schema/storage 兼容版本、源指纹、分片标识、行数、日期范围、排序键、重复键、空值/类型校验和构建参数。所有分片通过后，原子更新一个很小的 active manifest 指针。

活动回测在启动时固定 manifest identity 和分片集合，直到结束不追随新指针。Windows 下不替换正在打开的数据库文件；旧 generation 在没有活动引用且过了保留期后才由显式清理流程回收。发布失败只留下未引用 staging，不影响当前 generation。

替代方案是原地 `MERGE` 一个巨型数据库。它简化文件管理，但会扩大锁、回滚、刷新和复现风险，因此不作为首版生产发布模型。增量刷新通过只重建受影响的年度/领域分片实现。

### 6. 仓库内构建器是权威转换路径

正式 CLI 直接使用 DuckDB 批量 `read_parquet`/CTAS/COPY，复用 schema 注册表和验证器。它必须：

- 将每证券 Parquet 合并到有界的规范表，而不是一文件一表。
- 对 `stock_daily.parquet` 与 `daily_adj_*.parquet` 执行“现代数据优先、旧文件只补早期缺口”的确定性规则。
- 在 staging 中构建并排序，配置线程、内存、临时目录和临时空间上限。
- 支持领域/时间分片重建、dry-run、resume 或明确的幂等重跑。
- 生成 manifest、审计报告和可机器读取的失败结果。

`格式转换器最新.exe` 可以在复制的小样本上评估：检查输出表数量、schema、去重、排序、行数、日期覆盖、自动化能力和 DuckDB 版本。即使通过，也只能作为可选导入加速器；输出仍须进入权威校验/发布流程。没有源码、签名和非交互契约时不把它纳入必需依赖。

### 7. 回测引擎批量化并优先复用当前快照

数据库批量接口必须被引擎实际使用：

- `_update_positions()` 一次读取全部持仓收盘价。
- `_process_orders()` 复用本 bar 已加载的 `current_data`，不为每张订单重复读取执行价；只有不同语义确有需要时再批量补取。
- `_apply_dividends_for_day()` 一次读取全部持仓的事件窗口。
- `BacktestCurrentData` 提供内部 `preload(securities)`，并把同一时间点的行情快照共享给策略、撮合和估值。
- 多证券 `history/get_price` 直接进入 Provider 批量路径。

所有改造都是内部实现，策略事件顺序和公共返回结构保持不变。批量错误必须能定位到具体分组/证券；不能因一个缺失证券静默丢弃整个结果。

### 8. BacktestDataSession 使用 generation 隔离的 Arrow 块

Local DuckDB 回测可以在启动或首次访问时预取回测区间、证券集合、字段和频率匹配的 Arrow blocks。日线策略优先批量加载完整回测区间或大块；分钟策略按日期/证券池分块并用有界 LRU。历史窗口从块中切片，不重复执行 SQL。

缓存键包含 manifest identity、证券集合/证券、频率、字段、复权参数、停牌语义和时间范围。当前行情、参考因子、财务和公司行为缓存同样隔离 generation。默认 512 MiB 的现有预算不足以代表高性能模式，因此通过配置提高，但必须保留最大内存和最小空闲内存硬保护。

优先让 DuckDB 返回 Arrow Reader/Table，在确实需要公共 pandas 结果时才转换，避免 DuckDB→Pandas→再次 pivot 的重复复制。超出预算时降级为正确的批量 SQL 或标量路径并记录原因，不能改变结果。

### 9. 配置资源而不硬编码机器路径

保留兼容配置：

```env
DEFAULT_DATA_PROVIDER=local
LOCAL_DATA_BACKEND=parquet
LOCAL_DATA_PATH=./data/parquet
LOCAL_DATA_STRICT=true
```

高性能模式显式选择 DuckDB manifest，例如：

```env
LOCAL_DATA_BACKEND=duckdb
LOCAL_DATA_PATH=./data/duckdb/current-manifest.json
LOCAL_DATA_QUERY_MODE=batch
```

数据库根、临时目录、线程、内存和最大临时空间通过环境或程序化配置提供，并用 `pathlib.Path` 解析。外置大容量盘可以通过用户配置绝对路径或项目下的受管目录联接使用；代码和默认值不写死盘符。

参考机初始基准建议单回测使用 8–12 线程、约 12–16 GiB DuckDB memory limit，为 Python/Pandas 和操作系统留空间；构建进程可单独提高资源。多个并行回测必须按并发数划分线程和内存，不能每个进程占满主机。

### 10. 正确性门禁先于性能门禁

建立三路同源对照：Parquet 标量、DuckDB 标量、DuckDB native batch。契约测试覆盖代码/单位/dtype/排序、空值、停牌填充、前后复权和动态参考日、count、未来数据隔离、财务 PIT、指数成分和公司行为。端到端比较订单、成交、现金、持仓、分红和关键净值点。

性能基准同时记录冷/热延迟、后端查询次数、读取行数、峰值内存、临时磁盘、构建时间和整场回测耗时。参考目标为：20 股票一年两字段相对当前标量 Parquet 至少 5 倍查询提速；代表性多证券日线策略至少 3 倍端到端提速。目标未达到要报告瓶颈，不能为达标放宽语义。

## Risks / Trade-offs

- [仅换数据库但仍逐证券/逐 bar 查询，端到端收益有限] → 批量后端、引擎热路径和数据会话作为同一交付门禁。
- [全量物化、两代 generation 和 spill 超过仓库盘容量] → 预检磁盘预算，数据库/临时目录可配置到独立大容量盘，按分片发布。
- [一个物理排序无法同时优化截面与单证券历史] → 以真实 workload 选择排序；先利用日期下界和分片，只对经基准证明的热点增加小范围副本/索引。
- [32 GiB 内存被 DuckDB、Pandas 和预加载共同耗尽] → 限制 DuckDB memory、Arrow 块预算和最小空闲内存；分钟数据分块 LRU。
- [Windows 活动文件无法替换] → 不可变文件名和原子 manifest 指针，不原地替换已打开分片。
- [DuckDB/Pandas dtype 或空值细节造成策略差异] → 三路 golden/contract 测试并在 Provider 边界规范化。
- [公司行为重复或修订造成重复入账] → 稳定自然键、载荷哈希、状态过滤、generation 隔离和重复导入测试。
- [外部转换器输出不可审计或版本不兼容] → 只在隔离样本评估；仓库构建器与验证器始终权威。
- [多进程回测各自占满线程/内存导致负加速] → 显式每进程资源预算并纳入运行元数据和并发基准。
- [分片数量过多增加 attach/catalog 开销] → schema 注册表限制粒度，分钟按资产/频率/年度起步，基于实测再调整。

## Migration Plan

1. 冻结现有 Parquet Provider 结果为 golden fixtures 和端到端基线，补齐查询计数与耗时观测。
2. 增加非抽象批量后端方法及标量默认回退，证明现有 Parquet 行为不变。
3. 实现公司行为规范表、Tushare 显式导入和批量事件查询。
4. 实现 schema 注册表、权威 DuckDB 分片构建器、generation manifest、预检与小样本转换；独立评估外部 EXE。
5. 实现只读 `DuckDBDataBackend` 的标量和 native batch 路径，完成 Parquet/DuckDB/批量三路契约测试。
6. 批量化当前行情、持仓估值、订单执行价和公司行为，并扩展 generation 隔离的 Arrow 数据会话。
7. 先物化日线、元数据、财务和公司行为并跑端到端基准，再按频率/年度逐步物化全部分钟数据。
8. 满足正确性和性能门禁后，文档将全量物化 batch 模式设为高性能推荐；默认 Parquet 配置仍向后兼容。

回滚只需让新回测显式选择旧 manifest、DuckDB scalar 模式或 Parquet 后端；已经启动的回测始终固定原 generation。Parquet 源数据不被构建器修改。

## Open Questions

- 分钟分片首版按年度是否足够，还是 1m 股票数据需要按季度/月度以缩短重建和 attach 范围？通过 pilot benchmark 决定。
- 日线是否需要第二份按 `ts_code, trade_date` 排序的热点副本？先比较日期优先表、ART 索引和 Arrow 预加载再决定。
- `BacktestCurrentData.preload()` 的证券集合由当前持仓、待处理订单和策略 universe 合并即可，还是需要策略显式声明动态 universe 上限？
- 外部格式转换器是否能提供源码、CLI 文档或可复现的样本输出？在此之前保持非依赖状态。
