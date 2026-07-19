## Why

`LocalDataProvider` 已能用本地 Parquet 完成核心行情与财务查询，但当前缺少明确的聚宽接口兼容边界、可用的除权分红事件数据，以及面向大规模重复回测的高性能执行路径。当前逐证券、逐交易日的小查询会让多证券回测近似线性变慢，因此需要在保持策略 API 稳定的前提下，以全量 DuckDB 物化、后端批量查询和回测区间预加载共同消除 I/O 与 Python 调度瓶颈。

## What Changes

- 建立 `LocalDataProvider` 与聚宽 `DataProvider` 的稳定能力契约，区分完整支持、有限支持和明确不支持的接口，并提供可诊断的错误信息。
- 为本地数据层增加标准化公司行为数据模型，使 `get_split_dividend()` 能从本地数据返回股票、ETF/基金的除权、送转与现金分红事件。
- 增加显式的 Tushare 公司行为同步/转换流程，将 `dividend`、`fund_div` 等源数据规范化后写入本地数据集；回测查询阶段仍保持完全离线，不静默访问网络。
- 增加实现同一 `LocalDataBackend` 契约的 DuckDB 后端，以及从现有 Parquet 构建 DuckDB 的可审查工具；高性能运行形态将所有已支持数据物化为按领域、资产、频率和时间分片的版本化数据库，外部 Parquet 视图仅作为诊断/迁移能力。
- 为 `LocalDataBackend` 增加带默认逐条回退的非破坏性批量查询契约；DuckDB 使用单次批量 SQL，既有后端无需立即实现即可保持兼容。
- 批量化回测引擎的持仓估值、订单执行价、当前行情与公司行为读取，并增强 `BacktestDataSession` 的多证券 Arrow 预加载和有界块缓存，避免每个 bar 重复访问数据库。
- 完善使用 `LocalDataProvider` 进行回测的配置、资源预检、版本化 manifest、示例、性能基准和最小烟雾测试，并保证策略层接口不因后端或查询模式切换而改变。
- 对 `/tool/格式转换器最新.exe` 只进行受控小样本评估；正式构建流程必须版本可控、可自动化、可校验，不能依赖不透明 GUI 二进制。
- 不移除或重命名现有 Provider 公共接口；现有 `provider=local`、相对 `LOCAL_DATA_PATH` 和 Parquet 行为保持向后兼容。

## Capabilities

### New Capabilities

- `local-provider-capability-contract`: 定义 Local Provider 对聚宽接口的兼容等级、稳定返回语义、能力查询、非破坏性批量后端扩展和不支持接口的失败方式。
- `local-corporate-actions`: 定义本地除权分红规范数据、Tushare 导入规则以及 `get_split_dividend()` 的离线查询行为。
- `local-duckdb-backend`: 定义全量物化、分片与版本切换、Parquet 到 DuckDB 的可重复构建、批量查询、资源配置、校验和一致性要求。
- `local-backtest-workflow`: 定义使用本地 Provider 启动回测、执行数据预检、多证券预加载、切换查询模式和验证正确性/性能的用户流程。

### Modified Capabilities

无；仓库当前没有已发布的 OpenSpec 主规格。

## Impact

- 影响 `bullet_trade/data/providers/local.py`、`bullet_trade/data/local/`、`bullet_trade/data/backtest_session.py`、`bullet_trade/data/api.py`、回测引擎内部行情访问、Provider 工厂与环境配置加载逻辑。
- 新增公司行为数据同步/转换与 Parquet→版本化 DuckDB 分片构建命令，并增加 DuckDB Python 可选依赖。
- 扩展 `/data/parquet` 数据约定、文档、单元测试和本地回测集成测试。
- 高性能全量构建需要独立可配置的数据库与临时目录，并在预检阶段验证可用磁盘、内存、线程数和分片覆盖；任何路径都不得在代码中硬编码盘符。
- `JQDataProvider` 与 `TushareProvider` 的现有远程查询行为不变；新增流程只读取用户显式提供的账号配置进行数据准备。
