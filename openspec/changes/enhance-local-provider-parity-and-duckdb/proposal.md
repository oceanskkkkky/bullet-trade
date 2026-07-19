## Why

`LocalDataProvider` 已能用本地 Parquet 完成核心行情与财务查询，但当前缺少明确的聚宽接口兼容边界、可用的除权分红事件数据，以及可选的 DuckDB 后端。这使用户难以判断策略能否离线运行，也无法为依赖 `get_split_dividend()` 的撮合/复权逻辑准备完整数据。

## What Changes

- 建立 `LocalDataProvider` 与聚宽 `DataProvider` 的稳定能力契约，区分完整支持、有限支持和明确不支持的接口，并提供可诊断的错误信息。
- 为本地数据层增加标准化公司行为数据模型，使 `get_split_dividend()` 能从本地数据返回股票、ETF/基金的除权、送转与现金分红事件。
- 增加显式的 Tushare 公司行为同步/转换流程，将 `dividend`、`fund_div` 等源数据规范化后写入本地数据集；回测查询阶段仍保持完全离线，不静默访问网络。
- 增加实现同一 `LocalDataBackend` 契约的 DuckDB 后端，以及从现有 Parquet 构建 DuckDB 数据库的工具；支持外部 Parquet 视图和物化表两种模式。
- 完善使用 `LocalDataProvider` 进行回测的配置、数据预检、示例和最小烟雾测试，并保证策略层接口不因后端切换而改变。
- 不移除或重命名现有 Provider 公共接口；现有 `provider=local`、相对 `LOCAL_DATA_PATH` 和 Parquet 行为保持向后兼容。

## Capabilities

### New Capabilities

- `local-provider-capability-contract`: 定义 Local Provider 对聚宽接口的兼容等级、稳定返回语义、能力查询和不支持接口的失败方式。
- `local-corporate-actions`: 定义本地除权分红规范数据、Tushare 导入规则以及 `get_split_dividend()` 的离线查询行为。
- `local-duckdb-backend`: 定义 Parquet 到 DuckDB 的构建模式、存储后端契约、配置、校验和一致性要求。
- `local-backtest-workflow`: 定义使用本地 Provider 启动回测、执行数据预检、切换后端和验证最小回测的用户流程。

### Modified Capabilities

无；仓库当前没有已发布的 OpenSpec 主规格。

## Impact

- 影响 `bullet_trade/data/providers/local.py`、`bullet_trade/data/local/`、Provider 工厂与环境配置加载逻辑。
- 新增公司行为数据同步/转换与 Parquet→DuckDB 构建命令，并增加 DuckDB Python 依赖或可选依赖。
- 扩展 `/data/parquet` 数据约定、文档、单元测试和本地回测集成测试。
- `JQDataProvider` 与 `TushareProvider` 的现有远程查询行为不变；新增流程只读取用户显式提供的账号配置进行数据准备。
