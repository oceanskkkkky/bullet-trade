#!/usr/bin/env python
"""
BulletTrade 命令行主入口

用法示例:
    bullet-trade backtest strategy.py --start 2023-01-01 --end 2023-12-31 --output backtest_results/demo --auto-report
    bullet-trade optimize strategy.py --params params.json --start 2023-01-01 --end 2023-12-31 --output optimization.csv --processes 4
    bullet-trade live strategy.py --broker qmt --runtime-dir runtime/live --log-dir logs/live
    bullet-trade report --input backtest_results/demo --format html --output reports/demo.html
    bullet-trade server --server-type qmt --listen 0.0.0.0 --port 8080
    bullet-trade lab --notebook-dir notebooks --no-browser --port 8088
    bullet-trade --env-file .env.dev backtest strategy.py --start 2023-01-01 --end 2023-12-31
"""

import argparse
import os
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Dict, Optional

if TYPE_CHECKING:
    from bullet_trade.core.globals import Logger


def apply_cli_overrides(args, logger: Optional["Logger"] = None) -> Dict[str, str]:
    """
    应用 CLI 覆写逻辑，返回可传入 LiveConfig 的额外配置。
    """
    overrides: Dict[str, str] = {}

    log_dir = getattr(args, "log_dir", None)
    if log_dir:
        resolved = str(Path(log_dir).expanduser().resolve())
        os.environ["LOG_DIR"] = resolved
        if logger is None:
            from bullet_trade.core.globals import log as global_log

            logger = global_log
        logger.configure_file_logging(log_dir=resolved)

    runtime_dir = getattr(args, "runtime_dir", None)
    if runtime_dir:
        resolved = str(Path(runtime_dir).expanduser().resolve())
        os.environ["RUNTIME_DIR"] = resolved
        overrides["runtime_dir"] = resolved

    return overrides


def _refresh_env_dependents() -> None:
    """
    刷新依赖环境变量的单例对象。
    """
    try:
        from bullet_trade.data.api import reload_data_provider_from_env

        reload_data_provider_from_env()
    except Exception:
        pass
    try:
        from bullet_trade.core.globals import log

        log.reload_from_env()
    except Exception:
        pass


def create_parser():
    """创建命令行参数解析器"""
    parser = argparse.ArgumentParser(
        prog="bullet-trade",
        description="BulletTrade - 专业的量化交易系统",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  # 运行回测
  bullet-trade backtest strategy.py --start 2023-01-01 --end 2023-12-31 --output backtest_results/demo

  # 实盘
  bullet-trade live strategy.py --broker qmt --runtime-dir runtime/live --log-dir logs/live

  # 服务端
  bullet-trade server --server-type qmt --listen 0.0.0.0 --port 8080

  # 研究环境 (JupyterLab)
  bullet-trade lab

  # 切换 env 文件
  bullet-trade --env-file .env.dev backtest strategy.py --start 2023-01-01 --end 2023-12-31

  # 参数优化
  bullet-trade optimize strategy.py --params params.json --start 2023-01-01 --end 2023-12-31 --output optimization.csv --processes 4

  更多信息请访问: https://github.com/BulletTrade/bullet-trade
               https://bullettrade.cn/
        """,
    )

    # 从单一来源读取版本号
    from bullet_trade.__version__ import __version__

    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")

    # 全局 env 文件参数（对所有子命令生效）
    parser.add_argument(
        "--env-file", dest="env_file", default=None, help="指定要加载的 .env 文件（优先于默认搜索）"
    )

    subparsers = parser.add_subparsers(
        title="commands", description="可用命令", dest="command", help="命令帮助"
    )

    # backtest 命令
    backtest_parser = subparsers.add_parser("backtest", help="运行策略回测")
    backtest_parser.add_argument("strategy_file", type=str, help="策略文件路径")
    backtest_parser.add_argument(
        "--start", type=str, required=True, help="回测开始日期 (YYYY-MM-DD)"
    )
    backtest_parser.add_argument("--end", type=str, required=True, help="回测结束日期 (YYYY-MM-DD)")
    backtest_parser.add_argument(
        "--cash", type=float, default=100000, help="初始资金 (默认: 100000)"
    )
    backtest_parser.add_argument(
        "--frequency",
        type=str,
        choices=["day", "minute"],
        default="day",
        help="回测频率: day=日线回测, minute=分钟回测 (默认: day)",
    )
    backtest_parser.add_argument(
        "--benchmark", type=str, default=None, help="基准指数代码 (如: 000300.XSHG)"
    )
    backtest_parser.add_argument(
        "--output",
        type=str,
        default="./backtest_results",
        help="输出目录 (默认: ./backtest_results)",
    )
    backtest_parser.add_argument("--log", type=str, default=None, help="日志文件路径")
    backtest_parser.add_argument(
        "--images",
        dest="generate_images",
        action="store_true",
        help="生成PNG图表输出 (默认关闭，以提升速度；需安装 bullet-trade[report])",
    )
    backtest_parser.add_argument(
        "--no-csv", dest="generate_csv", action="store_false", help="不导出CSV明细 (默认导出)"
    )
    backtest_parser.add_argument(
        "--no-html",
        dest="generate_html",
        action="store_false",
        help="不生成交互式HTML报告 (默认生成)",
    )
    backtest_parser.add_argument(
        "--no-logs",
        dest="generate_logs",
        action="store_false",
        help="仅在终端打印日志，不写日志文件 (默认写入输出目录)",
    )
    backtest_parser.add_argument(
        "--backtest-data-session",
        dest="backtest_data_session",
        action="store_true",
        help="启用回测数据会话优化（默认关闭，仅影响回测）",
    )
    backtest_parser.add_argument(
        "--backtest-data-session-manifest",
        dest="backtest_data_session_manifest",
        type=str,
        default=None,
        help="回测数据会话 manifest 输出路径（默认写入输出目录）",
    )
    backtest_parser.add_argument(
        "--backtest-data-session-max-bytes",
        dest="backtest_data_session_max_bytes",
        type=int,
        default=None,
        help="回测数据会话内存缓存硬上限字节数",
    )
    backtest_parser.add_argument(
        "--backtest-price-block-cache",
        dest="backtest_price_block_cache",
        action="store_true",
        help="启用回测内存行情块缓存（默认关闭；动态前复权会自动降级）",
    )
    backtest_parser.set_defaults(
        generate_images=False,
        generate_csv=True,
        generate_html=True,
        generate_logs=True,
        auto_report=False,
        report_format="html",
        report_template=None,
        report_metrics=None,
        report_title=None,
        backtest_data_session=False,
        backtest_price_block_cache=False,
    )
    backtest_parser.add_argument(
        "--auto-report", action="store_true", help="回测完成后自动生成标准化报告"
    )
    backtest_parser.add_argument(
        "--report-format", choices=["html", "pdf"], default="html", help="自动报告格式 (默认: html)"
    )
    backtest_parser.add_argument(
        "--report-template", type=str, default=None, help="自定义报告模板路径"
    )
    backtest_parser.add_argument(
        "--report-metrics", type=str, default=None, help="报告中展示的指标名称列表（逗号分隔）"
    )
    backtest_parser.add_argument(
        "--report-title", type=str, default=None, help="报告标题（默认使用输出目录名称）"
    )
    backtest_parser.add_argument(
        "--report-output",
        type=str,
        default=None,
        help="自动报告输出路径（默认写入 <output>/standard_report.<format>）",
    )

    # optimize 命令
    optimize_parser = subparsers.add_parser("optimize", help="参数优化")
    optimize_parser.add_argument("strategy_file", type=str, help="策略文件路径")
    optimize_parser.add_argument("--params", type=str, required=True, help="参数配置文件 (JSON)")
    optimize_parser.add_argument("--start", type=str, required=True, help="回测开始日期")
    optimize_parser.add_argument("--end", type=str, required=True, help="回测结束日期")
    optimize_parser.add_argument(
        "--processes", type=int, default=None, help="并行进程数 (默认: CPU核心数)"
    )
    optimize_parser.add_argument(
        "--output", type=str, default="./optimization_results.csv", help="输出CSV文件路径"
    )

    # live 命令（待实现）
    live_parser = subparsers.add_parser("live", help="实盘交易")
    live_parser.add_argument("strategy_file", type=str, help="策略文件路径")
    live_parser.add_argument(
        "--broker",
        type=str,
        choices=["qmt", "qmt-remote", "simulator"],
        default=None,
        help="券商类型（默认读取 DEFAULT_BROKER）",
    )
    live_parser.add_argument("--log-dir", type=str, default=None, help="覆盖 LOG_DIR，优先于 .env")
    live_parser.add_argument(
        "--runtime-dir", type=str, default=None, help="覆盖 RUNTIME_DIR，优先于 .env"
    )

    # report 命令
    report_parser = subparsers.add_parser("report", help="根据回测结果目录生成标准化报告")
    report_parser.add_argument("--input", "-i", required=True, help="回测结果目录")
    report_parser.add_argument(
        "--output", "-o", type=str, default=None, help="输出文件路径（默认写入回测目录）"
    )
    report_parser.add_argument(
        "--format", "-f", choices=["html", "pdf"], default="html", help="报告格式 (默认: html)"
    )
    report_parser.add_argument("--template", type=str, default=None, help="自定义HTML模板文件路径")
    report_parser.add_argument(
        "--metrics", type=str, default=None, help="展示的指标名称，使用逗号分隔"
    )
    report_parser.add_argument("--title", type=str, default=None, help="报告标题")

    # server 命令
    server_parser = subparsers.add_parser("server", help="启动远程数据/券商服务")
    server_parser.add_argument(
        "--server-type",
        dest="server_type",
        default="qmt",
        help="服务类型（默认 qmt；大 QMT 用 big_qmt 或 big-qmt）",
    )
    server_parser.add_argument(
        "--listen", dest="listen", default=None, help="监听地址（覆盖 QMT_SERVER_LISTEN）"
    )
    server_parser.add_argument(
        "--port", dest="port", type=int, default=None, help="监听端口（覆盖 QMT_SERVER_PORT）"
    )
    server_parser.add_argument(
        "--token", dest="token", default=None, help="访问 token（覆盖 QMT_SERVER_TOKEN）"
    )
    server_parser.add_argument(
        "--tls-cert", dest="tls_cert", default=None, help="TLS 证书路径（可选）"
    )
    server_parser.add_argument(
        "--tls-key", dest="tls_key", default=None, help="TLS 私钥路径（可选）"
    )
    server_parser.add_argument(
        "--enable-data",
        dest="enable_data",
        action="store_true",
        default=None,
        help="强制启用数据服务",
    )
    server_parser.add_argument(
        "--disable-data", dest="enable_data", action="store_false", help="强制禁用数据服务"
    )
    server_parser.add_argument(
        "--enable-broker",
        dest="enable_broker",
        action="store_true",
        default=None,
        help="强制启用券商服务",
    )
    server_parser.add_argument(
        "--disable-broker", dest="enable_broker", action="store_false", help="强制禁用券商服务"
    )
    server_parser.add_argument(
        "--allowlist", dest="allowlist", default=None, help="允许访问的 IP 列表（逗号分隔）"
    )
    server_parser.add_argument(
        "--max-connections", dest="max_connections", type=int, default=None, help="最大并发连接数"
    )
    server_parser.add_argument(
        "--max-subscriptions",
        dest="max_subscriptions",
        type=int,
        default=None,
        help="单会话 tick 订阅上限",
    )
    server_parser.add_argument(
        "--accounts",
        dest="accounts",
        default=None,
        help="多账户配置（例: main=123456:stock,hedge=654321:future）",
    )
    server_parser.add_argument(
        "--sub-accounts",
        dest="sub_accounts",
        default=None,
        help="虚拟子账户配置（例: demo@main:limit=1000000,test@hedge）",
    )
    server_parser.add_argument(
        "--log-file",
        dest="log_file",
        default=None,
        help="将服务器日志写入指定文件（默认仅输出到控制台）",
    )
    server_parser.add_argument(
        "--log-account-overview",
        dest="log_account_snapshot",
        action="store_true",
        default=None,
        help="允许 admin.print_account 将账户快照打印到日志",
    )
    server_parser.add_argument(
        "--no-log-account-overview",
        dest="log_account_snapshot",
        action="store_false",
        help="禁止账户快照打印到日志",
    )
    server_parser.add_argument(
        "--access-log",
        dest="access_log",
        action="store_true",
        default=None,
        help="显式开启请求访问日志",
    )
    server_parser.add_argument(
        "--no-access-log", dest="access_log", action="store_false", help="关闭请求访问日志"
    )

    data_parser = subparsers.add_parser("data", help="准备和验证本地回测数据")
    data_subparsers = data_parser.add_subparsers(dest="data_command", required=True)
    actions_parser = data_subparsers.add_parser(
        "import-corporate-actions", help="从 Tushare 显式导入规范公司行为数据"
    )
    actions_parser.add_argument("--root", default="./data/parquet", help="Parquet 数据根目录")
    actions_parser.add_argument("--output", default=None, help="输出 Parquet 文件")
    actions_parser.add_argument("--start", default=None, help="除权日开始日期")
    actions_parser.add_argument("--end", default=None, help="除权日结束日期")
    actions_parser.add_argument("--securities", default=None, help="逗号分隔的 Tushare 证券代码")
    actions_parser.add_argument(
        "--token", default=None, help="Tushare token；默认读取 TUSHARE_TOKEN"
    )
    actions_parser.add_argument(
        "--requests-per-minute", type=int, default=120, help="请求速率上限（默认 120）"
    )
    actions_parser.add_argument(
        "--checkpoint", default=None, help="可恢复导入状态文件；默认位于输出文件旁"
    )
    actions_parser.add_argument(
        "--checkpoint-every", type=int, default=25, help="每批完成多少证券后原子保存进度"
    )
    actions_resume = actions_parser.add_mutually_exclusive_group()
    actions_resume.add_argument(
        "--resume", dest="resume", action="store_true", default=True, help="从兼容 checkpoint 续传"
    )
    actions_resume.add_argument(
        "--no-resume", dest="resume", action="store_false", help="清除旧 checkpoint 并重新导入"
    )
    actions_parser.add_argument(
        "--print-all-rejections",
        action="store_true",
        help="打印全部拒绝原因；默认仅打印前 20 条与省略数量",
    )
    duckdb_parser = data_subparsers.add_parser(
        "build-duckdb", help="从 Parquet 构建不可变 DuckDB generation"
    )
    duckdb_parser.add_argument("--source", default="./data/parquet", help="Parquet 数据根")
    duckdb_parser.add_argument("--output", default="./data/duckdb", help="DuckDB generation 根")
    duckdb_parser.add_argument(
        "--base-manifest",
        default=None,
        help="继承未重建 shard 的已有 manifest；同盘优先使用硬链接",
    )
    duckdb_parser.add_argument("--temp-directory", default=None, help="DuckDB spill 临时目录")
    duckdb_parser.add_argument("--threads", type=int, default=8)
    duckdb_parser.add_argument("--memory-limit", default="12GB")
    duckdb_parser.add_argument("--max-temp-size", default="200GB")
    duckdb_parser.add_argument(
        "--source-file-batch-size",
        type=int,
        default=256,
        help="每批读取的单文件 Parquet 数量上限",
    )
    duckdb_parser.add_argument(
        "--domains", default="meta,daily,finance,actions", help="逗号分隔的构建领域"
    )
    duckdb_parser.add_argument(
        "--inherit-only",
        action="store_true",
        help="只把 base manifest 的 shard 组合到新 generation，不构建新 domain",
    )
    duckdb_parser.add_argument(
        "--datasets",
        default="",
        help="可选 canonical dataset 列表；空值表示所选 domain 的全部数据集",
    )
    duckdb_parser.add_argument(
        "--minute-years",
        default="",
        help="可选分钟年份，如 2024-2026 或 2024,2026；空值表示源数据全部年份",
    )
    duckdb_parser.add_argument(
        "--minute-quarterly-datasets",
        default="",
        help="需要按季度分片的分钟数据集；高性能推荐 stock_1m，其余默认按年度",
    )
    resume_group = duckdb_parser.add_mutually_exclusive_group()
    resume_group.add_argument(
        "--resume", dest="resume", action="store_true", default=True, help="续建相同 generation"
    )
    resume_group.add_argument(
        "--no-resume", dest="resume", action="store_false", help="已有 staging 时拒绝构建"
    )
    duckdb_parser.add_argument(
        "--retain-generations", type=int, default=2, help="显式清理时至少保留的旧 generation 数"
    )
    duckdb_parser.add_argument("--dry-run", action="store_true", help="只做源数据和资源预检")
    duckdb_parser.add_argument(
        "--no-publish", action="store_true", help="构建但不切换 active manifest"
    )
    duckdb_parser.add_argument(
        "--print-full-manifest",
        action="store_true",
        help="在终端打印完整源指纹；默认只打印摘要，完整内容始终写入 manifest 文件",
    )
    cleanup_parser = data_subparsers.add_parser(
        "cleanup-duckdb", help="列出或显式清理不再活动的旧 DuckDB generation"
    )
    cleanup_parser.add_argument("--output", default="./data/duckdb", help="DuckDB generation 根")
    cleanup_parser.add_argument("--keep-generations", type=int, default=2)
    cleanup_parser.add_argument("--apply", action="store_true", help="实际删除候选目录；默认仅列出")

    # jupyterlab / lab 命令
    lab_parser = subparsers.add_parser(
        "lab", aliases=["jupyterlab"], help="启动 BulletTrade 研究环境 (JupyterLab)"
    )
    lab_parser.add_argument("--ip", dest="ip", default=None, help="监听地址，默认 127.0.0.1")
    lab_parser.add_argument(
        "--port", dest="port", type=int, default=None, help="监听端口，默认 8088"
    )
    lab_parser.add_argument(
        "--notebook-dir",
        dest="notebook_dir",
        default=None,
        help="Notebook 根目录（默认 ~/bullet-trade）",
    )
    lab_parser.add_argument(
        "--no-browser", dest="no_browser", action="store_true", help="启动时不自动打开浏览器"
    )
    lab_parser.add_argument(
        "--browser", dest="browser", action="store_true", help="强制启动后打开浏览器"
    )
    lab_parser.add_argument("--token", dest="token", default=None, help="指定访问 token")
    lab_parser.add_argument(
        "--no-token", dest="no_token", action="store_true", help="关闭 token 验证（不建议）"
    )
    lab_parser.add_argument(
        "--password", dest="password", default=None, help="访问密码（可选，建议在公网监听时设置）"
    )
    lab_parser.add_argument("--certfile", dest="certfile", default=None, help="TLS 证书路径")
    lab_parser.add_argument("--keyfile", dest="keyfile", default=None, help="TLS 私钥路径")
    lab_parser.add_argument(
        "--allow-origin", dest="allow_origin", default=None, help="允许的跨域来源"
    )
    lab_parser.add_argument(
        "--diagnose", dest="diagnose", action="store_true", help="仅做依赖/端口诊断，不启动服务"
    )
    return parser


def main():
    """主函数"""
    parser = create_parser()
    args = parser.parse_args()
    # 若提供 env 文件，覆盖加载一次，便于区分客户端/服务端环境
    try:
        if getattr(args, "env_file", None):
            from bullet_trade.utils.env_loader import load_env  # 延迟导入，避免循环

            load_env(env_file=args.env_file, override=True)
            _refresh_env_dependents()
    except Exception:
        pass
    overrides = apply_cli_overrides(args) if getattr(args, "command", None) else {}

    if not args.command:
        parser.print_help()
        return 0

    # 导入命令处理模块
    if args.command == "backtest":
        from .backtest import run_backtest

        return run_backtest(args)
    elif args.command == "optimize":
        from .optimize import run_optimize

        return run_optimize(args)
    elif args.command == "report":
        from .report import run_report

        return run_report(args)
    elif args.command == "live":
        from .live import run_live

        return run_live(args, live_config_override=(overrides or None))
    elif args.command == "server":
        from bullet_trade.server.cli import run_server_command

        return run_server_command(args)
    elif args.command == "data":
        from bullet_trade.cli.data import run_data_command

        return run_data_command(args)
    elif args.command in ("lab", "jupyterlab"):
        from bullet_trade.cli.jupyterlab import run_lab

        return run_lab(args)
    else:
        print(f"未知命令: {args.command}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
