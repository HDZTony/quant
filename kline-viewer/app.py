"""
pywebview 桌面应用入口。

用法：
    uv run python app.py          # 生产模式，加载 frontend/dist
    uv run python app.py --dev    # 开发模式，连接 Vite dev server
"""

import argparse
import sys
from pathlib import Path

import webview

from api import Api

DIST_DIR = Path(__file__).resolve().parent / "frontend" / "dist" / "index.html"
DEV_URL = "http://localhost:5173"


def main():
    parser = argparse.ArgumentParser(description="K 线查看器")
    parser.add_argument("--dev", action="store_true", help="开发模式，连接 Vite dev server")
    args = parser.parse_args()

    api = Api()

    if args.dev:
        url = DEV_URL
    else:
        if not DIST_DIR.exists():
            print(f"构建产物不存在: {DIST_DIR}", file=sys.stderr)
            print("请先执行: 在 kline-viewer 目录运行 pnpm build（或 cd frontend && pnpm run build）", file=sys.stderr)
            sys.exit(1)
        url = str(DIST_DIR)

    webview.create_window(
        title="159506 ETF K 线查看器",
        url=url,
        js_api=api,
        width=1400,
        height=900,
        min_size=(1000, 600),
    )
    webview.start(debug=args.dev)


if __name__ == "__main__":
    main()
