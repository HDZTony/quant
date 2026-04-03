"""
K 线监控面板入口。

用法：
    uv run python app.py              # 生产模式，端口 8000
    uv run python app.py --dev        # 开发模式，CORS 允许 Vite dev server
    uv run python app.py --port 9000  # 自定义端口
"""

import argparse

import uvicorn


def main():
    parser = argparse.ArgumentParser(description="K 线监控面板")
    parser.add_argument("--dev", action="store_true", help="开发模式")
    parser.add_argument("--port", type=int, default=9090, help="服务端口（默认 9090）")
    parser.add_argument("--host", default="0.0.0.0", help="监听地址（默认 0.0.0.0）")
    args = parser.parse_args()

    uvicorn.run(
        "api:app",
        host=args.host,
        port=args.port,
        reload=args.dev,
    )


if __name__ == "__main__":
    main()
