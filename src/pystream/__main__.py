"""PyStream 命令行模块入口。

执行 ``python -m pystream`` 时转交给统一 CLI，进程退出码由 CLI 返回。
"""

from pystream.cli import main

if __name__ == "__main__":
    raise SystemExit(main())
