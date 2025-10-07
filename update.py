cat update.py                                                                                                                                                                                            ─╯
#!/usr/bin/env python3
"""
ast 自更新脚本，独立进程，不占用自身文件句柄
"""
import os
import sys
import subprocess
import tempfile
import shutil

AST_GIT = "https://github.com/lambdanil/astOS.git"
AST_DIR = "/usr/share/ast"

def run(cmd):
    print(f":: {cmd}")
    subprocess.run(cmd, shell=True, check=True)

def main():
    if os.geteuid() != 0:
        print("Must run as root")
        sys.exit(1)
    tmp = tempfile.mkdtemp()
    try:
        run(["git", "clone", "--depth", "1", AST_GIT, tmp])
        run(["cp", "-r", f"{tmp}/astpk.py", f"{tmp}/main-cli.py", f"{tmp}/update.py", AST_DIR + "/"])
        run(["sync"])
        print("ast updated successfully – restart any open ast sessions")
    finally:
        shutil.rmtree(tmp, ignore_errors=True)

if __name__ == "__main__":
    main()
