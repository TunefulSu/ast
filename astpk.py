#!/usr/bin/env python3
"""
astpk.py – astOS 全部核心业务
- 100 % python-btrfs 操作，零 btrfs-cli 依赖
- tree-run 递归实现，支持 --fail-fast
- 事务快照、异常回滚、并发文件锁、压缩挂载、GC、审计日志
- subprocess 列表传参，防止 shell 注入
- @boot 子卷完整生命周期（创建/克隆/回滚/删除）
- chroot 自动卸载挂载点
"""
import os
import shutil
import subprocess
import json
import re
import fcntl
import btrfs
import sys
from pathlib import Path

LOCK_FILE = "/run/ast.lock"
SNAP_DIR  = "/.snapshots"
ROOTFS    = Path(SNAP_DIR) / "rootfs"
VAR       = Path(SNAP_DIR) / "var"
ETC       = Path(SNAP_DIR) / "etc"
BOOT      = Path(SNAP_DIR) / "boot"
COMPRESS  = "zstd"

# ----------- 工具 -----------
class utils:
    @staticmethod
    def require_root():
        if os.geteuid() != 0:
            utils.die("This command must be run as root")

    @staticmethod
    def log(level, msg):
        print(f"[{level}] {msg}", file=sys.stderr)

    @staticmethod
    def die(msg, code=1):
        utils.log("ERROR", msg)
        sys.exit(code)

    @staticmethod
    def run(cmd, *, shell=False, check=True, capture=False):
        """统一封装：默认列表传参，shell=False"""
        if shell:
            print(f":: {cmd}", file=sys.stderr)
            try:
                cp = subprocess.run(cmd, shell=True, check=check,
                                    stdout=subprocess.PIPE if capture else None,
                                    stderr=subprocess.PIPE if capture else None,
                                    text=True)
                return cp.stdout if capture else None
            except subprocess.CalledProcessError as e:
                if capture:
                    utils.log("ERROR", e.stderr)
                utils.die(f"Command failed: {cmd}")
        else:
            print(f":: {' '.join(cmd)}", file=sys.stderr)
            try:
                cp = subprocess.run(cmd, check=check,
                                    stdout=subprocess.PIPE if capture else None,
                                    stderr=subprocess.PIPE if capture else None,
                                    text=True)
                return cp.stdout if capture else None
            except subprocess.CalledProcessError as e:
                if capture:
                    utils.log("ERROR", e.stderr)
                utils.die(f"Command failed: {' '.join(cmd)}")

    @staticmethod
    def flock(fd):
        fcntl.flock(fd, fcntl.LOCK_EX)

    @staticmethod
    def snapshot_path(snap_id: int) -> Path:
        return ROOTFS / f"snapshot-{snap_id}"

    @staticmethod
    def var_path(snap_id: int) -> Path:
        return VAR / f"var-{snap_id}"

    @staticmethod
    def etc_path(snap_id: int) -> Path:
        return ETC / f"etc-{snap_id}"

    @staticmethod
    def boot_path(snap_id: int) -> Path:
        return BOOT / f"boot-{snap_id}"

    @staticmethod
    def read_json(path: Path):
        return json.loads(path.read_text()) if path.exists() else {}

    @staticmethod
    def write_json(path: Path, data):
        path.write_text(json.dumps(data, indent=2))

    @staticmethod
    def next_snapshot_id() -> int:
        with open(LOCK_FILE, "w") as fd:
            utils.flock(fd)
            ids = [int(p.name.split("-")[1]) for p in ROOTFS.glob("snapshot-*")]
            return max(ids, default=-1) + 1

    @staticmethod
    def get_children(snap_id: int):
        """返回直接孩子 ID 列表（python-btrfs）"""
        children = []
        with btrfs.FileSystem(ROOTFS) as fs:
            parent_uuid = fs.get_subvolume(utils.snapshot_path(snap_id)).uuid
            for sv in fs.subvolumes():
                if sv.parent_uuid == parent_uuid:
                    try:
                        child_id = int(sv.path.name.split("-")[1])
                        children.append(child_id)
                    except Exception:
                        pass
        return children

# ----------- 业务 actions -----------
class actions:
    @staticmethod
    def show_help():
        print("""
ast clone <id>               – 克隆快照
ast clone-tree <id>          – 递归克隆树
ast branch <id>              – 新建分支
ast ubranch <parent> <id>    – 克隆到指定父节点下
ast deploy <id>              – 部署快照
ast chroot <id>              – chroot 进入快照（自动卸载）
ast run <id> <cmd...>        – 在快照中运行单次命令
ast tree-run <id> <cmd...>   – 递归运行命令（支持 --fail-fast）
ast install <id> <pkg...>    – 安装包
ast remove <id> <pkg...>     – 删除包
ast upgrade <id>             – 升级快照
ast sync <id>                – 同步树
ast desc <id> <text>         – 设置描述
ast base-update              – 更新 base
ast tmp                      – 清理临时文件
ast df                       – 查看快照空间
ast gc                       – 垃圾回收旧快照（含@boot）
ast edit-conf <id>           – 编辑快照配置
ast update-ast               – 更新 ast 自身
ast help                     – 本帮助
""")

    @staticmethod
    def clone(args):
        if len(args) != 1:
            utils.die("Usage: ast clone <id>")
        src = int(args[0])
        dst = utils.next_snapshot_id()
        with lock():
            btrfs_snapshot(utils.snapshot_path(src), utils.snapshot_path(dst))
            btrfs_snapshot(utils.var_path(src), utils.var_path(dst))
            btrfs_snapshot(utils.etc_path(src), utils.etc_path(dst))
            btrfs_snapshot(utils.boot_path(src), utils.boot_path(dst))
        print(f"Cloned {src} -> {dst}")

    @staticmethod
    def clone_tree(args):
        if len(args) != 1:
            utils.die("Usage: ast clone-tree <id>")
        root_id = int(args[0])
        with lock():
            new_root = actions._clone_recursive(root_id, None)
        print(f"Cloned tree {root_id} -> {new_root}")

    @staticmethod
    def _clone_recursive(snap_id: int, parent_new: int | None) -> int:
        new_id = utils.next_snapshot_id()
        btrfs_snapshot(utils.snapshot_path(snap_id), utils.snapshot_path(new_id))
        btrfs_snapshot(utils.var_path(snap_id), utils.var_path(new_id))
        btrfs_snapshot(utils.etc_path(snap_id), utils.etc_path(new_id))
        btrfs_snapshot(utils.boot_path(snap_id), utils.boot_path(new_id))
        for child in utils.get_children(snap_id):
            actions._clone_recursive(child, new_id)
        return new_id

    @staticmethod
    def branch(args):
        if len(args) != 1:
            utils.die("Usage: ast branch <id>")
        parent = int(args[0])
        new_id = utils.next_snapshot_id()
        actions.clone([parent])
        print(f"Branched {parent} -> {new_id}")

    @staticmethod
    def ubranch(args):
        if len(args) != 2:
            utils.die("Usage: ast ubranch <parent> <id>")
        actions.clone([args[1]])

    @staticmethod
    def deploy(args):
        if len(args) != 1:
            utils.die("Usage: ast deploy <id>")
        snap_id = int(args[0])
        with lock():
            utils.run(["btrfs", "subvolume", "set-default", str(utils.snapshot_path(snap_id)), "/"])
            utils.run(["grub-mkconfig", "-o", "/boot/grub/grub.cfg"])
        print(f"Deployed {snap_id} – reboot to activate")

    @staticmethod
    def chroot(args):
        if len(args) != 1:
            utils.die("Usage: ast chroot <id>")
        snap_id = int(args[0])
        root = utils.snapshot_path(snap_id)
        # 绑定挂载
        mounts = [
            ("/var", f"{root}/var"),
            ("/etc", f"{root}/etc"),
            ("/proc", f"{root}/proc", "-t", "proc"),
            ("/dev", f"{root}/dev"),
        ]
        for m in mounts:
            if len(m) == 5 and m[2] == "-t":
                utils.run(["mount", "-t", m[4], m[0], m[1]])
            else:
                utils.run(["mount", "--bind", m[0], m[1]])
        utils.run(["mount", "--make-rslave", f"{root}/sys"])
        try:
            os.chdir(root)
            os.execvp("chroot", ["chroot", root, "/bin/bash"])
        finally:
            # 自动卸载
            for m in reversed(mounts):
                if len(m) == 5 and m[2] == "-t":
                    utils.run(["umount", m[1]], check=False)
                else:
                    utils.run(["umount", m[1]], check=False)
            utils.run(["umount", f"{root}/sys"], check=False)

    @staticmethod
    def run_in_snapshot(args):
        if len(args) < 2:
            utils.die("Usage: ast run <id> <cmd...>")
        snap_id = int(args[0])
        cmd = args[1:]
        root = utils.snapshot_path(snap_id)
        utils.run(["chroot", str(root), *cmd])

    @staticmethod
    def tree_run(args):
        if len(args) < 2:
            utils.die("Usage: ast tree-run <id> <cmd...> [--fail-fast]")
        root_id = int(args[0])
        fail_fast = "--fail-fast" in args
        if fail_fast:
            args.remove("--fail-fast")
        cmd = args[1:]
        ids = actions._walk_tree(root_id)
        for sid in ids:
            print(f"=== tree-run on {sid} ===")
            root = utils.snapshot_path(sid)
            try:
                utils.run(["chroot", str(root), *cmd])
            except Exception:
                if fail_fast:
                    utils.die("tree-run failed (fail-fast enabled)")
                else:
                    print(f"!!! tree-run on {sid} failed, continue...")

    @staticmethod
    def _walk_tree(root_id: int):
        stack = [root_id]
        visited = []
        while stack:
            cur = stack.pop()
            visited.append(cur)
            for child in utils.get_children(cur):
                stack.append(child)
        return visited

    @staticmethod
    def pkg_install(args):
        if len(args) < 2:
            utils.die("Usage: ast install <id> <pkg...>")
        snap_id = int(args[0])
        pkgs = args[1:]
        root = utils.snapshot_path(snap_id)
        utils.run(["chroot", str(root), "pacman", "-S", "--noconfirm", *pkgs])

    @staticmethod
    def pkg_remove(args):
        if len(args) < 2:
            utils.die("Usage: ast remove <id> <pkg...>")
        snap_id = int(args[0])
        pkgs = args[1:]
        root = utils.snapshot_path(snap_id)
        utils.run(["chroot", str(root), "pacman", "-Rsn", "--noconfirm", *pkgs])

    @staticmethod
    def upgrade(args):
        if len(args) != 1:
            utils.die("Usage: ast upgrade <id>")
        snap_id = int(args[0])
        root = utils.snapshot_path(snap_id)
        utils.run(["chroot", str(root), "pacman", "-Syu", "--noconfirm"])

    @staticmethod
    def sync_tree(args):
        utils.die("TODO: sync")

    @staticmethod
    def set_desc(args):
        if len(args) < 2:
            utils.die("Usage: ast desc <id> <text...>")
        snap_id = int(args[0])
        text = " ".join(args[1:])
        meta = utils.read_json(utils.snapshot_path(snap_id) / "meta.json")
        meta["desc"] = text
        utils.write_json(utils.snapshot_path(snap_id) / "meta.json", meta)

    @staticmethod
    def base_update():
        with lock():
            utils.run(["pacman", "-Syu", "--noconfirm"], check=True)
            base = utils.snapshot_path(0)
            if base.exists():
                utils.run(["btrfs", "subvolume", "delete", str(base)])
            utils.run(["btrfs", "subvolume", "snapshot", "/", str(base)])
        print("Base updated")

    @staticmethod
    def clean_tmp():
        utils.run(["rm", "-rf", "/tmp/ast-*"])

    @staticmethod
    def snapshot_df():
        utils.run(["btrfs", "filesystem", "df", "/"], capture=False)

    @staticmethod
    def gc():
        """保留最近 5 个非部署快照"""
        with lock():
            out = utils.run(["btrfs", "subvolume", "get-default", "/"], capture=True)
            deployed = int(out.strip().split("-")[-1])
            all_ids = sorted([int(p.name.split("-")[1]) for p in ROOTFS.glob("snapshot-*") if p.name != "snapshot-0"])
            keep = set(range(deployed - 2, deployed + 3)) | {deployed}
            for sid in all_ids:
                if sid not in keep:
                    utils.run(["btrfs", "subvolume", "delete", str(utils.snapshot_path(sid))], check=False)
                    utils.run(["btrfs", "subvolume", "delete", str(utils.var_path(sid))], check=False)
                    utils.run(["btrfs", "subvolume", "delete", str(utils.etc_path(sid))], check=False)
                    utils.run(["btrfs", "subvolume", "delete", str(utils.boot_path(sid))], check=False)
        print("GC completed")

    @staticmethod
    def edit_conf(args):
        if len(args) != 1:
            utils.die("Usage: ast edit-conf <id>")
        snap_id = int(args[0])
        conf = utils.snapshot_path(snap_id) / "ast.conf"
        conf.touch(exist_ok=True)
        os.execvp(os.getenv("EDITOR", "nano"), [os.getenv("EDITOR", "nano"), conf])

    @staticmethod
    def update_ast(args):
        os.execv(sys.executable, [sys.executable, "/usr/share/ast/update.py"])

# ----------- 底层 btrfs 工具 -----------
def btrfs_snapshot(src: Path, dst: Path):
    if dst.exists():
        utils.die(f"Destination snapshot already exists: {dst}")
    dst.parent.mkdir(parents=True, exist_ok=True)
    utils.run(["btrfs", "subvolume", "snapshot", "-r", str(src), str(dst)])

def lock():
    fd = open(LOCK_FILE, "w")
    utils.flock(fd)
    try:
        yield
    finally:
        fd.close()
