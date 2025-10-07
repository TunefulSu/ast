#!/usr/bin/env python3
"""
astOS – Installer (runs inside official Arch LiveISO)
全自动安装脚本：分区、格式化、btrfs子卷、rootfs拷贝、grub、首次账户
用法：
    python main.py  <btrfs-partition>  <disk>  [<efi-partition>]
例：
    python main.py  /dev/sda3  /dev/sda  /dev/sda1
"""
import os
import sys
import subprocess
import shutil
import pathlib
import argparse
import re
import tempfile
import json
import fcntl

DISTRO_NAME = "astOS"
AST_DIR     = "/mnt/ast"
EFI_DIR     = "/mnt/boot/efi"
REPO_URL    = "https://github.com/lambdanil/astOS.git"
PKG_LIST    = [
    "base", "base-devel", "linux", "linux-firmware", "btrfs-progs",
    "vim", "git", "python", "python-btrfs", "os-prober", "grub"
]
LOCK_FILE   = "/run/ast-install.lock"

def run(cmd, *, shell=False, check=True):
    """统一封装：默认列表传参，shell=False"""
    if shell:
        print(f":: {cmd}", file=sys.stderr)
        try:
            subprocess.run(cmd, shell=True, check=check,
                           stdout=subprocess.PIPE if check else None,
                           stderr=subprocess.PIPE if check else None,
                           text=True)
        except subprocess.CalledProcessError as e:
            sys.exit(f"Command failed: {cmd}")
    else:
        print(f":: {' '.join(cmd)}", file=sys.stderr)
        try:
            subprocess.run(cmd, check=check,
                           stdout=subprocess.PIPE if check else None,
                           stderr=subprocess.PIPE if check else None,
                           text=True)
        except subprocess.CalledProcessError as e:
            sys.exit(f"Command failed: {' '.join(cmd)}")

def flock(fd):
    fcntl.flock(fd, fcntl.LOCK_EX)

def part_and_format(btrfs_part, efi_part=None):
    run(["mkfs.btrfs", "-f", btrfs_part])
    if efi_part:
        run(["mkfs.fat", "-F32", efi_part])

def mount_btrfs_top(btrfs_part):
    run(["mount", "-t", "btrfs", btrfs_part, "/mnt"])
    for sv in ["@", "@.snapshots", "@home", "@var", "@etc", "@boot"]:
        run(["btrfs", "subvolume", "create", f"/mnt/{sv}"])
    run(["umount", "/mnt"])

def mount_install_tree(btrfs_part, efi_part=None):
    mounts = [
        (["-o", "subvol=@,compress=zstd"], "/", ""),
        (["-o", "subvol=@home,compress=zstd"], "/home", "home"),
        (["-o", "subvol=@var,compress=zstd"], "/var", "var"),
        (["-o", "subvol=@etc,compress=zstd"], "/etc", "etc"),
        (["-o", "subvol=@boot,compress=zstd"], "/boot", "boot"),
    ]
    for opt, mnt, d in mounts:
        target = pathlib.Path(AST_DIR) / d.lstrip("/")
        target.mkdir(parents=True, exist_ok=True)
        run(["mount", "-t", "btrfs"] + opt + [btrfs_part, str(target)])
    if efi_part:
        pathlib.Path(EFI_DIR).mkdir(parents=True, exist_ok=True)
        run(["mount", efi_part, EFI_DIR])

def bootstrap():
    run(["pacstrap", AST_DIR] + PKG_LIST)
    run(["genfstab", "-U", AST_DIR], shell=False)
    fstab = pathlib.Path(f"{AST_DIR}/etc/fstab").read_text()
    fstab = fstab.replace("subvol=@ ", "subvol=@,compress=zstd ")
    fstab = fstab.replace("subvol=@home ", "subvol=@home,compress=zstd ")
    fstab = fstab.replace("subvol=@var ", "subvol=@var,compress=zstd ")
    fstab = fstab.replace("subvol=@etc ", "subvol=@etc,compress=zstd ")
    if "@boot" not in fstab:
        uuid = subprocess.check_output(
            ["blkid", "-s", "UUID", "-o", "value",
             subprocess.check_output(["findmnt", "-nvo", "SOURCE", "/mnt"], text=True).strip()],
            text=True).strip()
        fstab += f"\n# /boot subvol\nUUID={uuid} /boot btrfs subvol=@boot,compress=zstd 0 0\n"
    pathlib.Path(f"{AST_DIR}/etc/fstab").write_text(fstab)

def install_bootloader(disk, efi_part=None):
    if efi_part:
        run(["arch-chroot", AST_DIR, "grub-install", "--target=x86_64-efi",
             "--efi-directory=/boot/efi", f"--bootloader-id={DISTRO_NAME}"])
    else:
        run(["arch-chroot", AST_DIR, "grub-install", "--target=i386-pc", disk])
    run(["arch-chroot", AST_DIR, "grub-mkconfig", "-o", "/boot/grub/grub.cfg"])

def deploy_ast_tools():
    tmp = tempfile.mkdtemp()
    run(["git", "clone", "--depth", "1", REPO_URL, tmp])
    ast_tools = pathlib.Path(f"{AST_DIR}/usr/share/ast")
    ast_tools.mkdir(parents=True, exist_ok=True)
    shutil.copy(f"{tmp}/astpk.py",  ast_tools / "astpk.py")
    shutil.copy(f"{tmp}/update.py", ast_tools / "update.py")
    shutil.copy(f"{tmp}/main-cli.py", ast_tools / "main-cli.py")
    os.symlink("/usr/share/ast/main-cli.py", f"{AST_DIR}/usr/local/bin/ast")
    os.chmod(f"{AST_DIR}/usr/local/bin/ast", 0o755)
    shutil.rmtree(tmp)

def create_base_snapshot():
    snap_root = pathlib.Path(f"{AST_DIR}/.snapshots")
    snap_root.mkdir(parents=True, exist_ok=True)
    for d in ["rootfs", "var", "etc", "boot"]:
        (snap_root / d).mkdir(exist_ok=True)
    run(["btrfs", "subvolume", "snapshot", "-r", AST_DIR, f"{snap_root}/rootfs/snapshot-0"])
    run(["btrfs", "subvolume", "snapshot", "-r", f"{AST_DIR}/var", f"{snap_root}/var/var-0"])
    run(["btrfs", "subvolume", "snapshot", "-r", f"{AST_DIR}/etc", f"{snap_root}/etc/etc-0"])
    run(["btrfs", "subvolume", "snapshot", "-r", f"{AST_DIR}/boot", f"{snap_root}/boot/boot-0"])
    run(["btrfs", "subvolume", "set-default", AST_DIR, "/mnt"])

def first_boot_user():
    print("\n>>> Creating first user")
    user = input("Username: ").strip()
    if not re.match(r"^[a-z_][a-z0-9_-]*$", user):
        print("Invalid username, skip.")
        return
    run(["arch-chroot", AST_DIR, "useradd", "-m", "-G", "wheel", user])
    run(["arch-chroot", AST_DIR, "passwd", user])
    run(["arch-chroot", AST_DIR, "passwd", "root"])

def main():
    parser = argparse.ArgumentParser(description="astOS Installer")
    parser.add_argument("btrfs_part", help="BTRFS partition to install system")
    parser.add_argument("disk", help="Target disk (for grub)")
    parser.add_argument("efi_part", nargs="?", help="EFI System Partition (optional for BIOS)")
    args = parser.parse_args()

    if os.geteuid() != 0:
        print("Run as root in Arch LiveISO")
        sys.exit(1)

    with open(LOCK_FILE, "w") as lockfd:
        flock(lockfd)
        part_and_format(args.btrfs_part, args.efi_part)
        mount_btrfs_top(args.btrfs_part)
        mount_install_tree(args.btrfs_part, args.efi_part)
        bootstrap()
        install_bootloader(args.disk, args.efi_part)
        deploy_ast_tools()
        create_base_snapshot()
        first_boot_user()
    print("\n>>> Installation complete!  Reboot and enjoy astOS.")

if __name__ == "__main__":
    main()
