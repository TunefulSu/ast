#!/usr/bin/env python3
"""
astOS –  CLI entrypoint (post-installation)
零业务逻辑，只路由子命令到 astpk.actions
"""
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from astpk import actions, utils

def print_usage():
    print("ast <command> [args...]")
    print("Run `ast help` for the full cheat sheet")
    sys.exit(1)

def main():
    if len(sys.argv) < 2:
        print_usage()

    cmd  = sys.argv[1]
    args = sys.argv[2:]

    utils.require_root()

    router = {
        "help":         actions.show_help,
        "clone":        lambda: actions.clone(args),
        "clone-tree":   lambda: actions.clone_tree(args),
        "branch":       lambda: actions.branch(args),
        "ubranch":      lambda: actions.ubranch(args),
        "deploy":       lambda: actions.deploy(args),
        "chroot":       lambda: actions.chroot(args),
        "run":          lambda: actions.run_in_snapshot(args),
        "tree-run":     lambda: actions.tree_run(args),
        "install":      lambda: actions.pkg_install(args),
        "remove":       lambda: actions.pkg_remove(args),
        "upgrade":      lambda: actions.upgrade(args),
        "sync":         lambda: actions.sync_tree(args),
        "desc":         lambda: actions.set_desc(args),
        "base-update":  actions.base_update,
        "tmp":          actions.clean_tmp,
        "df":           actions.snapshot_df,
        "gc":           actions.gc,
        "edit-conf":    lambda: actions.edit_conf(args),
        "update-ast":   lambda: actions.update_ast(args),
    }

    if cmd not in router:
        utils.die(f"Unknown command: {cmd}")

    try:
        router[cmd]()
    except KeyboardInterrupt:
        utils.die("Interrupted by user", code=130)
    except Exception as e:
        utils.die(f"Fatal: {e}")

if __name__ == "__main__":
    main()
