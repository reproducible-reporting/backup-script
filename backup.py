#!/usr/bin/env python3
"""Backup with btrfs snapshots and multiple borg repositories.

The script is completely configured with a YAML file and executed as follows

```
backup.py CONFIG [-n] [-s]
```

The option `-n` will result in a dry run, which can be used to check the configuration.

The option `-s` skips the creation of a new snapshot, useful to retry after a failed borg backup.


The config file has the following format:

```
datetime_format: '%Y_%m_%d__%H_%M_%S'  # Datetime format used for snapshots.
keep_tenminutely: 12  # Number of 10-minutely snapshots that are kept.
keep_hourly: 48  # Number of hourly snapshots that are kept
keep_daily: 14  # Number of daily snapshots that are kept
keep_weekly: 20  # Number of weekly snapshots that are kept
keep_monthly: 12  # Number of monthly snapshots that are kept

btrfs:
  uuid: '' # UUID of btrfs disk
  mount: '' # Mount point of btrfs root
  source: '' # btrfs volume to take snapshots of
  prefix: '' # prefix for btrfs snapshots
  pre: [] # Commands to be executed before making a snapshot
  post: [] # Commands to be executed after making a snapshot
  # Note that each command is a list of lists and is not processed by a shell.

borg:
  prefix: '' # Prefix used for archive names
  env: {} # Dictionary with environment variables set for each Borg command.
  extra: [] # Extra arguments for borg in list format
  paths: [] # Paths inside the subvolume to back up
  repositories: [] # List of borg repositories to where snapshots are backed up.
  # If you prefer to skip the borg backup, leave the list of repositories empty.
```

"""

import argparse
import os
import signal
import subprocess
import sys
from datetime import datetime

import yaml


def grandfatherson(dts, *, tenminutely=0, hourly=0, daily=0, weekly=0, monthly=0):
    """Determine which datetimes should be kept and which should be pruned.

    Parameters
    ----------
    dts
        A list of datetime objects.
    tenminutely
        The number of most recent snapshots that should be kept every ten minutes.
    hourly
        The number of most recent hourly snapshots that should be kept.
    daily
        The number of most recent daily snapshots that should be kept.
    weekly
        The number of most recent weekly snapshots that should be kept.
    monthly
        The number of most recent monthly snapshots that should be kept.

    Returns
    -------
    keep_dts
        The datetimes to be kept.
    prune_dts
        The datetimes to be pruned.

    Notes
    -----
    The most recent date is always kept.
    """
    dts = sorted(dts, reverse=True)
    keep_flags = [False] * len(dts)
    # Always keep the most recent.
    keep_flags[0] = True

    timelines = [
        ("%Y-%m-%d-%H-%M", tenminutely, 1),
        ("%Y-%m-%d-%H", hourly, 0),
        ("%Y-%m-%d", daily, 0),
        ("%Y-%W", weekly, 0),
        ("%Y-%m", monthly, 0),
    ]

    for fmt, need, drop in timelines:
        if need == 0:
            continue
        # We're keeping the most recent one, but don't include it in the counts
        have = 0
        labels = [dt.strftime(fmt) for dt in dts]
        if drop > 0:
            labels = [label[:-drop] for label in labels]
        for i in range(len(labels) - 1):
            if labels[i] != labels[i + 1]:
                keep_flags[i] = True
                have += 1
                if have > need or need < 0:
                    break
        # Always keep the last one when we don't have enough.
        if need > 0 and have < need:
            keep_flags[-1] = True

    keep_dts = []
    prune_dts = []
    for keep, dt in zip(keep_flags, dts, strict=False):
        if keep:
            keep_dts.append(dt)
        else:
            prune_dts.append(dt)
    return keep_dts, prune_dts


def test_grandfatherson_none():
    dts = [
        datetime(2022, 5, 5, 0, 0, 0),
        datetime(2022, 5, 4, 0, 0, 0),
        datetime(2022, 5, 2, 0, 0, 0),
        datetime(2022, 5, 1, 0, 0, 0),
    ]
    assert grandfatherson(dts) == (dts[:1], dts[1:])


def test_grandfatherson_tenminutely():
    dts = [
        datetime(2022, 5, 5, 17, 30, 0),
        datetime(2022, 5, 5, 17, 25, 0),
        datetime(2022, 5, 5, 17, 20, 0),
        datetime(2022, 5, 5, 16, 55, 0),
        datetime(2022, 5, 5, 16, 50, 0),
        datetime(2022, 5, 5, 16, 40, 0),
        datetime(2022, 5, 5, 16, 30, 0),
    ]
    keep_dts = [dts[0], dts[2], dts[4], dts[5]]
    prune_dts = [dts[1], dts[3], dts[6]]
    assert grandfatherson(dts, tenminutely=3) == (keep_dts, prune_dts)


def test_grandfatherson_hourly():
    dts = [
        datetime(2022, 5, 8, 0, 0, 0),
        datetime(2022, 5, 5, 17, 0, 0),
        datetime(2022, 5, 5, 16, 30, 0),
        datetime(2022, 5, 5, 16, 0, 0),
        datetime(2022, 5, 5, 15, 0, 0),
    ]
    keep_dts = [dts[0], dts[1], dts[3]]
    prune_dts = [dts[2], dts[4]]
    assert grandfatherson(dts, hourly=3) == (keep_dts, prune_dts)


def test_grandfatherson_daily1():
    dts = [
        datetime(2022, 5, 5, 0, 0, 0),
        datetime(2022, 5, 4, 0, 0, 0),
        datetime(2022, 5, 2, 0, 0, 0),
        datetime(2022, 5, 1, 0, 0, 0),
    ]
    assert grandfatherson(dts, daily=3) == (dts[:-1], dts[-1:])


def test_grandfatherson_daily2():
    dts = [
        datetime(2022, 5, 5, 10, 0, 0),
        datetime(2022, 5, 4, 10, 0, 0),
        datetime(2022, 5, 4, 9, 0, 0),
        datetime(2022, 5, 2, 10, 0, 0),
        datetime(2022, 5, 1, 10, 0, 0),
    ]
    keep_dts = [dts[0], dts[2], dts[3]]
    prune_dts = [dts[1], dts[4]]
    assert grandfatherson(dts, daily=3) == (keep_dts, prune_dts)


def test_grandfatherson_daily_too_few():
    dts = [
        datetime(2022, 5, 5, 0, 0, 0),
        datetime(2022, 5, 4, 0, 0, 0),
    ]
    assert grandfatherson(dts, daily=3) == (dts[:2], [])


def test_grandfatherson_daily_too_few_oldest1():
    dts = [
        datetime(2022, 5, 5, 10, 0, 0),
        datetime(2022, 5, 4, 10, 0, 0),
        datetime(2022, 5, 4, 9, 0, 0),
    ]
    keep_dts = [dts[0], dts[2]]
    prune_dts = [dts[1]]
    assert grandfatherson(dts, daily=3) == (keep_dts, prune_dts)


def test_grandfatherson_daily_too_few_oldest2():
    dts = [
        datetime(2022, 5, 5, 10, 0, 0),
        datetime(2022, 5, 5, 8, 0, 0),
    ]
    assert grandfatherson(dts, daily=3) == (dts, [])


def test_grandfatherson_monthly():
    dts = [
        datetime(2022, 5, 5, 0, 0, 0),
        datetime(2022, 5, 4, 0, 0, 0),
        datetime(2022, 5, 2, 0, 0, 0),
        datetime(2022, 5, 1, 0, 0, 0),
        datetime(2022, 4, 20, 0, 0, 0),
        datetime(2022, 4, 10, 0, 0, 0),
        datetime(2022, 3, 8, 0, 0, 0),
        datetime(2022, 3, 7, 0, 0, 0),
        datetime(2022, 2, 8, 0, 0, 0),
    ]
    keep_dts = [dts[0], dts[3], dts[5], dts[7]]
    prune_dts = [dts[1], dts[2], dts[4], dts[6], dts[8]]
    assert grandfatherson(dts, monthly=3) == (keep_dts, prune_dts)


def test_grandfatherson_weekly():
    dts = [
        datetime(2022, 6, 13, 0, 0, 0),
        datetime(2022, 6, 12, 0, 0, 0),
        datetime(2022, 6, 11, 0, 0, 0),
        datetime(2022, 6, 7, 0, 0, 0),
        datetime(2022, 6, 6, 0, 0, 0),
        datetime(2022, 6, 5, 0, 0, 0),
        datetime(2022, 5, 29, 0, 0, 0),
        datetime(2022, 5, 30, 0, 0, 0),
    ]
    keep_dts, prune_dts = grandfatherson(dts, weekly=3)
    assert keep_dts == [dts[0], dts[4], dts[7]]
    assert prune_dts == [dts[1], dts[2], dts[3], dts[5], dts[6]]


def parse_args():
    parser = argparse.ArgumentParser(description="Backup with btrfs and borg")
    parser.add_argument("config", help="YAML config file")
    parser.add_argument("-n", "--dry-run", default=False, action="store_true")
    parser.add_argument("-s", "--skip-snapshot", default=False, action="store_true")
    return parser.parse_args()


def main():
    args = parse_args()
    with open(args.config, encoding="utf-8") as f:
        config = yaml.safe_load(f)

    # Work on the btrfs part
    subvol_new = None if args.skip_snapshot else _create_btrfs_snapshot(config, args)
    snapshots = _prune_old_btrfs_snapshots(config, args, subvol_new)

    # Work on the borg part
    env = config["borg"].get("env")
    for repository in config["borg"]["repositories"]:
        if not _check_borg_repository(repository, env):
            info(f"Could not access {repository}")
            continue
        archives = _get_borg_archives(config, repository, env)

        info(f"Creating new borg archives ({repository})")
        for dt_keep, subvol in snapshots.items():
            if dt_keep in archives:
                continue
            _create_borg_archive(config, args, repository, env, subvol)

        info(f"Pruning old archives if any ({repository})")
        removed = _prune_old_borg_archives(args, repository, env, snapshots, archives)
        if removed:
            _compact_borg_repository(args, repository, env)


def _create_btrfs_snapshot(config, args):
    try:
        info("Preparing for snapshot")
        for split_args in config["btrfs"]["pre"]:
            run(split_args, args.dry_run)

        info("Making a new snapshot")
        suffix_new = datetime.now().strftime(config["datetime_format"])
        subvol_new = config["btrfs"]["prefix"] + suffix_new
        dn_new = config["btrfs"]["mount"] + subvol_new
        run(
            [
                "btrfs",
                "subvolume",
                "snapshot",
                "-r",
                config["btrfs"]["mount"] + config["btrfs"]["source"],
                dn_new,
            ],
            args.dry_run,
        )
    finally:
        info("Cleaning after snapshot")
        for split_args in config["btrfs"]["post"]:
            run(split_args, args.dry_run)
    return subvol_new


def _prune_old_btrfs_snapshots(config, args, subvol_new):
    info("Pruning old snapshots")
    # Loop over existing snapshots, derive dates and keep a dictionary.
    snapshots = {}
    output = run(["btrfs", "subvolume", "list", config["btrfs"]["mount"]], capture=True)
    for line in output.split("\n"):
        words = line.split()
        if len(words) == 0:
            continue
        subvol = words[-1]
        if not subvol.startswith(config["btrfs"]["prefix"]):
            continue
        dt = parse_suffix(subvol[len(config["btrfs"]["prefix"]) :], config["datetime_format"])
        snapshots[dt] = subvol

    # Add new snapshot in case of dry run
    if args.dry_run and not args.skip_snapshot:
        # Overwrite dt_new with the parsed one for consistency
        dt_new = parse_suffix(
            subvol_new[len(config["btrfs"]["prefix"]) :], config["datetime_format"]
        )
        snapshots[dt_new] = subvol_new

    # Determine which to prune
    _, dts_prune = grandfatherson(
        list(snapshots),
        tenminutely=config["keep_tenminutely"],
        hourly=config["keep_hourly"],
        daily=config["keep_daily"],
        weekly=config["keep_weekly"],
        monthly=config["keep_monthly"],
    )

    # Execute subvolume deletion commands
    for dt in dts_prune:
        run(
            [
                "btrfs",
                "subvolume",
                "delete",
                config["btrfs"]["mount"] + snapshots[dt],
            ],
            args.dry_run,
        )
        del snapshots[dt]

    return snapshots


def _check_borg_repository(repository, env):
    try:
        run(["borg", "info", repository], env=(os.environ | env))
    except subprocess.CalledProcessError:
        return False
    return True


def _get_borg_archives(config, repository, env):
    info(f"Getting a list of borg archives ({repository})")
    prefix = config["borg"]["prefix"]
    output = run(["borg", "list", repository], env=(os.environ | env), capture=True)
    archives = {}
    for line in output.split("\n"):
        words = line.strip().split()
        if len(words) == 0:
            continue
        archive = words[0]
        assert archive.startswith(prefix)
        dt = parse_suffix(archive[len(prefix) :], config["datetime_format"])
        archives[dt] = archive
    return archives


def _prune_old_borg_archives(args, repository, env, snapshots, archives):
    info(f"Removing old borg archives ({repository})")
    removed = False
    for dt, archive in archives.items():
        if dt not in snapshots:
            removed = True
            run(
                [
                    "borg",
                    "delete",
                    f"{repository}::{archive}",
                ],
                args.dry_run,
                env=(os.environ | env),
            )
    return removed


def _compact_borg_repository(args, repository, env):
    info(f"Compacting repository after removing old archives ({repository})")
    run(
        [
            "borg",
            "compact",
            repository,
        ],
        args.dry_run,
        env=(os.environ | env),
    )


def _create_borg_archive(config, args, repository, env, subvol):
    dn_current = config["btrfs"]["mount"] + config["btrfs"]["prefix"] + "current"
    if os.path.isdir(dn_current):
        run(["umount", dn_current], args.dry_run, check=False)
    else:
        info("Creating " + dn_current)
        os.makedirs(dn_current)

    run(
        ["mount", "UUID=" + config["btrfs"]["uuid"], dn_current, "-o", f"subvol={subvol},noatime"],
        args.dry_run,
    )

    # Ignore non-existing paths
    paths = [
        path for path in config["borg"]["paths"] if os.path.exists(os.path.join(dn_current, path))
    ]

    try:
        suffix = subvol[len(config["btrfs"]["prefix"]) :]
        dt = parse_suffix(suffix, config["datetime_format"])
        timestamp = dt.isoformat()
        run(
            [
                "borg",
                "create",
                "--verbose",
                "--stats",
                "--show-rc",
                "--timestamp",
                timestamp,
            ]
            + config["borg"]["extra"]
            + [
                f"{repository}::{config['borg']['prefix']}{suffix}",
            ]
            + paths,
            args.dry_run,
            env=(os.environ | env),
            cwd=dn_current,
        )
    finally:
        run(["umount", dn_current], args.dry_run)
        info("Removing " + dn_current)
        os.rmdir(dn_current)


def parse_suffix(suffix, datetime_format):
    return datetime.strptime(suffix, datetime_format)


def info(message):
    """Print a timestamped info message."""
    print(datetime.now().isoformat(), message)


def run(cmd, dry_run=False, check=True, capture=False, **kwargs):
    """Print and run a command."""
    if dry_run:
        info("Skipping " + " ".join(cmd))
        return ""
    info("Running " + " ".join(cmd))
    # Make sure output is written in correct order.
    sys.stdout.flush()

    if "stdin" not in kwargs:
        kwargs["stdin"] = subprocess.DEVNULL
    if "encoding" not in kwargs:
        kwargs["encoding"] = "utf-8"
        kwargs["universal_newlines"] = True
    if capture:
        kwargs["stdout"] = subprocess.PIPE
    with subprocess.Popen(cmd, **kwargs) as process:
        try:
            process.wait()
        except KeyboardInterrupt:
            process.send_signal(signal.SIGINT)
        if check and process.returncode != 0:
            raise subprocess.CalledProcessError(process.returncode, cmd)
        if capture:
            return process.stdout.read()
    return ""


if __name__ == "__main__":
    main()
