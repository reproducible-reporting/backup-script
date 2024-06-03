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
import logging
import os
import signal
import subprocess
import sys
from datetime import datetime

import yaml

LOGGER = logging.getLogger(__name__)


def grandfatherson(
    dts: list[datetime],
    *,
    tenminutely: int = 0,
    hourly: int = 0,
    daily: int = 0,
    weekly: int = 0,
    monthly: int = 0,
) -> tuple[list[datetime], list[datetime]]:
    """Use the GFS algorithm to determine which datetimes should be kept and which should be pruned.

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
    For more info on the GFS algorithm, see:
    https://en.wikipedia.org/wiki/Backup_rotation_scheme#Grandfather-father-son
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


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(prog="backup.py", description="Backup with btrfs and borg")
    parser.add_argument("config", help="YAML config file")
    parser.add_argument(
        "-n",
        "--dry-run",
        default=False,
        action="store_true",
        help="Skip actual BTRFS and Borg commands, except when getting info from them.",
    )
    parser.add_argument(
        "-s",
        "--skip-snapshot",
        default=False,
        action="store_true",
        help="Do not make a new snapshot. Only run Borg.",
    )
    parser.add_argument(
        "-q",
        "--quiet",
        default=False,
        action="store_true",
        help="Only show output of BTRFS and Borg commands.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None):
    """Main program."""
    args = parse_args(argv)

    # Set log level
    logging.basicConfig(level=logging.ERROR if args.quiet else logging.INFO)

    # Load yaml config file
    with open(args.config, encoding="utf-8") as f:
        config = yaml.safe_load(f)

    # Work on the btrfs part
    subvol_new = None if args.skip_snapshot else _create_btrfs_snapshot(config, args.dry_run)
    snapshots = _prune_old_btrfs_snapshots(config, args.dry_run, args.skip_snapshot, subvol_new)

    # Work on the borg part
    env = config["borg"].get("env", {})
    for repository in config["borg"]["repositories"]:
        if not _check_borg_repository(repository, env):
            LOGGER.info(f"Could not access {repository}")
            continue
        archives = _get_borg_archives(config, repository, env)

        LOGGER.info(f"Creating new borg archives ({repository})")
        for dt_keep, subvol in snapshots.items():
            if dt_keep in archives:
                continue
            _create_borg_archive(config, args.dry_run, repository, env, subvol)

        LOGGER.info(f"Pruning old archives if any ({repository})")
        removed = _prune_old_borg_archives(args.dry_run, repository, env, snapshots, archives)
        if removed:
            _compact_borg_repository(args.dry_run, repository, env)


def _create_btrfs_snapshot(config: dict[str], dry_run: bool) -> str:
    """Create a new BTRFS snapshot."""
    try:
        LOGGER.info("Preparing for snapshot")
        for split_args in config["btrfs"]["pre"]:
            run(split_args, dry_run)

        LOGGER.info("Making a new snapshot")
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
            dry_run,
        )
    finally:
        LOGGER.info("Cleaning after snapshot")
        for split_args in config["btrfs"]["post"]:
            run(split_args, dry_run)
    return subvol_new


def _prune_old_btrfs_snapshots(
    config: dict[str], dry_run: bool, skip_snapshot: bool, subvol_new: str
) -> dict[datetime, str]:
    """Delete old BTRFS snapshots using the GFS algorithm."""
    LOGGER.info("Pruning old snapshots")
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

    # Add new a snapshot in case of dry run.
    if dry_run and not skip_snapshot:
        # Overwrite dt_new with the parsed one for consistency
        dt_new = parse_suffix(
            subvol_new[len(config["btrfs"]["prefix"]) :], config["datetime_format"]
        )
        snapshots[dt_new] = subvol_new

    # Determine which snapshots to prune.
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
            dry_run,
        )
        del snapshots[dt]

    return snapshots


def _check_borg_repository(repository: str, env: dict[str, str]) -> bool:
    """Get basic info from a borg repository."""
    try:
        run(["borg", "info", repository], env=os.environ | env)
    except subprocess.CalledProcessError:
        return False
    return True


def _get_borg_archives(
    config: dict[str], repository: str, env: dict[str, str]
) -> dict[datetime, str]:
    """Get a list of archives in the Borg repository."""
    LOGGER.info(f"Getting a list of borg archives ({repository})")
    prefix = config["borg"]["prefix"]
    output = run(["borg", "list", repository], env=(os.environ | env), capture=True)
    archives = {}
    for line in output.split("\n"):
        words = line.strip().split()
        if len(words) == 0:
            continue
        archive = words[0]
        if not archive.startswith(prefix):
            raise AssertionError(f"Archive '{archive}' has the wrong prefix. Should be '{prefix}'")
        dt = parse_suffix(archive[len(prefix) :], config["datetime_format"])
        archives[dt] = archive
    return archives


def _prune_old_borg_archives(
    dry_run: bool,
    repository: str,
    env: dict[str, str],
    snapshots: dict[datetime, str],
    archives: dict[datetime, str],
) -> bool:
    """Delete old Bort archives using the GFS algorithm."""
    LOGGER.info(f"Removing old borg archives ({repository})")
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
                dry_run,
                env=(os.environ | env),
            )
    return removed


def _compact_borg_repository(dry_run: bool, repository: str, env: dict[str, str]):
    """Reduce the space occupied by the Borg archive by removing unused data."""
    LOGGER.info(f"Compacting repository after removing old archives ({repository})")
    run(
        [
            "borg",
            "compact",
            repository,
        ],
        dry_run,
        env=(os.environ | env),
    )


def _create_borg_archive(
    config: dict[str], dry_run: bool, repository: str, env: dict[str, str], subvol: str
):
    """Create a Borg backup from a BTRFS snapshot."""
    dn_current = config["btrfs"]["mount"] + config["btrfs"]["prefix"] + "current"
    if os.path.isdir(dn_current):
        run(["umount", dn_current], dry_run, check=False)
    else:
        LOGGER.info("Creating " + dn_current)
        os.makedirs(dn_current)

    run(
        ["mount", "UUID=" + config["btrfs"]["uuid"], dn_current, "-o", f"subvol={subvol},noatime"],
        dry_run,
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
            dry_run,
            env=(os.environ | env),
            cwd=dn_current,
        )
    finally:
        run(["umount", dn_current], dry_run)
        LOGGER.info("Removing " + dn_current)
        os.rmdir(dn_current)


def parse_suffix(suffix: str, datetime_format: str) -> datetime:
    """Extract the datetime object from the suffix of an archive directory."""
    return datetime.strptime(suffix, datetime_format)


def run(
    cmd: list[str], dry_run: bool = False, check: bool = True, capture: bool = False, **kwargs
) -> str:
    """Print and run a command."""
    if dry_run:
        LOGGER.info("Skipping " + " ".join(cmd))
        return ""
    LOGGER.info("Running " + " ".join(cmd))
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
    main(sys.argv[1:])
