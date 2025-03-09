#!/usr/bin/env python3
"""Backups, revisited"""
import argparse
import concurrent.futures
import datetime
import os
import os.path as ospath
import sys
import threading
from queue import Queue
from random import shuffle
from subprocess import PIPE, Popen, call
from typing import Any, Iterable, TypeAlias

BACKUP_VOL = os.environ.get("BACKUP_VOL", "/var/backup")
RSYNC_ARGS = (
    "--acls",
    "--archive",
    "--compress",
    "--human-readable",
    "--inplace",
    "--numeric-ids",
    "--one-file-system",
    "--quiet",
    "--sparse",
    "--stats",
    "--xattrs",
    "-F",
)

RSYNC_STATUS = {
    0: "Success",
    23: "Partial transfer due to error",
    24: "Partial transfer due to vanished source files",
}
USER = os.getlogin()

COMPLETE = "\U000026aa"
FAIL = "\U0001f534"
RUNNING = "\U0001f536"
SKIPPING = "\U0001f535"
WAITING = "\U000026ab"

os.environ["TZ"] = "UTC"

ARGS: TypeAlias = tuple[Any, ...]  # pylint: disable=invalid-name
KWARGS: TypeAlias = dict[str, Any]  # pylint: disable=invalid-name


class OutputThread(threading.Thread):
    """Thread responsible for Output from backup threads"""

    queue: Queue[tuple[ARGS, KWARGS]] = Queue()
    daemon = True

    def run(self) -> None:
        while True:
            args, kwargs = self.queue.get()
            sprint(*args, **kwargs)
            self.queue.task_done()

    def print(self, *args: Any, **kwargs: Any) -> None:
        """Schedule content to be printed"""
        self.queue.put((args, kwargs))


def parse_args() -> argparse.Namespace:
    """Return the command line arguments parsed (or fail)."""
    parser = argparse.ArgumentParser(description="Back up a system")
    parser.add_argument(
        "-u",
        "--update",
        action="store_true",
        default=False,
        help="Update last backup instead of creating a new " "one.",
    )
    parser.add_argument(
        "-l", "--link", default=None, help="Create a symlink of this backup to LINK"
    )
    parser.add_argument(
        "-j", "--jobs", type=int, default=1, help="Number of parallel jobs"
    )
    parser.add_argument(
        "-v",
        "--volume",
        default=BACKUP_VOL,
        help=f"Backup volume (default: {BACKUP_VOL})",
    )
    parser.add_argument(
        "-r",
        "--random",
        action="store_true",
        default=False,
        help="Backup host's filesystems in random order",
    )
    parser.add_argument(
        "--user", default=USER, help="specify the remote user to log into"
    )
    parser.add_argument("host", type=str, nargs="+")

    return parser.parse_args()


def sprint(*args: Any, **kwargs: Any) -> None:
    """
    Print and flush standard out.
    """
    print(*args, **kwargs)
    sys.stdout.flush()


def is_executable(path: str) -> bool:
    """Return True if `path` exists and is executable"""
    realpath = ospath.realpath(path)

    return ospath.isfile(realpath) and os.access(realpath, os.X_OK)


class BackupClient:
    """Backup client for a host/volume pair"""

    def __init__(self, hostname: str, volume: str, user: str = USER) -> None:
        self.hostname = f"{user}@{hostname}"
        self.volume = ospath.realpath(volume)
        self.backup_vol: str | None = None
        self.host_dir = f"{volume}/{hostname}"
        self.filesystems = self.get_filesystems()
        self.stats = {i: WAITING for i in self.filesystems}
        self.output = OutputThread()
        self.output.start()

        if not ospath.isdir(self.host_dir):
            os.mkdir(self.host_dir)

    def get_filesystems(self) -> list[str]:
        """Return the list of host's filesystems to back up"""
        filesystems = []
        filename = f"{self.host_dir}/filesystems"
        with open(filename, encoding="UTF-8") as fs_file:
            for line in fs_file:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                filesystems.append(line)

        return filesystems

    def ssh(self, args: Iterable[str]) -> int:
        """Like subprocess.Popen: Execute args but using ssh on the client."""
        return call(["ssh", self.hostname, *args])

    def run_hook(self, name: str, *args: str) -> int:
        """Run the backup hook with given `name`, if available

        Return the exit status or 0 if nothing ran
        """
        hook = ospath.join(self.volume, name)

        if is_executable(hook):
            return call((hook,) + args)

        return 0

    def pre_backup(self) -> int:
        """To be run before .backup()"""
        hook_status = self.run_hook("pre-host", self.hostname, self.volume)

        if hook_status != 0:
            return hook_status

        with Popen(
            ("ssh", self.hostname, "mktemp", "-d", "--suffix=.backup"), stdout=PIPE
        ) as popen:
            assert popen.stdout is not None
            self.backup_vol = popen.stdout.read().rstrip().decode("utf-8")

        return popen.wait()

    @staticmethod
    def parse_path(filesystem: str) -> tuple[str, str]:
        """Given the `filesystem` entry return the path and backup "label"""
        parts = filesystem.partition(":")
        path = parts[0]
        label = ospath.basename(parts[2] if parts[2] else path) or "root"

        return path.strip(), label.strip()

    def backup_filesystem(
        self, filesystem: str, target: str, last_dir, update: bool
    ) -> None:
        """Back up the specified filesystem to target

        If last_dir is not None, use it as a --link-dest argument to rsync.

        If update is True, update the last backup instead of creating a new one.
        """
        hook_status = self.run_hook(
            "pre-filesystem",
            self.hostname,
            self.volume,
            filesystem,
            ["no", "yes"][update],
        )
        if hook_status != 0:
            self.print_stats((filesystem, SKIPPING))
            sys.exit(hook_status)

        self.print_stats((filesystem, RUNNING))
        source, dirname = self.parse_path(filesystem)
        assert self.backup_vol is not None
        bind_mount = ospath.join(self.backup_vol, dirname)

        self.ssh(("mkdir", "-p", bind_mount))
        status = self.ssh(("mount", "--bind", source, bind_mount))
        if status != 0:
            sys.exit(status)

        target_path = ospath.join(self.volume, self.hostname, target, dirname)

        if not target_path.startswith(self.volume):
            sys.stderr.write(
                f"Refusing to backup outside of {self.volume}: {target_path}\n"
            )
            self.ssh(("rmdir", bind_mount))
            sys.exit(1)

        args = self.build_args(update, last_dir, dirname, bind_mount, target_path)
        status = call(args)
        self.ssh(("umount", bind_mount))
        self.ssh(("rmdir", bind_mount))

        self.print_stats((filesystem, COMPLETE if status == 0 else FAIL))

        if status != 0:
            sys.exit(status)

        sys.exit(
            self.run_hook(
                "post-filesystem",
                self.hostname,
                self.volume,
                filesystem,
                ["no", "yes"][update],
                target_path,
            )
        )

    def backup(
        self, update=False, link_to: str | None = None, jobs=3, random=False
    ) -> None:
        """Back up the filesystems"""
        last_dir = get_last_dir(self.host_dir)
        target = self.get_target(update, last_dir)
        futures = []
        filesystems = self.filesystems[:]

        if random:
            shuffle(filesystems)

        with concurrent.futures.ThreadPoolExecutor(max_workers=jobs) as executor:
            for _dir in filesystems:
                futures.append(
                    executor.submit(
                        self.backup_filesystem, _dir, target, last_dir, update
                    )
                )
            concurrent.futures.wait(futures)

        timestamp = get_timestamp()
        self.output.print("")

        os.rename(
            f"{self.volume}/{self.hostname}/{target}",
            f"{self.volume}/{self.hostname}/{timestamp}",
        )

        if link_to:
            os.symlink(timestamp, f"{self.hostname}/{self.hostname}/{link_to}")

        latest_link = f"{self.volume}/{self.hostname}/latest"

        if ospath.exists(latest_link) or ospath.islink(latest_link):
            os.unlink(latest_link)
        os.symlink(timestamp, latest_link)

    def get_target(self, update: bool, last_dir: str | None) -> str:
        """
        Return (and create if necessary) the rsync target based on the
        options. exit() and print message to standard error if there are issues.

        update: whether this is an "update" backup.

        last_dir: The directory of the previous backup or None.
            If update == True, then last_dir must not be None

        """
        if update:
            if not last_dir:
                sys.stderr.write(
                    "--update specified, but no directory to update from.\n"
                )
                sys.exit(1)
            target = last_dir
        else:
            target = "0"
            full_target = f"{self.volume}/{self.hostname}/{target}"
            if ospath.isdir(full_target):
                sys.stderr.write(f"{target} already exists. Abort.\n")
                sys.exit(1)
            os.mkdir(full_target)

        return target

    def print_stats(self, update=None) -> None:
        """Prints the current status of the backup"""
        filesystems = self.filesystems[:]
        filesystems.sort(key=lambda i: self.parse_path(i)[1])

        if update:
            self.stats[update[0]] = update[1]
        self.output.print("\r", end="")
        for filesystem in filesystems:
            dirname = self.parse_path(filesystem)[1]
            self.output.print(f"{dirname}:{self.stats[filesystem]}", end=" ")

    def post_backup(self) -> int:
        """To be run after .backup()"""
        assert self.backup_vol
        status = self.ssh(("rmdir", self.backup_vol))

        hook_status = self.run_hook("post-host", self.hostname, self.volume)

        if hook_status != 0:
            return hook_status

        return status

    def build_args(  # pylint: disable=too-many-arguments,too-many-positional-arguments
        self,
        update: bool,
        last_dir: str | None,
        dirname: str,
        bind_mount: str,
        target_path: str,
    ) -> list[str]:
        """Return the command line args to back up a filesystem"""
        args = ["rsync", *RSYNC_ARGS]

        if last_dir:
            link_dest_path = ospath.join(self.volume, self.hostname, last_dir, dirname)
            args.append(f"--link-dest={link_dest_path}")

        if update:
            args.append("--del")

        args.extend(["--", f"{self.hostname}:{bind_mount}/", f"{target_path}/"])

        return args


def get_last_dir(dir_name: str) -> str | None:
    """Return the last (sorted) directory in dir_name"""
    dirs = []
    for _dir in os.listdir(dir_name):
        fullpath = f"{dir_name}/{_dir}"
        if (not ospath.isdir(fullpath)) or ospath.islink(fullpath):
            continue
        dirs.append(_dir)

    if not dirs:
        return None
    dirs.sort()
    return dirs[-1]


def get_timestamp(time: datetime.datetime | None = None) -> str:
    """Return the timestamp (directory name) for the given time"""
    if not time:
        time = datetime.datetime.now()
    return time.strftime("%Y%m%d.%H%M")


def main() -> None:
    """Main program entry point."""
    args = parse_args()
    hosts = args.host
    end = ""

    for hostname in hosts:
        print("", end=end)
        end = "\n"
        print(hostname)
        client = BackupClient(hostname, args.volume, user=args.user)
        client.pre_backup()
        client.backup(
            jobs=args.jobs, link_to=args.link, random=args.random, update=args.update
        )
        client.post_backup()

    print("done")


if __name__ == "__main__":
    main()
