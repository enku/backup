#!/usr/bin/python3
# -*- encoding: utf-8 -*-
"""
Backups, revisited
"""
# we don't need this but it shuts up vim/pylint
from __future__ import print_function

import argparse
import concurrent.futures
import datetime
import os
from subprocess import call, Popen, PIPE
import threading
import queue
import sys

ENV = {'TZ': 'UTC'}
BACKUP_VOL = '/var/backup'
RSYNC_ARGS = (
    '--acls',
    '--archive',
    '--compress',
    '--human-readable',
    '--numeric-ids',
    '--one-file-system',
    '--quiet',
    '--sparse',
    '--stats',
    '--xattrs',
    '-F',
)

RSYNC_STATUS = {
    0: 'Success',
    23: 'Partial transfer due to error',
    24: 'Partial transfer due to vanished source files',
}

ESC = '\x1b['
NORMAL = ESC + '00m'
FAIL = ESC + '01;31m' + 'F' + NORMAL
RUNNNG = ESC + '01;34m' + 'R' + NORMAL
COMPLETE = ESC + '01;32m' + 'C' + NORMAL
WAITING = ESC + '01;36m' + 'W' + NORMAL

os.environ['TZ'] = 'UTC'
format = str.format


class OutputThread(threading.Thread):
    queue = queue.Queue()
    daemon = True

    def run(self):
        while True:
            args, kwargs = self.queue.get()
            sprint(*args, **kwargs)
            self.queue.task_done()

    def print(self, *args, **kwargs):
        self.queue.put((args, kwargs))


def parse_args():
    """Return the cmdline arguments parsed (or fail)."""
    parser = argparse.ArgumentParser(description='Back up a system')
    parser.add_argument('-u', '--update', action='store_true', default=False,
                        help='Update last backup instead of creating a new '
                        'one.')
    parser.add_argument('-l', '--link', default=None,
                        help='Create a symlink of this backup to LINK')
    parser.add_argument('-j', '--jobs', type=int, default=3,
                        help='Number of parallel jobs')
    parser.add_argument('host', type=str, nargs='+')
    return parser.parse_args()


def sprint(*args, **kwargs):
    """
    Print and flush stdout.
    """
    print(*args, **kwargs)
    sys.stdout.flush()


class BackupClient(object):

    def __init__(self, hostname):
        self.hostname = hostname
        self.host_dir = '%s/%s' % (BACKUP_VOL, hostname)
        self.filesystems = self.get_filesystems()
        self.stats = dict([(i, WAITING) for i in self.filesystems])
        self.output = OutputThread()
        self.output.start()

        if not os.path.isdir(self.host_dir):
            os.mkdir(self.host_dir)

    def get_filesystems(self):
        filesystems = []
        filename = '%s/filesystems' % self.host_dir
        fp = open(filename)
        for line in fp:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            filesystems.append(line)
        return filesystems

    def ssh(self, cmd):
        """Like subprocess.Popen: Execute cmd but using ssh on the client."""
        new_cmd = ['ssh', self.hostname, ' '.join(cmd)]
        status = call(new_cmd)
        return status

    def pre_backup(self):
        popen = Popen(
            ('ssh', self.hostname, 'mktemp', '-d', '--suffix=.backup'),
            stdout=PIPE
        )
        self.backup_vol = popen.stdout.read().rstrip().decode('utf-8')
        return popen.wait()

    def backup_filesystem(self, filesystem, target, last_dir, update):
        self.print_stats((filesystem, RUNNNG))
        dirname = os.path.basename(filesystem) or 'root'
        bind_mount = os.path.join(self.backup_vol, dirname)

        self.ssh(('mkdir', '-p', bind_mount))
        status = self.ssh(('mount', '--bind', filesystem, bind_mount))
        if status != 0:
            sys.exit(status)

        target_path = os.path.join(BACKUP_VOL, self.hostname, target, dirname)

        if not target_path.startswith(BACKUP_VOL):
            sys.stderr.write(format('Refusing to backup outside of {0}: {1}\n',
                             BACKUP_VOL, target_path))
            self.ssh(('rmdir', bind_mount))
            sys.exit(1)

        if last_dir:
            link_dest_path = os.path.join(
                BACKUP_VOL, self.hostname, last_dir, dirname)

        args = ['rsync']
        args.extend(RSYNC_ARGS)
        if update:
            args.append('--del')
        if last_dir:
            args.append('--link-dest=%s' % link_dest_path)

        args.append('--')
        args.append('%s:%s/' % (self.hostname, bind_mount))
        args.append('%s/' % target_path)
        status = call(args)
        self.ssh(('umount', bind_mount))
        self.ssh(('rmdir', bind_mount))

        self.print_stats((filesystem, COMPLETE if status == 0 else FAIL))
        sys.exit(status)

    def backup(self, update=False, link_to=None, jobs=3):
        last_dir = get_last_dir(self.host_dir)
        target = self.get_target(update, last_dir)
        futures = []

        with concurrent.futures.ThreadPoolExecutor(
                max_workers=jobs) as executor:
            self.output.print('')
            for _dir in self.filesystems:
                futures.append(executor.submit(self.backup_filesystem, _dir,
                                               target, last_dir, update))
            concurrent.futures.wait(futures)

        timestamp = get_timestamp()
        self.output.print('')

        os.rename('%s/%s/%s' % (BACKUP_VOL, self.hostname, target),
                  '%s/%s/%s' % (BACKUP_VOL, self.hostname, timestamp))

        if link_to:
            os.symlink(timestamp, '%s/%s/%s' % (BACKUP_VOL, self.hostname,
                                                link_to))

        latest_link = format('{backup_vol}/{hostname}/latest',
                             backup_vol=BACKUP_VOL, hostname=self.hostname)

        if os.path.exists(latest_link) or os.path.islink(latest_link):
            os.unlink(latest_link)
        os.symlink(timestamp, latest_link)

    def get_target(self, update, last_dir):
        """
        Return (and create if neccessary) the rsync target based on the
        options. exit() and print message to stderr if there are issues.

        update: bool: whether this is an "update" backup.

        last_dir: str: The directory of the previous backup or None.
            If update == True, then last_dir must not be None

        """
        if update:
            if not last_dir:
                sys.stderr.write(
                    '--update specified, but no directory to update from.\n')
                sys.exit(1)
            target = last_dir
        else:
            target = '0'
            full_target = '%s/%s/%s' % (BACKUP_VOL, self.hostname, target)
            if os.path.isdir(full_target):
                sys.stderr.write('%s already exists.  Abort.\n' % target)
                sys.exit(1)
            os.mkdir(full_target)

        return target

    def print_stats(self, update=None):
        if update:
            self.stats[update[0]] = update[1]
        self.output.print('\r', end='')
        for filesystem in self.filesystems:
            self.output.print(
                format('{0}:{1}', os.path.basename(filesystem) or 'root',
                       self.stats[filesystem]), end=' ')

    def post_backup(self):
        return self.ssh(('rmdir', self.backup_vol))


def get_last_dir(dirname):
    """Return the last (sorted) directory in «dirname»"""
    dirs = []
    for _dir in os.listdir(dirname):
        fullpath = '%s/%s' % (dirname, _dir)
        if (not os.path.isdir(fullpath)) or os.path.islink(fullpath):
            continue
        dirs.append(_dir)

    if not dirs:
        return None
    dirs.sort()
    return dirs[-1]


def get_timestamp(time=None):
    if not time:
        time = datetime.datetime.now()
    return time.strftime('%Y%m%d.%H%M')


def main():
    """Main program entry point."""
    args = parse_args()
    hosts = args.host

    for hostname in hosts:
        try:
            client = BackupClient(hostname)
            client.pre_backup()
            client.backup(args.update, args.link, args.jobs)
            client.post_backup()
        except Exception:
            break

    print('done')


if __name__ == '__main__':
    main()
