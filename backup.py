#!/usr/bin/env python3
# -*- encoding: utf-8 -*-
"""
Backups, revisited
"""
import argparse
import datetime
import os
from subprocess import call
import sys

ENV = {'TZ': 'UTC'}
SOURCE_VOL = '/mnt/gentoo'
BACKUP_VOL = '/var/backup'
RSYNC_ARGS = (
    '--archive',
    '--compress',
    '--human-readable',
    '--one-file-system',
    '--sparse',
    '--stats',
    '-F',
)

DIRS = ('boot', 'root', 'gentoo')

os.environ['TZ'] = 'UTC'


def parse_args():
    """Return the cmdline arguments parsed (or fail)."""
    parser = argparse.ArgumentParser(
        description='Back up a server')
    parser.add_argument('-u', '--update', action='store_true', default=False,
                        help='Update last backup instead of creating a new one.')
    parser.add_argument('hostname', type=str)
    return parser.parse_args()


def ssh(hostname, cmd):
    """Like subprocess.Popen: Execute cmd but using ssh on «hostname»"""
    new_cmd = ['ssh', hostname, ' '.join(cmd)]
    status = call(new_cmd)
    return status


def pre_backup(hostname):
    status = ssh(hostname, ('mountpoint', '-q', '/boot'))
    if status != 0:
        ssh(hostname, ('mount', '/boot'))
    status = ssh(
        hostname, ('mount', '--bind', '/boot', '%s/boot' % SOURCE_VOL))
    if status != 0:
        sys.exit(status)
    status = ssh(hostname, ('mountpoint', '-q', '%s/root' % SOURCE_VOL))
    if status != 0:
        status = ssh(
            hostname, ('mount', '--bind', '/', '%s/root' % SOURCE_VOL))
    if status != 0:
        sys.exit(status)


def post_backup(hostname):
    ssh(hostname, ('umount', '%s/boot' % SOURCE_VOL))
    ssh(hostname, ('umount', '%s/root' % SOURCE_VOL))
    ssh(hostname, ('umount', '/boot'))


def get_last_dir(dirname):
    """Return the last (sorted) directory in «dirname»"""
    dirs = [i for i in os.listdir(
        dirname) if os.path.isdir('%s/%s' % (dirname, i))]
    if not dirs:
        return None
    dirs.sort()
    return dirs[-1]


def print_time(func, *args, **kwargs):
    """execute func(*args, **kwargs) and return the time it took to run.

    returns the return value of func().
    """
    if func is call:
        print(' '.join(args[0]))
    start = datetime.datetime.now()
    status = func(*args, **kwargs)
    stop = datetime.datetime.now()
    print(stop - start)
    return status


def get_timestamp(time=None):
    if not time:
        time = datetime.datetime.now()
    return time.strftime('%Y%m%d.%H%M')


def main():
    """Main program entry point."""
    args = parse_args()
    hostname = args.hostname
    hostdir = os.path.join(BACKUP_VOL, hostname)

    if not os.path.isdir(hostdir):
        os.mkdir(hostdir)

    last_dir = get_last_dir(hostdir)

    pre_backup(hostname)
    if args.update:
        if not last_dir:
            sys.stderr.write(
                '--update specified, but no directory to update from.\n')
            sys.exit(1)
        target = last_dir
    else:
        target = '0'
        full_target = '%s/%s/%s' % (BACKUP_VOL, hostname, target)
        if os.path.isdir(full_target):
            sys.stderr.write('%s already exists.  Abort.\n' % target)
            sys.exit(1)
        os.mkdir(full_target)

    for _dir in DIRS:
        source_path = os.path.join(SOURCE_VOL, _dir)
        target_path = os.path.join(BACKUP_VOL, hostname, target, _dir)
        if last_dir:
            link_dest_path = os.path.join(BACKUP_VOL, hostname, last_dir, _dir)

        print()
        print(_dir)
        if args.update:
            status = print_time(call, ('rsync',) + RSYNC_ARGS +
                                ('--del',
                                 '--link-dest=%s' % link_dest_path,
                                 '%s:%s/' % (hostname, source_path),
                                 '%s/' % target_path
                                 ))
        elif not last_dir:
            status = print_time(call, ('rsync',) + RSYNC_ARGS +
                                ('%s:%s/' % (hostname, source_path),
                                 '%s/' % target_path
                                 ))
        else:
            status = print_time(call, ('rsync',) + RSYNC_ARGS +
                                (
                                '--link-dest=%s' % link_dest_path,
                                '%s:%s/' % (hostname, source_path),
                                '%s/' % target_path
                                ))

        if status != 0:
            sys.exit(status)
    post_backup(hostname)
    timestamp = get_timestamp()
    os.rename(
        '%s/%s/%s' % (BACKUP_VOL, hostname, target),
        '%s/%s/%s' % (BACKUP_VOL, hostname, timestamp)
    )
    print('done')


if __name__ == '__main__':
    main()
