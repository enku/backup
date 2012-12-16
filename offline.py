#!/usr/bin/env python3

import argparse
import os
import subprocess
import sys


BACKUP_VOL = '/var/backup'
RSYNC_ARGS = (
    '--acls',
    '--archive',
    '--hard-links',
    '--numeric-ids',
    '--one-file-system',
    '--quiet',
    '--sparse',
    '--whole-file',
    '--xattrs',
)


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument('--source', '-s', default=BACKUP_VOL)
    p.add_argument('dest')

    return p.parse_args()


def get_latest_dirs(source):
    dirs = []
    for fname in os.listdir(source):
        latest = os.path.join(source, fname, 'latest')
        if not os.path.exists(latest):
            continue
        if not os.path.islink(latest):
            continue

        dest = os.readlink(latest)
        dirs.append((fname, os.path.join(source, fname, dest)))
    return dirs


def main():
    args = parse_args()

    for host, f in get_latest_dirs(args.source):
        timestamp = os.path.basename(f)
        fullsource = os.path.join(args.source, f)
        fulldest = os.path.join(args.dest, host, timestamp)
        if os.path.exists(fulldest):
            print('{} already exists.  Skipping.'.format(fulldest))
            continue
        os.makedirs(fulldest)
        command = ('rsync',) + RSYNC_ARGS + ('{}/'.format(fullsource),
                                             '{}/'.format(fulldest))
        print(' '.join(command))
        status = subprocess.call(command)
        if status != 0:
            return

if __name__ == '__main__':
    try:
        main()
    except Exception as error:
        print(error)
        sys.exit(1)
    sys.exit(0)
