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

os.environ['TZ'] = 'UTC'


def parse_args():
    """Return the cmdline arguments parsed (or fail)."""
    parser = argparse.ArgumentParser(
        description='Back up a server')
    parser.add_argument('-u', '--update', action='store_true', default=False,
                        help='Update last backup instead of creating a new one.')
    parser.add_argument('-l', '--link', default=None,
                        help='Create a symlink of this backup to LINK')
    parser.add_argument('hostname', type=str)
    return parser.parse_args()


class BackupClient(object):
    def __init__(self, hostname):
        self.hostname = hostname
        self.host_dir = '%s/%s' % (BACKUP_VOL, hostname)
        self.filesystems = self.get_filesystems()
        self.backup_vol = '/mnt/backup'

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
        return

    def backup(self, update=False, link_to=None):
        last_dir = get_last_dir(self.host_dir)

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

        for _dir in self.filesystems:
            status = self.ssh(('mount', '--bind', _dir, self.backup_vol))
            if status != 0:
                sys.exit(status)

            dirname = os.path.basename(_dir)
            if dirname == '':
                dirname = 'root'

            target_path = os.path.join(BACKUP_VOL, self.hostname, target, dirname)

            if not target_path.startswith(BACKUP_VOL):
                sys.stderr.write(
                    'Refusing to backup outside of {0}: {1}\n'.format(
                        BACKUP_VOL, target_path))
                self.post_backup()
                sys.exit(1)

            if last_dir:
                link_dest_path = os.path.join(BACKUP_VOL,
                                              self.hostname,
                                              last_dir,
                                              dirname)
            print()
            print(dirname)
            if update:
                status = print_time(call, ('rsync',) + RSYNC_ARGS +
                                    ('--del',
                                    '--link-dest=%s' % link_dest_path,
                                    '%s:%s/' % (self.hostname, self.backup_vol),
                                    '%s/' % target_path
                                     ))
            elif not last_dir:
                status = print_time(call, ('rsync',) + RSYNC_ARGS +
                                    ('%s:%s/' % (self.hostname, self.backup_vol),
                                    '%s/' % target_path
                                     ))
            else:
                status = print_time(call, ('rsync',) + RSYNC_ARGS +
                                    (
                                    '--link-dest=%s' % link_dest_path,
                                    '%s:%s/' % (self.hostname, self.backup_vol),
                                    '%s/' % target_path
                                    ))
            self.ssh(('umount', self.backup_vol))
            try:
                print(RSYNC_STATUS[status])
            except KeyError:
                self.post_backup()
                sys.exit(status)

        timestamp = get_timestamp()

        os.rename(
            '%s/%s/%s' % (BACKUP_VOL, self.hostname, target),
            '%s/%s/%s' % (BACKUP_VOL, self.hostname, timestamp)
        )

        if link_to:
            os.symlink(timestamp, '%s/%s/%s' % (BACKUP_VOL, self.hostname, link_to))

        latest_link = '{backup_vol}/{hostname}/latest'.format(
            backup_vol=BACKUP_VOL, hostname=self.hostname)

        if os.path.exists(latest_link) or os.path.islink(latest_link):
            os.unlink(latest_link)
        os.symlink(timestamp, latest_link)

    def post_backup(self):
        return


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

    client = BackupClient(hostname)
    client.pre_backup()
    client.backup(args.update, args.link)
    client.post_backup()

    print('done')


if __name__ == '__main__':
    main()
