#!/usr/bin/env python3
"""Purge old backups for the given host from the backup volume"""
import argparse
import datetime as dt
import os
import re
import shutil
import sys
from typing import List, Optional, Set

BACKUP_VOL = "/var/backup"
BACKUP_REGEX = re.compile(r"[1-9]\d+\.\d{4}")
os.environ["TZ"] = "UTC"

DTList = List[dt.datetime]


def parse_args() -> argparse.Namespace:
    """Return the command line arguments parsed (or fail)."""
    parser = argparse.ArgumentParser(description="Purge old backups")
    parser.add_argument(
        "-v",
        "--volume",
        default=BACKUP_VOL,
        help=f"Backup volume (default: {BACKUP_VOL})",
    )
    parser.add_argument(
        "-y",
        "--yes",
        help='Assume "yes" to prompts',
        action="store_true",
        default=False,
    )
    parser.add_argument("host", help="The backup host to purge")

    return parser.parse_args()


def get_all_backups(backup_dir: str) -> List[str]:
    """Return a list of all backup directories in backup_dir."""
    backups = []
    filenames = os.listdir(backup_dir)
    for filename in filenames:
        if BACKUP_REGEX.match(filename):
            backups.append(filename)
    return backups


def backups_to_dt_list(backups: List[str]) -> DTList:
    """
    Given a list of strings in backup format, return the corresponding
    list of datetime objects.
    """
    dates = []
    for backup in backups:
        backup_date = dt.datetime.strptime(backup, "%Y%m%d.%H%M")
        dates.append(backup_date)
    return dates


def dt_list_to_backups(dt_list: DTList) -> List[str]:
    """
    Given the list of datetimes, return a list of strings in backup format.

    This does the exact opposite of backups_to_dt_list()
    """
    backups = []
    for datetime in dt_list:
        backups.append(datetime.strftime("%Y%m%d.%H%M"))
    return backups


def filter_range(dt_list: DTList, start: dt.datetime, end: dt.datetime) -> DTList:
    """
    Given a list of datetimes, return a subset of dt_list between start
    and end (inclusive).
    """
    lst = []
    for datetime in dt_list:
        if start <= datetime <= end:
            lst.append(datetime)
    return lst


def append_latest(dt_list: DTList, lst: DTList) -> None:
    """
    If dt_list is a non-empty list of datetime objects, take the one with the later
    datetime and append it to the list.  If the list is empty, do nothing.
    """
    if dt_list:
        latest = sorted(dt_list)[-1]
        lst.append(latest)


def last_day_of_month(datetime: dt.datetime) -> dt.datetime:
    """
    Return the last day (hour minute and second) of the month of
    provided datetime object.
    """
    year = datetime.year
    month = datetime.month
    next_month = datetime.replace(
        day=1,
        month=month + 1 if month < 12 else 1,
        year=year if month < 12 else year + 1,
        hour=23,
        minute=59,
        second=59,
        microsecond=0,
    )
    return next_month - dt.timedelta(days=1)


def yesterday_plus(dt_list: DTList) -> DTList:
    """Return every datetime object in dt_list from yesterday up."""
    lst: DTList = []
    today = dt.datetime.now()
    yesterday = today - dt.timedelta(hours=24)
    yesterday = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
    for datetime in dt_list:
        if datetime >= yesterday:
            lst.append(datetime)
    return lst


def one_per_day_last_week(dt_list: DTList) -> DTList:
    """
    Given the list of datetime objects, return one datetime for every day
    within the past week.
    """
    lst: DTList = []
    today = dt.datetime.now()
    last_week = today - dt.timedelta(days=7)
    last_week = last_week.replace(hour=0, minute=0, second=0, microsecond=0)

    for i in range(7):
        day = last_week + dt.timedelta(days=i)
        end_of_day = day.replace(hour=23, minute=59, second=59)
        day_list = filter_range(dt_list, day, end_of_day)
        append_latest(day_list, lst)
    return lst


def one_per_week_last_month(dt_list: DTList) -> DTList:
    """
    Given a list of datetime objects, return a the subset of them
    comprising of at most one from each week last month. If multiple
    datetimes fit within the week, use the later.
    """
    lst: DTList = []
    today = dt.datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
    last_month = today - dt.timedelta(days=31)
    start_of_month = last_month.replace(day=1)
    end_of_month = today.replace(day=1) - dt.timedelta(days=1)

    start_day = start_of_month
    while start_day <= end_of_month:
        weekday = start_day.weekday()
        try:
            end_of_week = start_day.replace(day=6 - weekday + start_day.day)
        except ValueError:
            end_of_week = end_of_month
        end_of_week = dt.datetime(
            year=end_of_week.year,
            month=end_of_week.month,
            day=end_of_week.day,
            hour=23,
            minute=59,
            second=59,
        )
        weeks_backups = filter_range(dt_list, start_day, end_of_week)
        append_latest(weeks_backups, lst)
        start_day = start_day + dt.timedelta(days=7)

    return lst


def one_per_month_last_year(dt_list: DTList) -> DTList:
    """
    Given the list of datetime objects, return a list of dt_list which
    include a maximum of one for each month of the past year. If
    multiple datetimes fit the criteria for a month, use the latest.
    """
    lst: DTList = []
    now = dt.datetime.now()
    last_year = now - dt.timedelta(days=365)
    last_year = last_year.replace(hour=0, minute=0, second=0, microsecond=0)

    datetime = last_year
    while datetime <= now:
        start_of_month = datetime.replace(
            month=datetime.month, day=1, hour=0, minute=0, second=0
        )
        end_of_month = last_day_of_month(start_of_month)
        months_dts = filter_range(dt_list, start_of_month, end_of_month)
        append_latest(months_dts, lst)
        datetime = end_of_month + dt.timedelta(seconds=1)

    return lst


def one_per_year(dt_list: DTList) -> DTList:
    """
    Given a list of datetimes, return a subset consisting of at most
    one datetime per year. If multiple datetimes satisfy a given year,
    use the later.
    """
    lst = []
    years = []
    dt_revsort = sorted(dt_list, reverse=True)
    for datetime in dt_revsort:
        year = datetime.year
        if year not in years:
            lst.append(datetime)
            years.append(year)
    return lst


def main() -> None:
    """Main program entry point"""
    args = parse_args()
    backup_dir = os.path.join(args.volume, args.host)

    all_backups = get_all_backups(backup_dir)
    dt_list = backups_to_dt_list(all_backups)
    keep: Set[dt.datetime] = set()
    keep.update(yesterday_plus(dt_list))
    keep.update(one_per_day_last_week(dt_list))
    keep.update(one_per_week_last_month(dt_list))
    keep.update(one_per_month_last_year(dt_list))
    keep.update(one_per_year(dt_list))

    to_remove = sorted(set(dt_list) - keep)

    print(f"Want to remove {len(to_remove)} out of {len(all_backups)} backups")

    keep_lst: DTList = list(keep)
    keep_lst.sort()
    print_list(keep_lst, to_remove)

    if to_remove:
        confirm_removal = "Y" if args.yes else input("\nOK? [y/N] ")
        if confirm_removal.upper() == "Y":
            remove_backups(backup_dir, dt_list_to_backups(to_remove))
        else:
            print("Fair enough.")
    else:
        print("Nothing to purge.")


def remove_backups(backup_dir: str, to_remove: List[str]) -> None:
    """Remove to_remove directories from backup_dir"""
    for backup in to_remove:
        dirname = os.path.join(backup_dir, backup)
        print(f"Removing {dirname}", end=" ")
        sys.stdout.flush()
        shutil.rmtree(dirname)
        print("done")


def print_list(to_keep: DTList, to_remove: DTList) -> None:
    """Print table of backups to keep and remove"""
    to_keep_col: List[Optional[str]] = [*dt_list_to_backups(to_keep)]
    to_remove_col: List[Optional[str]] = [*dt_list_to_backups(to_remove)]
    if len(to_keep_col) > len(to_remove_col):
        to_remove_col = to_remove_col + [None] * (len(to_keep_col) - len(to_remove_col))
    elif len(to_remove_col) > len(to_keep_col):
        to_keep_col = to_keep_col + [None] * (len(to_remove_col) - len(to_keep_col))
    print("┌─────────────┬─────────────┐")
    print("│ Keep        │      Remove │")
    print("├─────────────┼─────────────┤")
    for keep, remove in zip(to_keep_col, to_remove_col):
        print(f"│{keep or ' ' * 13}│{remove or ' ' * 13}│")
    print("└─────────────┴─────────────┘")


if __name__ == "__main__":
    main()
