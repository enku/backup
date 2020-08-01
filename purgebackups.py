#!/usr/bin/env python3
import datetime
import os
import re
import shutil
import sys

BACKUP_VOL = "/var/backup"
BACKUP_REGEX = re.compile(r"[1-9]\d+\.\d{4}")
os.environ["TZ"] = "UTC"


def get_all_backups(backup_dir):
    """Return a list of all backup directories in backup_dir."""
    backups = []
    files = os.listdir(backup_dir)
    for f in files:
        if BACKUP_REGEX.match(f):
            backups.append(f)
    return backups


def backups_to_dt_list(backups):
    """
    Given a list of strings in backup format, return the correspinding
    list of datetime objects.
    """
    dates = []
    for backup in backups:
        backup_date = datetime.datetime.strptime(backup, "%Y%m%d.%H%M")
        dates.append(backup_date)
    return dates


def dt_list_to_backups(dt_list):
    """
    Given the list of datetimes, return a list of strings in backup format.

    This does the exact opposit of backups_to_dt_list()
    """
    backups = []
    for dt in dt_list:
        backups.append(dt.strftime("%Y%m%d.%H%M"))
    return backups


def filter_range(dt_list, start, end):
    """
    Given a list of datetimes, return a subset of dt_list between start
    and end (inclusive).
    """
    lst = []
    for dt in dt_list:
        if start <= dt <= end:
            lst.append(dt)
    return lst


def append_latest(dt_list, lst):
    """
    If dt_list is a non-empty list of datetime objects, take the one with
    the later datetime and append it to lst.  If lst is empty, do nothing.

    Return the datetime object appended or None.
    """
    if dt_list:
        latest = sorted(dt_list)[-1]
        lst.append(latest)
        return latest
    return None


def last_day_of_month(dt):
    """
    Return the last day (hour minute and second) of the month of
    provided datetime object.
    """
    year = dt.year
    month = dt.month
    next_month = dt.replace(
        day=1,
        month=month + 1 if month < 12 else 1,
        year=year if month < 12 else year + 1,
        hour=23,
        minute=59,
        second=59,
        microsecond=0,
    )
    return next_month - datetime.timedelta(days=1)


def yesterday_plus(dt_list):
    """Return every datetime object in dt_list from yesterday up."""
    lst = []
    today = datetime.datetime.now()
    yesterday = today - datetime.timedelta(hours=24)
    yesterday = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
    for dt in dt_list:
        if dt >= yesterday:
            lst.append(dt)
    return lst


def one_per_day_last_week(dt_list):
    """
    Given the list of datetime objects, return one dt for every day
    within the past week.
    """
    lst = []
    today = datetime.datetime.now()
    last_week = today - datetime.timedelta(days=7)
    last_week = last_week.replace(hour=0, minute=0, second=0, microsecond=0)

    for i in range(7):
        day = last_week + datetime.timedelta(days=i)
        end_of_day = day.replace(hour=23, minute=59, second=59)
        day_list = filter_range(dt_list, day, end_of_day)
        append_latest(day_list, lst)
    return lst


def one_per_week_last_month(dt_list):
    """
    Given a list of datetime objects, return a the subset of them
    comprising of at most one from each week last month. If multiple
    datetimes fit within the week, use the later.
    """
    lst = []
    today = datetime.datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
    last_month = today - datetime.timedelta(days=31)
    start_of_month = last_month.replace(day=1)
    end_of_month = today.replace(day=1) - datetime.timedelta(days=1)

    start_day = start_of_month
    while start_day <= end_of_month:
        weekday = start_day.weekday()
        try:
            end_of_week = start_day.replace(day=6 - weekday + start_day.day)
        except ValueError:
            end_of_week = end_of_month
        end_of_week = datetime.datetime(
            year=end_of_week.year,
            month=end_of_week.month,
            day=end_of_week.day,
            hour=23,
            minute=59,
            second=59,
        )
        weeks_backups = filter_range(dt_list, start_day, end_of_week)
        append_latest(weeks_backups, lst)
        start_day = start_day + datetime.timedelta(days=7)

    return lst


def one_per_month_last_year(dt_list):
    """
    Given the list of datetime objects, return a list of dt_list which
    include a maximum of one for each month of the past year. If
    multiple datetimes fit the criteria for a month, use the latest.
    """
    lst = []
    now = datetime.datetime.now()
    last_year = now - datetime.timedelta(days=365)
    last_year = last_year.replace(hour=0, minute=0, second=0, microsecond=0)

    dt = last_year
    while dt <= now:
        start_of_month = dt.replace(month=dt.month, day=1, hour=0, minute=0, second=0)
        end_of_month = last_day_of_month(start_of_month)
        months_dts = filter_range(dt_list, start_of_month, end_of_month)
        append_latest(months_dts, lst)
        dt = end_of_month + datetime.timedelta(seconds=1)

    return lst


def one_per_year(dt_list):
    """
    Given a list of datetimes, return a subset consisting of at most
    one datetime per year. If multiple datetimes satisfy a given year,
    use the later.
    """
    lst = []
    years = []
    dt_revsort = sorted(dt_list, reverse=True)
    for dt in dt_revsort:
        year = dt.year
        if year not in years:
            lst.append(dt)
            years.append(year)
    return lst


def main():
    hostname = sys.argv[1]
    backup_dir = os.path.join("/var/backup", hostname)

    all_backups = get_all_backups(backup_dir)
    dt_list = backups_to_dt_list(all_backups)
    keep = set()
    keep.update(yesterday_plus(dt_list))
    keep.update(one_per_day_last_week(dt_list))
    keep.update(one_per_week_last_month(dt_list))
    keep.update(one_per_month_last_year(dt_list))
    keep.update(one_per_year(dt_list))

    to_remove = sorted(set(dt_list) - keep)

    print(
        "Want to remove {0} out of {1} backups".format(len(to_remove), len(all_backups))
    )

    keep = list(keep)
    keep.sort()
    print("Keeping: ")
    for backup in dt_list_to_backups(keep):
        print("    {0}".format(backup))

    if to_remove:
        ok = raw_input("\nOK? [y/N] ")
        if ok.upper() == "Y":
            remove_backups(backup_dir, dt_list_to_backups(to_remove))
        else:
            print("Fair enough.")
    else:
        print("Nothing to purge.")


def remove_backups(backup_dir, to_remove):
    for backup in to_remove:
        dirname = os.path.join(backup_dir, backup)
        print("Removing {}".format(dirname), end=" ")
        sys.stdout.flush()
        shutil.rmtree(dirname)
        print("done")


if __name__ == "__main__":
    main()
