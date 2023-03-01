# %%
## Running Imports ##

if __name__ != "__main__":
    print(f"Importing {__name__}")

import datetime
import pandas as pd
import sys
from time import sleep

# append parent
sys.path.append("..")

from utils.config_utils import (
    home_dir,
    file_dir,
    parent_dir,
    grandparent_dir,
    great_grandparent_dir,
    gdrive_path,
    gdrive_path_for_shared,
    data_dir,
)

sys.path.append(file_dir)
sys.path.append(parent_dir)
sys.path.append(grandparent_dir)

from utils.google_tools import gc, WriteToSheets, get_book, get_book_sheet

from utils.display_tools import print_logger, pprint_df, pprint_ls


# %%
## Define Functions ##


def get_datetime_format_string(format):
    """
    Returns a datetime format string from a format string that is more readable
    :param format: string options are:
        "%Y%m%d%H%M%S" or "number"
        "%Y%m%d" or "YYYYMMDD"
        "%H%M%S" or "HHMMSS" or "time_number"
        "%Y-%m-%d %H:%M:%S" or "readable"
        "%Y-%m-%d" or "YYYY-MM-DD"
        "%H:%M" or "hour_mins"
        "%A" or "Weekday"
    :return: string
    """
    if format == "%Y%m%d%H%M%S" or format == "number":
        return "%Y%m%d%H%M%S"
    elif format == "%Y%m%d" or format == "YYYYMMDD":
        return "%Y%m%d"
    elif format == "%H%M%S" or format == "HHMMSS" or format == "time_number":
        return "%H%M%S"
    elif format == "%Y-%m-%d %H:%M:%S" or format == "readable":
        return "%Y-%m-%d %H:%M:%S"
    elif format == "%Y-%m-%d" or format == "YYYY-MM-DD":
        return "%Y-%m-%d"
    elif format == "%H:%M" or format == "hour_mins":
        return "%H:%M"
    elif format == "%A" or format == "Weekday":
        return "%A"
    else:
        print("Invalid format string")
        return format


def get_current_datetime(format="%Y%m%d%H%M%S"):
    """
    Returns the current datetime in a format string
    :param format: string options are:
        "%Y%m%d%H%M%S" or "number"
        "%Y%m%d" or "YYYYMMDD"
        "%H%M%S" or "HHMMSS" or "time_number"
        "%Y-%m-%d %H:%M:%S" or "readable"
        "%Y-%m-%d" or "YYYY-MM-DD"
        "%H:%M" or "hour_mins"
        "%A" or "Weekday"
    :return: datetime object in format of string passed in
    """
    return datetime.datetime.now().strftime(get_datetime_format_string(format))


# %%
## Define Variables ##

currentDT = get_current_datetime(get_datetime_format_string("number"))
current_date_time_readable = get_current_datetime(
    get_datetime_format_string("readable")
)


# %%
## Get Date Data ##

df_days = get_book_sheet("Weeks", "Days").get_as_df(start="A1", end="K")
df_weeks = get_book_sheet("Weeks", "ImportableWeeks").get_as_df(start="A1", end="J")
df_scm_weeks = get_book_sheet("Weeks", "SCM_Week_Days").get_as_df()


# %%
## Date Lists ##

all_days_list = df_days["dashed_pad_desc"].tolist()
ls_days_slashed_no_pad = df_days["slashed_nopad"].tolist()
all_days_list_dashed_desc = df_days["dashed_pad_desc"].tolist()
all_weeks_list = df_weeks["WeekString"].to_list()


# %%
## Imports to Dictionary Maps ##

# from slashed_pad
dict_slashed_pad_date = dict(zip(df_days["slashed_pad"], df_days["WeekString"]))
dict_slashed_pad_to_dashed_pad_desc = dict(
    zip(df_days["slashed_pad"], df_days["dashed_pad_desc"])
)
dict_slashed_pad_to_slashed_nopad = dict(
    zip(df_days["slashed_pad"], df_days["slashed_nopad"])
)

# from slashed_nopad
dict_slashed_nopad_to_dashed_pad_desc = dict(
    zip(df_days["slashed_nopad"], df_days["dashed_pad_desc"])
)
dict_slashed_nopad_to_weekdaynumtext = dict(
    zip(df_days["slashed_nopad"], df_days["WeekDayNumDashName"])
)
dict_slashed_nopad_date = dict(zip(df_days["slashed_nopad"], df_days["WeekString"]))
dict_slashed_no_pad_to_slashed_pad = dict(
    zip(df_days["slashed_nopad"], df_days["slashed_pad"])
)

# from dict_slashed_pad_desc_date
dict_slashed_pad_desc_date = dict(
    zip(df_days["slashed_pad_desc"], df_days["WeekString"])
)

# from dashed_pad_desc
dict_dashed_pad_desc_to_weekdaynumtext = dict(
    zip(df_days["dashed_pad_desc"], df_days["WeekDayNumDashName"])
)
dict_dashed_pad_desc_to_scmweekdaynumtext = dict(
    zip(df_days["dashed_pad_desc"], df_days["SCMWeekDayNumDashName"])
)
dict_dashed_pad_desc_to_weekday = dict(
    zip(df_days["dashed_pad_desc"], df_days["WeekDayName"])
)
dict_dashed_pad_desc_to_slashed_pad = dict(
    zip(df_days["dashed_pad_desc"], df_days["slashed_pad"])
)
dict_dashed_pad_desc_date = dict(zip(df_days["dashed_pad_desc"], df_days["WeekString"]))
dict_dashed_pad_desc_to_slashed_nopad = dict(
    zip(df_days["dashed_pad_desc"], df_days["slashed_nopad"])
)

# from WeekString
dict_mon_roster_dates = dict(
    zip(df_weeks["WeekString"], df_weeks["RosterForWeekBegin"])
)
dict_mon_roster_dates_full_year = dict(
    zip(df_weeks["WeekString"], df_weeks["RosterForWeekBeginSlashedNoPadFullYear"])
)

# from RosterForWeekBegin
dict_mon_roster_dates_inverted = dict(
    zip(df_weeks["RosterForWeekBegin"], df_weeks["WeekString"])
)

# from Week_SCM_Weekday
dict_scm_weeks = dict(
    zip(df_scm_weeks["Week_SCM_Weekday"], df_scm_weeks["dashed_pad_desc"])
)


# %%
## Imports to Dictionary Maps ##

dict_days = {
    "Sunday": "7 - Sunday",
    "Monday": "1 - Monday",
    "Tuesday": "2 - Tuesday",
    "Wednesday": "3 - Wednesday",
    "Thursday": "4 - Thursday",
    "Friday": "5 - Friday",
    "Saturday": "6 - Saturday",
}

dict_day_sort_order = {
    "Sunday": 1,
    "Monday": 2,
    "Tuesday": 3,
    "Wednesday": 4,
    "Thursday": 5,
    "Friday": 6,
    "Saturday": 7,
}

dict_day_abbrev_to_day = {
    "Mon": "Monday",
    "Tue": "Tuesday",
    "Wed": "Wednesday",
    "Thu": "Thursday",
    "Fri": "Friday",
    "Sat": "Saturday",
    "Sun": "Sunday",
}


Year = get_book_sheet("LaborPlanRunner", "LaborPlans").get_value((1, 4))[:4]
WeekNum = get_book_sheet("LaborPlanRunner", "LaborPlans").get_value((1, 2))
WorkingWeek = str(Year) + "-" + "W" + str(WeekNum)

all_weeks_list = []
starting_year = 2018
starting_week = 1
for i in range(600):
    all_weeks_list.append(str(starting_year) + "-W" + str(starting_week).zfill(2))
    if starting_week == 52:
        starting_week = 1
        starting_year += 1
    else:
        starting_week += 1
df_week_list = pd.DataFrame(all_weeks_list, columns=["Week"])


# %%
## Main Functions ##


def week_span_to_week_list(base_week, num_weeks_back, num_weeks_forward):
    """
    Returns a list of weeks from base_week, num_weeks_back, num_weeks_forward
    :param base_week: The week to start from in the format 2022-W01
    :param num_weeks_back: The number of weeks to go back from base_week
    :param num_weeks_forward: The number of weeks to go forward from base_week
    :return: A list of weeks in the format 2022-W01
    """
    week_list = []

    for i in range(
        all_weeks_list.index(base_week) - num_weeks_back,
        all_weeks_list.index(base_week) + num_weeks_forward + 1,
    ):
        week_list.append(all_weeks_list[i])

    return week_list


def day_span_to_day_list(base_day, num_days_back, num_days_forward):
    """
    Returns a list of days from base_day, num_days_back, num_days_forward
    :param base_day: The day to start from in the format 2022-01-01
    :param num_days_back: The number of days to go back from base_day
    :param num_days_forward: The number of days to go forward from base_day
    :return: A list of days in the format 2022-01-01
    """
    day_list = []

    for i in range(
        all_days_list.index(base_day) - num_days_back,
        all_days_list.index(base_day) + num_days_forward + 1,
    ):
        day_list.append(all_days_list[i])

    return day_list


def day_range_to_day_list(start_day, end_day):
    """
    Returns a list of days from start_day to end_day
    :param start_day: The day to start from in the format 2022-01-01
    :param end_day: The day to end on in the format 2022-01-01
    :return: A list of days in the format 2022-01-01
    """
    day_list = []

    for i in range(
        all_days_list.index(start_day),
        all_days_list.index(end_day) + 1,
    ):
        day_list.append(all_days_list[i])

    return day_list


def day_span_to_day_list_no_pad(base_day, num_days_back, num_days_forward):
    """
    Returns a list of days from base_day, num_days_back, num_days_forward with the format 2022-1-1
    :param base_day: The day to start from in the format 2022-1-1
    :param num_days_back: The number of days to go back from base_day
    :param num_days_forward: The number of days to go forward from base_day
    :return: A list of days in the format 2022-1-1
    """
    day_list = []

    for i in range(
        ls_days_slashed_no_pad.index(base_day) - num_days_back,
        ls_days_slashed_no_pad.index(base_day) + num_days_forward + 1,
    ):
        day_list.append(ls_days_slashed_no_pad[i])

    return day_list


def getDiffWeek(base_week, num_weeks_diff):
    """
    Returns the week num_weeks_diff away from base_week
    :param base_week: The week to start from in the format 2022-W01
    :param num_weeks_diff: The number of weeks to go forward or backward from base_week
    :return: A week in the format 2022-W01
    """
    base_week_index = all_weeks_list.index(base_week)
    outputWeek = all_weeks_list[base_week_index + num_weeks_diff]
    return outputWeek


def getDiffDay(base_day, num_days_diff):
    """
    Returns the day num_days_diff away from base_day
    :param base_day: The day to start from in the format 2022-01-01
    :param num_days_diff: The number of days to go forward or backward from base_day
    :return: A day in the format 2022-01-01
    """
    base_day_index = all_days_list.index(base_day)
    outputDay = all_days_list[base_day_index + num_days_diff]
    return outputDay


def get_weeks_out_from_week(weekMade, weekRegards):
    """
    Returns the number of weeks between weekMade and weekRegards
    :param weekMade: The week to start from in the format 2022-W01
    :param weekRegards: The week to end on in the format 2022-W01
    :return: The number of weeks between weekMade and weekRegards
    """
    weeksOut = all_weeks_list.index(weekRegards) - all_weeks_list.index(weekMade)
    return weeksOut


def floatHourToTime(fh):
    """
    Converts a float hour to a time in the format (hours, minutes, seconds)
    :param fh: The float hour to convert
    :return: A time in the format (hours, minutes, seconds)
    """
    hours, hourSeconds = divmod(fh, 1)
    minutes, seconds = divmod(hourSeconds * 60, 1)
    return (
        int(hours),
        int(minutes),
        int(seconds * 60),
    )


def convert_week(convert_week):
    """
    Converts a week in the format 2242 (sanders format) to 2022-W42
    :param convert_week: The week to convert
    :return: The converted week in the format 2022-W42
    """
    WeekString = "20" + convert_week[0:2] + "-W" + convert_week[2:4]
    return WeekString


def fix_weeks(df):
    """
    Fixes the weeks in a dataframe from the format 2242 (sanders format) to 2022-W42
    :param df: The dataframe to fix
    :return: The fixed dataframe
    """
    df["WeekString"] = df["WeekString"].apply(convert_week)
    return df


def get_today_date(format_string):
    """
    Returns the current date in the format specified by format_string
    :param format_string: The format to return the date in currently only option is YYYY-MM-DD
    :return: The current date in the format specified by format_string
    """
    dict_formats = {"YYYY-MM-DD": "%Y-%m-%d"}
    today = datetime.date.today()
    return today.strftime(dict_formats[format_string])


def excel_date_to_datetime(excel_date):
    """
    Converts an excel date to a datetime
    :param excel_date: The excel date to convert
    :return: The converted datetime
    """
    dt = datetime.datetime.fromordinal(
        datetime.datetime(1900, 1, 1).toordinal() + int(excel_date) - 2
    )
    hour, minute, second = floatHourToTime(excel_date % 1)
    dt = dt.replace(hour=hour, minute=minute, second=second)
    return dt


def extract_use_weeks():
    """
    Extracts the use weeks from the database which are 12 back and 12 forward from the current week
    :return: A dataframe of the use weeks
    """
    ls_use_weeks = week_span_to_week_list(WorkingWeek, 12, 12)
    df_use_weeks = pd.DataFrame(ls_use_weeks, columns=["Week"])
    return df_use_weeks


def date_string_to_excel_date(date_string):
    """
    Converts a date in the format YYYY-MM-DD to an excel date
    :param date_string: The date to convert
    :return: The converted excel date
    """
    dt = datetime.datetime.strptime(date_string, "%Y-%m-%d")
    dt = (dt - datetime.datetime(1900, 1, 1)).days + 2
    return dt


def excel_date_to_date_string(excel_date):
    """
    Converts an excel date to a date in the format YYYY-MM-DD
    :param excel_date: The excel date to convert
    :return: The converted date in the format YYYY-MM-DD as a string
    """
    dt = datetime.datetime.fromordinal(
        datetime.datetime(1900, 1, 1).toordinal() + int(excel_date) - 2
    )
    hour, minute, second = floatHourToTime(excel_date % 1)
    dt = dt.replace(hour=hour, minute=minute, second=second)
    return str(dt.strftime("%Y-%m-%d"))


def convert_fix_date_to_no_pad(date):
    if date in dict_slashed_pad_to_slashed_nopad.keys():
        return dict_slashed_pad_to_slashed_nopad[date]
    elif date in dict_slashed_no_pad_to_slashed_pad.keys():
        return date
    elif date in dict_dashed_pad_desc_to_slashed_nopad.keys():
        return dict_dashed_pad_desc_to_slashed_nopad[date]
    else:
        print("Date not converted to No_Pad: ", date)
        raise ValueError


def convert_fix_date_to_pad(date):
    if date in dict_slashed_no_pad_to_slashed_pad.keys():
        return dict_slashed_no_pad_to_slashed_pad[date]
    elif date in dict_slashed_pad_to_slashed_nopad.keys():
        return date
    elif date in dict_dashed_pad_desc_to_slashed_pad.keys():
        return dict_dashed_pad_desc_to_slashed_pad[date]
    else:
        print("Date not converted to Pad: ", date)
        raise ValueError


# %%
## Define Functions ##

if __name__ == "__main__":
    print("Running as main")
    print(get_current_datetime("YYYYMMDD"))
    print(get_current_datetime("Weekday"))

    print(excel_date_to_date_string(44924))  # should be 2022-12-29 as a string

# %%
