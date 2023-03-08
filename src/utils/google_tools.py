# %%
## Imports ##

if __name__ != "__main__":
    print(f"Importing {__name__}")

import pygsheets
import os
from os.path import expanduser
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
import pandas as pd
import datetime
import sys
import time
from google.auth.exceptions import TransportError
from googleapiclient.errors import HttpError
from google.oauth2 import service_account
from googleapiclient.discovery import build


# append grandparent
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

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

from utils.display_tools import print_logger

file_dir = os.path.dirname(os.path.realpath(__file__))


# %%
## Google Auth Setup ##

# --- creating credentials ---
# Open the Google Cloud Console @ https://console.cloud.google.com/
# At the top-left, click Menu menu > APIs & Services > Credentials.
# Click Create Credentials > OAuth client ID.
# Click Application type > Desktop app.
# In the "Name" field, type a name for the credential. This name is only shown in the Cloud Console.
# Click Create. The OAuth client created screen appears, showing your new Client ID and Client secret.
# Click OK. The newly created credential appears under "OAuth 2.0 Client IDs."
# Save in parent folder of this script as OAuth.json

# --- downloading existing credentials ---
# Open the Google Cloud Console @ https://console.cloud.google.com/
# At the top-left, click Menu menu > APIs & Services > Credentials.
# Click OAuth client ID > Select the client ID you created.
# Click Download JSON.
# Save in parent folder of this script as OAuth.json


# %%
## Google ##


gc = pygsheets.authorize(
    service_file=os.path.join(
        expanduser("~"),
        "credentials",
        "team",
        "gsheets_auth_service",
        "service_account_credentials.json",
    ),
    credentials_directory=os.path.join(file_dir, "gsheets_auth_service"),
)

try:
    gc_oauth = pygsheets.authorize(
        client_secret=os.path.join(
            expanduser("~"),
            "credentials",
            "personal",
            "gsheets_auth_oauth",
            "oauth.json",
        ),
        credentials_directory=os.path.join(
            expanduser("~"), "credentials", "personal", "gsheets_auth_oauth"
        ),
    )
except Exception as e:
    print_logger(f"Error connecting to Google Sheets with OAuth because {e}")
    pass


# %%
## Frequently Used Functions ##

loc_sheets_output_cache = os.path.join(data_dir, "sheets_output_cache")


def WriteToSheetsCache(bookName, sheetName, df, indexes):
    write_folder = os.path.join(loc_sheets_output_cache, bookName)

    os.makedirs(write_folder, 0o755, exist_ok=True)
    # remove characters that windows cant have from filename
    sheetName = (
        sheetName.replace("/", " ")
        .replace(":", "")
        .replace("?", "")
        .replace("&", "and")
    )
    df.to_csv(os.path.join(write_folder, sheetName + ".done.csv"), index=indexes)


dict_connected_books = {}
dict_connected_sheets = {}


def get_book_from_id(id, retry=True):
    global dict_connected_books

    if id in dict_connected_books.keys():
        Workbook = dict_connected_books[id]
        print_logger(f"Using cached connection to {id}")
        return Workbook

    try:
        book_from_id = gc.open_by_key(id)
        dict_connected_books[id] = book_from_id
        print_logger(f"Opening new connection to {id}")
        return book_from_id
    except TransportError as e:
        print_logger(
            f"Error opening connection to {id}, Trying again in 5 seconds, error: {e}"
        )
        if retry:
            time.sleep(30)
            return get_book_from_id(id, retry=False)
        else:
            print_logger("Failed to connect to Google Sheets even after retrying")
            raise Exception(
                f"Failed to connect to Google Sheets even after retrying because of TransportError {e}"
            )
    except HttpError as e:
        if e.resp.status == 429:
            print_logger(
                f"Error HttpError 429, rate limited, opening connection to {id}, Trying again in 20 seconds, error: {e}"
            )
            if retry:
                time.sleep(20)
                return get_book_from_id(id, retry=False)
            else:
                print_logger(
                    f"Failed to connect to Google Sheets even after retrying because of HttpError 429: {e}"
                )
                raise Exception(
                    f"Failed to connect to Google Sheets even after retrying because of HttpError 429: {e}"
                )
        elif e.resp.status == 500:
            print_logger(
                f"Error HttpError 500, internal server error, opening connection to {id}, Trying again in 120 seconds, error: {e}"
            )
            if retry:
                time.sleep(120)
                return get_book_from_id(id, retry=False)
            else:
                print_logger(
                    f"Failed to connect to Google Sheets even after retrying because of HttpError 500: {e}"
                )
                raise Exception(
                    f"Failed to connect to Google Sheets even after retrying because of HttpError 500: {e}"
                )
        elif e.resp.status == 503:
            print_logger(
                f"Error HttpError 503, internal server error, opening connection to {id}, Trying again in 120 seconds, error: {e}"
            )
            if retry:
                time.sleep(120)
                return get_book_from_id(id, retry=False)
            else:
                print_logger(
                    f"Failed to connect to Google Sheets even after retrying because of HttpError 503: {e}"
                )
                raise Exception(
                    f"Failed to connect to Google Sheets even after retrying because of HttpError 503: {e}"
                )
        elif e.resp.status == 404:
            print_logger(
                f"HttpError 404 opening connection to {id}, Trying again in 5 seconds, error: {e}"
            )
            if retry:
                time.sleep(5)
                return get_book_from_id(id, retry=False)
            else:
                print_logger(
                    f"Failed to connect to Google Sheets even after retrying because of HttpError 404: {e}"
                )
                raise Exception(
                    f"Failed to connect to Google Sheets even after retrying because of HttpError 404: {e}"
                )

        else:
            raise Exception(
                f"Failed to connect to Google Sheets because of other HttpError {e}"
            )


dict_hardcoded_book_ids = {
    "DB_Attendance": "1pCmdmj556t0vC5xG_3z7rR3iX4IJQhXWHoO-4eYnQVA",
    "DB_Attrition": "1iZFlMf9cpxNFagrY6yYaxPC8pW_v01eLyqtdcWBJZd0",
    "DB_Budgets": "1ZENeCCo0MkSUZgGnF6NorpucmuKMJLxLM0xufB4vJSQ",
    "DB_Mock_Live": "1hNGIoN3l8Jo0CqzYucSTjhqog6mFa8euy3PU7xNT318",
    "DB_Risks": "1NdYvAaG_UJXiAjcn6ewOaUtXA-dJxL8BXdXrCkPudOU",
    "DB_Sheets_Links": "1rEmncTR6gd1XIbm4yQMiYLeGcDQanOUYjxQHEAYmpG0",
    "DB_Shifts": "1ExQxe8Fg81B48sLOTuAmZ29qiA8AV5fpToKDXlctd3w",
    "DB_Volume_Forecast": "1KBiEdvRHTeOwAkY0ozDD0JL9woccDK_2L7gzBIiBZuY",
    "DC Labor Planning 2.0": "1ShuHp98wlM3ycR-0sD8tNDYOsSxlyB0G7GgQaq17vE4",
    "MRMRDW Rosters": "1IKM7Rq6D3AAiG0dSvQMj15vvOofhLWmNvMn8QOPzG3c",
    "Non-Production Staffing 2.0": "1pBoEgjyuDrjXJYq3K1tZQgKYNVgtrl7CTNoahKrdnTY",
    "Roster Status": "1ODOkr1sYX7vKDOVLYGgCOqOoNdDS6qynm7IfVyI4fio",
    "Mock_Live_Update_Tasks": "1TKbvRpxgPtuG5zlJRt_O3Cn9GLEGIj0L-TRS-3IcrTE",
    "SDI 2022": "1khpR9U2ZtnDVbKorS2IiPmSOwFYwNo_i3tsPsvS1ZpU",
    "SDI v3": "1ho4qDBKtjtaDZUmf7-yjJMjijOlHEGYooNNFY_Q3vK4",
    "Strategic Fulfillment Planning Dashboards_WIP": "1dmld4Ta6sld2f4tRcCFYnHB528BKqPXcb-cTG04oK24",
    "Weeks": "1RTUZ9GSH1sP9pHMastqZoPkZEGv83P8rmPDyWkzk4lM",
    "Aliases": "1_jaqUR1vFXFJuUMSXpm0PvQqPXQazvcRiBarn8Kj8KM",
    "LaborPlanRunner": "1otD1P01CMH4-js1fiMlHYuYDhaKeZYTogVtdHkGKXjg",
    "Capacity v2": "1eP9c-goGvmKrsAudWhhGrlqzQMobpqCJXeVZF_zonP8",
    "2023 Kiwi - New Starts vs HC Requested by Planning - SCM Week": "1ZCvrkyyLtTQNjt75ZUnpvl8jA4nrMWe4_Wf6nteUowk",
    "Needs Sheet": "1lrjw0fzhATBzWaJWaLqZ9Mw1KqSsVesu1RF0Pvk8AiI",
    "Local Needs Sheet": "1lQUFKYLJkZxR91CEM3_Vg3tvT2guu0JJpuopIyv_cHk",
    "Workforce Balancing Tracker": "1fRhJNtYIfuXCdX4R-glgwC9tMkkgIoUosw3qOGt5sMI",
    "Labor_Optimization": "1v46yVUwn9kIGHWMH874EO5QA0htnJK6v_aE0CuEBUdM",
    "Labor_Optimization_Tracker": "13wbs_AQWu9-LGi7ZwowTBBdpgIAoLo4fmc90giwmBwE",
    "2022 Network HC Tracker": "1bAwciw650U66kjRm_BiyvjUCDwD7apUxxhbkG1N14Fc",
    "Kiwi_Extractions": "13agmwRZsZdYMYQ7rYRjCR4t30ATb2KuZb2PyvgDqqSE",
    "Ship Hybrid Needs Fulfillment Planning": "1PgSSiUkZIRGpjXvBeCPssTHqLWVMo1X-H-K5y8IgYLk",
    "MPBTracker": "1YqdQvJ2QY1Ye1GPtSznb6wm49winsIZ7dZi5ex2Xtkg",
    "VolumeSheet": "1jpDfSqyPJcpzLtyx4URkBEk8wZV-E4pJPEbWl2HvRRo",
    "WonoloLookerExtract": "1NAMHfY2-7VlovKPR-oejH0GojZI1o58WcCaFrBgG6Vg",
    "Bruno - Staffing Tracking": "1YkrCuAbaWVzAAjcYRQYqVkYr0XoDGmnfN0042c3mCUc",
    "We Don't Talk About Bruno v2.0 (DO NOT USE)": "1eiMEzZu1XMfdYk-uaBBof5wmCLJPhhzt8kUmTGEeoHo",
    "HF_DC Weekly Target Tracker": "1nQIeuVoTm6maSfhMZje-XibY2lE50Ci_UgIx134j-RE",
    "The Creator": "1mcWynyqek8iYQEnI3SCZs6kRZZ5HJb5tUSMPBg4qE9s",
    "EP_DC Weekly Target Tracker": "1oeaYC8re1S8DV42YkB3jHleKYZhmsorbWW9d30xm5Pg",
    "Long term Network Plan": "1yPVsAO5KA-0IzgA2Aypt8242OvLxzEdrHVw2XMULoVo",
    "GC LT Forecast by Lane_2022-2023 v2": "16QI5vIGBju-Qd2v78Wr4RAz0x6GgvqmmdZZGh9cawCc",
    "GC LT Forecast by Lane_2023 W10+ v2": "1IlcuLnrO96wIbpaw0ZtgR1NhBDgnjj6RuFdKfbx7t9U",
    "Peak Planning": "1JdereC3kuwsF3z5c2oaLjxqw0AWuX5rJ7PU4ojZVr18",
    "Risk_Builder_Triggers": "1EkJXfCndhDbdD_KZMj3EPuwZi6OvbF6LmAmrGLpQR00",
    "Mid Shift Attrition Tracker": "1mJ-HagGggzLsEV_ftF-o3ezOXS7iBXre6JAe5xISOr0",
    "TestApp": "1ZEf5IkCGyHEbR8hnvMKmjAd_-4DejRPs1de4iigUpF0",
    "Labor Orders and Fills": "14cc-V1orGSN1Y2JLqh_srrXEhUh8EkqcDvaEeGdtAkY",
    "Sanders_File_Extraction": "1Jji3VO7sscNq8bikHaywDDtZ9ZActiJ6zJNoSN5Xb2k",
    "NJ Planning Reports": "1ytNJcvsYaxXEtFDfjzWOdxZNKSQkigMINYFU0yrvH70",
    "TI Planning Reports": "1c77S9vW8gAVsrTXMyJoPRknQc7bexio8oCu_Ge-JHI0",
    "GA Planning Reports": "1w9VnxdFHE8ooUVAqRHBq6yw4fWlFjKEpWGV1JDcI9iA",
    "AZ Planning Reports": "1Bam0qEGtrRQMiHa3NfGvEKU64041Gt5-JLJ_5ZyWPdg",
    "TX Planning Reports": "1MlUIzIuNs7Ycw53iZFAR5Pd7nL_ZZ-5Uni_9pfRLY_4",
    "NT Planning Reports": "1USfqTZygq_ZEcbeFHhb9Cffm-wXy8hna6jSJ0qaDm40",
    "SW Planning Reports": "1GWkGSV2UigzL1g__UcipTvnyF6KUcbp5xGDZCT68oiQ",
    "AU Planning Reports": "1SycL3PzPfwy8DjhemJA-YsLPeLsj8JPAzhtc9N0jbbs",
}


def get_book(bookName, retry=True):
    global dict_connected_books

    if bookName in dict_hardcoded_book_ids.keys():
        print_logger(
            f"Book {bookName} in hardcoded book ids, using id: {dict_hardcoded_book_ids[bookName]}"
        )
        return get_book_from_id(dict_hardcoded_book_ids[bookName])
    else:
        print_logger(
            f"Book {bookName} not in hardcoded book ids, trying to open by name"
        )

    if bookName in dict_connected_books.keys():
        Workbook = dict_connected_books[bookName]
        print_logger(f"Using cached connection to {bookName}")
        return Workbook
    else:
        try:
            print_logger(f"Opening new connection to {bookName}")
            Workbook = gc.open(bookName)

            # print out what should add
            workbook_id_to_add_to_dict = Workbook.id
            print_logger(
                f'Consider adding this to dict hardcoded book ids: "{bookName}": "{workbook_id_to_add_to_dict}"'
            )
            # write to file what should add
            with open(
                os.path.join(data_dir, "dict_hardcoded_book_ids_to_add.txt"), "a"
            ) as f:
                f.write(f'"{bookName}": "{workbook_id_to_add_to_dict}"\n')

            dict_connected_books[bookName] = Workbook
            return Workbook
        except TransportError as e:
            print_logger(
                f"Error opening connection to {bookName}, Trying again in 5 seconds, error: {e}"
            )
            time.sleep(5)
            if retry:
                return get_book(bookName, retry=False)
            else:
                print_logger("Failed to connect to Google Sheets even after retrying")
                raise e
        except HttpError as e:
            if e.resp.status == 429:
                print_logger(
                    f"Error HttpError 429, rate limited, opening connection to {bookName}, Trying again in 20 seconds, error: {e}"
                )
                time.sleep(20)
                if retry:
                    return get_book(bookName, retry=False)
                else:
                    print_logger(
                        "Failed to connect to Google Sheets even after retrying"
                    )
                    raise e
            else:
                raise e


def get_book_sheet(bookName, sheetName):
    """
    This function will return a Worksheet object from a google sheet using the ss name and the sheet name, will return a cached connection if it exists
    :param bookName: the name of the google sheet
    :param sheetName: the name of the sheet within the google spreadsheet
    :return: a Worksheet object
    """
    global dict_connected_sheets

    if f"{bookName} : {sheetName}" in dict_connected_sheets.keys():
        Worksheet = dict_connected_sheets[f"{bookName} : {sheetName}"]
        print_logger(f"Using cached connection to {bookName} : {sheetName}")
        return Worksheet
    else:
        Workbook = get_book(bookName)
        Worksheet = Workbook.worksheet_by_title(sheetName)
        dict_connected_sheets[f"{bookName} : {sheetName}"] = Worksheet
        print_logger(f"Opening new connection to {bookName} : {sheetName}")
        return Worksheet


def get_book_sheet_from_id_name(id, sheetName):
    """
    This function will return a Worksheet object from a google sheet using the spreadsheet id and the sheet name, will return a cached connection if it exists
    :param id: the id of the google spreadsheet
    :param sheetName: the name of the sheet within the google spreadsheet
    :return: a Worksheet object
    """
    global dict_connected_sheets

    if f"{id} : {sheetName}" in dict_connected_sheets.keys():
        Worksheet = dict_connected_sheets[f"{id} : {sheetName}"]
        print_logger(f"Using cached connection to {id} : {sheetName}")
        return Worksheet
    else:
        Workbook = get_book_from_id(id)
        Worksheet = Workbook.worksheet_by_title(sheetName)
        dict_connected_sheets[f"{id} : {sheetName}"] = Worksheet
        print_logger(f"Opening new connection to {id} : {sheetName}")
        return Worksheet


def WriteToSheets(
    bookName,
    sheetName,
    df,
    indexes=False,
    set_note=None,
    retries=3,
):
    """
    This function will write a dataframe to a google sheet, will create the sheet if it doesnt exist
    :param bookName: the name of the google spreadsheet
    :param sheetName: the name of the sheet within the google spreadsheet
    :param df: the dataframe to write to the sheet
    :param indexes: whether to write the index column to the sheet
    :param set_note: the note to set on the sheet, None for no note, "DT" for date time, string for custom note
    :param retries: the number of times to retry if the connection fails
    :return: None
    """

    # global dict_connected_books
    # global dict_connected_sheets

    start_time = datetime.datetime.now()
    print_logger(
        f"Writing to Google Sheet: {bookName} - {sheetName} with size {df.shape}"
    )
    if isinstance(df, pd.Series):
        print_logger(
            "# Found a series when writing to sheets, converting to dataframe #"
        )
        df = df.reset_index()

    WriteToSheetsCache(bookName, sheetName, df, indexes)

    for i in range(retries):
        try:
            Workbook = get_book(bookName)

            try:
                Worksheet = get_book_sheet(bookName, sheetName)
            except pygsheets.WorksheetNotFound:
                Workbook.add_worksheet(sheetName)
                Worksheet = get_book_sheet(bookName, sheetName)

            if indexes == False:
                Worksheet.set_dataframe(df, (1, 1), fit=True, nan="")
            else:
                Worksheet.set_dataframe(df, (1, 1), fit=True, nan="", copy_index=True)
            try:
                if set_note != None:
                    if set_note == "DT":
                        Worksheet.cell((1, 1)).note = "Data updated at: " + str(
                            datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        )
                    else:
                        Worksheet.cell((1, 1)).note = set_note
            except Exception as e:
                print_logger(
                    f"Failed to set note when writing, error: {e}, trying one more time"
                )
                try:
                    if set_note != None:
                        if set_note == "DT":
                            Worksheet.cell((1, 1)).note = "Data updated at: " + str(
                                datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            )
                        else:
                            Worksheet.cell((1, 1)).note = set_note
                except:
                    pass
                pass
            print_logger(
                f"Finished writing to Google Sheet: {bookName} - {sheetName} with size {df.shape}, after {datetime.datetime.now() - start_time}"
            )
            return
        except Exception as e:
            print_logger(
                f"Failed to write to sheets with name {bookName} and sheet name {sheetName} and df of size {df.shape}, error: {e}"
            )
            print_logger(f"Retrying {i+1} of {retries} times after {i * 20} seconds")
            time.sleep(i * 20)
            print_logger("Retrying now")
            pass

    print_logger(f"Failed to write to sheet after {retries} retries")
    raise Exception(
        f"Failed to write to sheets with name {bookName} and sheet name {sheetName} and df of size {df.shape}"
    )


def ClearSheet(book_name, sheet_name, start_range, end_range):
    """
    This function will clear a range of cells on a sheet
    :param book_name: the name of the google spreadsheet
    :param sheet_name: the name of the sheet within the google spreadsheet
    :param start_range: the start range of the cells to clear in the format "A1"
    :param end_range: the end range of the cells to clear in the format "A1"
    :return: None
    """

    Worksheet = get_book_sheet(book_name, sheet_name)
    Worksheet.clear(start_range, end_range)


def clear_range_of_sheet_obj(sheet_obj, start, end, retries=3):
    """
    This function will clear a range of cells on a sheet
    :param sheet_obj: the sheet object to write to
    :param start: the start range of the cells to clear in the format "A1"
    :param end: the end range of the cells to clear in the format "A1"
    :return: None
    """

    for i in range(retries):
        try:
            sheet_obj.clear(start, end)
            return
        except Exception as e:
            print_logger(f"Failed to clear range, error: {e}")
            print_logger(f"Retrying {i+1} of {retries} times after {i * 20} seconds")
            time.sleep(i * 10)
            print_logger("Retrying now")
            pass

    print_logger(f"Failed to clear range after {retries} retries")
    raise Exception(f"Failed to clear range after {retries} retries")


def write_df_to_range_of_sheet_obj(
    sheet_obj,
    df,
    start,
    fit,
    nan="",
    copy_head=False,
    retries=3,
):
    """
    This function will write a dataframe to a range on a sheet
    :param sheet_obj: the sheet object to write to
    :param df: the dataframe to write to the sheet
    :param start: the start range of the cells to clear in the format "A1"
    :param fit: whether to fit the dataframe to the range
    :param nan: the value to use for nan
    :param copy_head: whether to copy the header
    :return: None
    """

    for i in range(retries):

        try:
            sheet_obj.set_dataframe(
                df=df, start=start, fit=fit, nan=nan, copy_head=copy_head
            )
            return
        except Exception as e:
            print_logger(
                f"Failed to write to range with error: {e}, retrying {i+1} of {retries} times"
            )
            time.sleep(i * 10)
            pass

    print_logger(f"Failed to write to range after {retries} retries")
    raise Exception(f"Failed to write to range with error: {e}")


# %%
## Entire Sheet Operations ##


def copy_sheet_book_to_book(source_book, ls_source_sheets, ls_dest_books):
    Workbook_src = gc.open(source_book)
    src_book_id = Workbook_src.id

    for dest_book in ls_dest_books:

        for source_sheet in ls_source_sheets:

            sheet_src = Workbook_src.worksheet_by_title(source_sheet)
            src_sheet_id = sheet_src.id
            src_tup = (src_book_id, src_sheet_id)

            Workbook_dest = gc.open(dest_book)

            try:
                Workbook_dest.del_worksheet(
                    Workbook_dest.worksheet_by_title(source_sheet)
                )
            except:
                pass

            Workbook_dest.add_worksheet(source_sheet, src_tuple=src_tup)


# %%
## Auth and gets ##


def get_google_authentication():
    gauth = GoogleAuth()
    # Try to load saved client credentials
    gauth.LoadCredentialsFile(
        os.path.join(
            expanduser("~"),
            "credentials",
            "personal",
            "google_auth",
            "google_auth_creds.txt",
        )
    )
    if gauth.credentials is None:
        # Authenticate if they're not there
        gauth.LocalWebserverAuth()
    elif gauth.access_token_expired:
        # Refresh them if expired
        gauth.Refresh()
    else:
        # Initialize the saved creds
        gauth.Authorize()
    # Save the current credentials to a file
    gauth.SaveCredentialsFile(
        os.path.join(
            expanduser("~"),
            "credentials",
            "personal",
            "google_auth",
            "google_auth_creds.txt",
        )
    )
    return gauth


def get_google_drive_obj():
    drive = GoogleDrive(get_google_authentication())
    return drive


def get_file_list_from_folder_id_oauth(folder_id):
    parent_folder_files = (
        get_google_drive_obj()
        .ListFile({"q": f"'{folder_id}' in parents and trashed=false"})
        .GetList()
    )

    return parent_folder_files


def get_book_id_from_parent_folder_id_oauth(parent_folder_id, book_name):

    print(
        f"Getting sheet ID for book named {book_name} inside parent folder ID {parent_folder_id}"
    )

    parent_folder_files = get_file_list_from_folder_id_oauth(parent_folder_id)

    for file in parent_folder_files:
        if file["title"] == book_name:
            file_id = file["id"]
            print(
                f"Found sheet ID {file_id} for book named {book_name} inside parent folder ID {parent_folder_id}"
            )
            return file_id


def get_file_list_from_folder_id(folder_id):

    # authenticate with the Google Drive API using your Pygsheets credentials
    credentials = service_account.Credentials.from_service_account_file(
        os.path.join(
            os.path.expanduser("~"),
            "credentials",
            "team",
            "gsheets_auth_service",
            "service_account_credentials.json",
        ),
        scopes=["https://www.googleapis.com/auth/drive"],
    )
    service = build("drive", "v3", credentials=credentials)

    # retrieve a list of files in the specified folder
    results = (
        service.files()
        .list(
            q=f"'{folder_id}' in parents and trashed=false",
            fields="nextPageToken, files(id, name)",
        )
        .execute()
    )
    files = results.get("files", [])

    if not files:
        print("No files found.")
        return None
    else:
        return files


def get_book_id_from_parent_folder_id(parent_folder_id, book_name):
    print(
        f"Getting sheet ID for book named {book_name} inside parent folder ID {parent_folder_id}"
    )

    parent_folder_files = get_file_list_from_folder_id(parent_folder_id)

    for file in parent_folder_files:
        file_id = file["id"]
        file_title = file["name"]
        print(f"Processing file ID {file_id} with the name {file_title}")
        if file_title == book_name:
            return file_id


def list_files_recursively(folder_id, level):
    file_list = (
        get_google_drive_obj()
        .ListFile({"q": f"'{folder_id}' in parents and trashed=false"})
        .GetList()
    )
    for file in file_list:
        if not (file["mimeType"] == "application/vnd.google-apps.folder") or (
            file["mimeType"] == "application/vnd.google-apps.shortcut"
        ):
            print_logger(
                "\t" * level
                + "title: %s" % file["title"]
                + " - ID: %s" % file["id"]
                + " - Type: %s" % file["mimeType"]
            )

    for file in file_list:
        if (
            file["mimeType"] == "application/vnd.google-apps.folder"
            or file["mimeType"] == "application/vnd.google-apps.shortcut"
        ):
            print_logger(
                "\t" * level
                + "title: %s" % file["title"]
                + " - ID: %s" % file["id"]
                + " - Type: %s" % file["mimeType"]
            )
            list_files_recursively(file["id"], level + 2)


def get_book_from_file_name(file_name):  # need to remove, aliased to new one for now
    book_from_file_name = get_book(file_name)
    return book_from_file_name


def get_df_from_sheet_id(
    id, sheet_name, start_range, end_range, include_tailing_empty=False, retry=True
):
    try:
        data_from_book = get_book_sheet_from_id_name(id, sheet_name).get_as_df(
            start=start_range,
            end=end_range,
            include_tailing_empty=include_tailing_empty,
        )
        return data_from_book
    except Exception as e:
        if retry:
            print_logger(
                f"Failed to get df from sheet id {id}, sheet_name: {sheet_name}, error: {e}, retrying"
            )
            return get_df_from_sheet_id(
                id,
                sheet_name,
                start_range,
                end_range,
                include_tailing_empty,
                retry=False,
            )
        else:
            print_logger(
                f"Failed to get df even after retry from sheet id {id}, sheet_name: {sheet_name}, error: {e}"
            )
            raise Exception(
                f"Failed to get df even after retry from sheet id {id}, sheet_name: {sheet_name}, error: {e}"
            )


def get_df_from_file_name(
    file_name, sheet_name, start_range, end_range, include_tailing_empty=False
):
    book_from_file_name = get_book_from_file_name(file_name)
    sheet_from_book = book_from_file_name.worksheet_by_title(sheet_name)
    data_from_book = sheet_from_book.get_as_df(
        start=start_range, end=end_range, include_tailing_empty=include_tailing_empty
    )
    return data_from_book


def get_df_and_id_from_file_name(
    file_name, sheet_name, start_range, end_range, include_tailing_empty=False
):
    book_from_file_name = get_book_from_file_name(file_name)
    sheet_id = book_from_file_name.id
    sheet_from_book = book_from_file_name.worksheet_by_title(sheet_name)
    data_from_book = sheet_from_book.get_as_df(
        start=start_range, end=end_range, include_tailing_empty=include_tailing_empty
    )
    return data_from_book, sheet_id


def get_book_from_id_oauth(id):
    book_from_id = gc_oauth.open_by_key(id)
    return book_from_id


def get_book_from_file_name_oauth(file_name):
    book_from_file_name = gc_oauth.open(file_name)
    return book_from_file_name


def get_df_from_sheet_id_oauth(
    id, sheet_name, start_range, end_range, include_tailing_empty=False
):
    book_from_id = get_book_from_id_oauth(id)
    sheet_from_book = book_from_id.worksheet_by_title(sheet_name)
    data_from_book = sheet_from_book.get_as_df(
        start=start_range, end=end_range, include_tailing_empty=include_tailing_empty
    )
    return data_from_book


def get_df_from_file_name_oauth(
    file_name, sheet_name, start_range, end_range, include_tailing_empty=False
):
    book_from_file_name = get_book_from_file_name_oauth(file_name)
    sheet_from_book = book_from_file_name.worksheet_by_title(sheet_name)
    data_from_book = sheet_from_book.get_as_df(
        start=start_range, end=end_range, include_tailing_empty=include_tailing_empty
    )
    return data_from_book


def copy_formulas_range_to_range(
    book_name, copy_sheet_name, copy_range, paste_sheet_name, paste_range_string
):
    copy_sheet = get_book_sheet(book_name, copy_sheet_name)
    paste_sheet = get_book_sheet(book_name, paste_sheet_name)
    paste_sheet.update_values(
        paste_range_string,
        copy_sheet.get_values(
            start=copy_range[0],
            end=copy_range[1],
            returnas="matrix",
            include_tailing_empty=True,
            include_tailing_empty_rows=True,
            value_render="FORMULA",
        ),
    )


def convert_to_sheets_link(sheet_id):
    if (sheet_id == "") or (sheet_id == None):
        return ""
    return f'=hyperlink("https://docs.google.com/spreadsheets/d/{sheet_id}","Link")'


# %%

if __name__ == "__main__":
    print_logger("########## Printing Service Account Auth ##########")
    print_logger(gc)

    print_logger("########## Printing OAuth Account Auth ##########")
    print_logger(gc_oauth)

    print_logger("########## Testing Google Service ##########")

    print_logger(
        get_df_from_file_name(
            "Weeks", "Weeks", "A1", "H10", include_tailing_empty=False
        )
    )

    print_logger("########## Testing Google OAuth ##########")

    print_logger(
        get_df_from_file_name_oauth(
            "Weeks", "Weeks", "A1", "H10", include_tailing_empty=False
        )
    )

    print_logger("########## Other Tests ##########")

    # list_files_recursively('1uZErieumi_aeOuUJYyUjEa1ycu0SecnU', 0) # sanders folder in my drive
    # try:
    #     list_files_recursively('1BgChau29JWZEFMVY268cfGwl4R_555J8', 0) # sanders 2022 folder
    # except Exception as e:
    #     print_logger(e)
    # list_files_recursively('1X1X3ekFXguOR_LxDl-e9Hhr94Gay2n97', 0) # MRDW Rosters
    # list_files_recursively('1tjme28fhyMXUOxX7AFJwM_L0yDbVVhFE', 0) # Roster Automation Folder # shows nothing
    # list_files_recursively('16d_RT52lfuPJ-X4DFNAtJ5xVqHNI53hd', 0) # test

    print(get_book_sheet("Weeks", "Days").get_as_df(start="A1", end="J").head(5))

    print_logger("Done")


# %%
