# %%
## Running Imports ##
from __future__ import print_function
from email.message import EmailMessage

if __name__ != "__main__":
    print(f"Importing {__name__}")

import os, glob
import os.path
from os.path import expanduser
from time import sleep
import sys

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import base64
import pandas as pd
import datetime
from pprint import pprint

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


# %%
## Gmail Setup ##

"""
######### Features #########

Without force download, each email will be downloaded only once, its message id is logged in a file after the first successful download

######### Usage #########

--- creating credentials ---
Open the Google Cloud Console @ https://console.cloud.google.com/
At the top-left, click Menu menu > APIs & Services > Credentials.
Click Create Credentials > OAuth client ID.
Click Application type > Desktop app.
In the "Name" field, type a name for the credential. This name is only shown in the Cloud Console.
Click Create. The OAuth client created screen appears, showing your new Client ID and Client secret.
Click OK. The newly created credential appears under "OAuth 2.0 Client IDs."
Save in parent folder of this script as OAuth.json

--- downloading existing credentials ---
Open the Google Cloud Console @ https://console.cloud.google.com/
At the top-left, click Menu menu > APIs & Services > Credentials.
Click OAuth client ID > Select the client ID you created.
Click Download JSON.
Save in parent folder of this script as OAuth.json

--- to import ---
from utils.gmail_tools import get_attachment_from_search_string

---search_string---
set gmail search string, can use todays date but must be a string or f string

---output_path---
use os.path.join on your path to make sure it is correct

---output_file_name---
set output_file_name to None if you want to use the original file name
set output_file_name to 'original with date' if you want to use the original file name with date

---force_download---
set force_download to True if you want to download the file even if it has already been downloaded

---retries---
Set retry variable if inconsistant results, default is 3, will not duplicate files or keep trying if it succeeds

######### Limitations #########

Only downloads first file in the attachments of each email

"""


# %%
## Constant Variables ##

# If modifying these scopes, delete the file token.json.
SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]


# %%
## Funtions ##


def get_attachment_from_search_string(
    search_string, output_path, output_file_name=None, force_download=False
):
    creds = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists(
        os.path.join(
            expanduser("~"), "credentials", "personal", "gmail_auth", "token.json"
        )
    ):
        creds = Credentials.from_authorized_user_file(
            os.path.join(
                expanduser("~"), "credentials", "personal", "gmail_auth", "token.json"
            ),
            SCOPES,
        )
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                os.path.join(
                    expanduser("~"),
                    "credentials",
                    "personal",
                    "gmail_auth",
                    "oauth.json",
                ),
                SCOPES,
            )
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open(
            os.path.join(
                expanduser("~"), "credentials", "personal", "gmail_auth", "token.json"
            ),
            "w",
        ) as token:
            token.write(creds.to_json())

    try:
        # search gmail for message
        service = build("gmail", "v1", credentials=creds)
        results = (
            service.users().messages().list(userId="me", q=search_string).execute()
        )
        all_messages_from_search = results.get("messages", [])
        ls_paths_with_file_names = []
        if not all_messages_from_search:
            print("No messages found.")
            return
        for this_message_from_reults in all_messages_from_search:
            message_id = this_message_from_reults["id"]
            print("message_id: " + str(message_id))

            message_was_already_done = False

            if not os.path.exists(os.path.join(file_dir, "done_message_ids.txt")):
                with open(os.path.join(file_dir, "done_message_ids.txt"), "w") as f:
                    f.write("")

            else:
                with open(
                    os.path.join(file_dir, "done_message_ids.txt"), "r+"
                ) as message_log:
                    for line in message_log:
                        if line.strip() == str(message_id):
                            message_was_already_done = True
                            continue

            if message_was_already_done and not force_download:
                print("Message already downloaded and force_download is False")
                continue
            elif message_was_already_done and force_download:
                print(
                    "Message already downloaded and force_download is True, downloading again"
                )

            executed_message = (
                service.users().messages().get(userId="me", id=message_id).execute()
            )

            internal_date_received = executed_message["internalDate"]

            internal_dt_received = pd.to_datetime(
                int(float(internal_date_received) / 1000), unit="s", origin="unix"
            ).strftime(format="%Y-%m-%d")
            str_internal_dt_received = str(internal_dt_received)

            internal_dt_received_with_seconds = pd.to_datetime(
                int(float(internal_date_received) / 1000), unit="s", origin="unix"
            ).strftime(format="%Y.%m.%d %H.%M.%S")
            str_internal_dt_received_with_seconds = str(
                internal_dt_received_with_seconds
            )

            message_subject = ""
            for item in executed_message["payload"]["headers"]:
                if item["name"] == "Subject":
                    message_subject = item["value"]
                    print(f"subject is {message_subject}")
                    break
            for part in executed_message["payload"]["parts"]:
                if part["filename"]:
                    print("detected attachment type 1")
                    original_file_extension = part["filename"].split(".")[-1]
                    if output_file_name == None:
                        print("output_file_name is None, using original file name")
                        filename = part["filename"]
                        filename = (
                            filename.replace("/", " ")
                            .replace(":", "")
                            .replace("?", "")
                            .replace("&", "and")
                        )
                    elif output_file_name == "original with date":
                        print(
                            "output_file_name is original with date, using original file name with date"
                        )
                        filename = (
                            part["filename"]
                            + " "
                            + str_internal_dt_received
                            + "."
                            + original_file_extension
                        )
                        filename = (
                            filename.replace("/", " ")
                            .replace(":", "")
                            .replace("?", "")
                            .replace("&", "and")
                        )
                    elif output_file_name == "original with date and seconds":
                        print(
                            "output_file_name is original with date and seconds, using original file name with date and seconds"
                        )
                        filename = (
                            part["filename"]
                            + " "
                            + str_internal_dt_received_with_seconds
                            + "."
                            + original_file_extension
                        )
                        filename = (
                            filename.replace("/", " ")
                            .replace(":", "")
                            .replace("?", "")
                            .replace("&", "and")
                        )
                    else:
                        filename = output_file_name
                        filename = (
                            filename.replace("/", " ")
                            .replace(":", "")
                            .replace("?", "")
                            .replace("&", "and")
                        )
                    print(f"filename is {filename}")
                    attachment = (
                        service.users()
                        .messages()
                        .attachments()
                        .get(
                            id=part["body"]["attachmentId"],
                            userId="me",
                            messageId=message_id,
                        )
                        .execute()
                    )
                    file_data = base64.urlsafe_b64decode(
                        attachment["data"].encode("utf-8")
                    )
                    break
                else:
                    try:
                        if part["parts"][0]["filename"]:
                            print("detected attachment type 2")
                            original_file_name = part["parts"][0]["filename"]
                            # replace characters for windows
                            original_file_name = (
                                original_file_name.replace(":", " -")
                                .replace("/", "-")
                                .replace("\\", "-")
                                .replace("&", "-")
                            )
                            original_file_name = (
                                original_file_name.replace("/", " ")
                                .replace(":", "")
                                .replace("?", "")
                                .replace("&", "and")
                            )
                            original_file_extension = original_file_name.split(".")[-1]
                            if output_file_name == "original with subject and datetime":
                                filename = (
                                    original_file_name
                                    + " "
                                    + message_subject
                                    + str_internal_dt_received_with_seconds
                                    + "."
                                    + original_file_extension
                                )
                            elif output_file_name == "domo split":
                                filename = (
                                    original_file_name.split(" - ")[0]
                                    + " - Active Roster - "
                                    + str_internal_dt_received
                                    + " - "
                                    + original_file_name.replace("|||", "$").split("$")[
                                        1
                                    ]
                                    + "."
                                    + original_file_extension
                                )
                            elif output_file_name == "highjump":
                                filename = (
                                    "highjump file"
                                    + " "
                                    + str_internal_dt_received_with_seconds
                                    + "."
                                    + original_file_extension
                                )
                            attachment_id = part["parts"][0]["body"]["attachmentId"]
                            attachment = (
                                service.users()
                                .messages()
                                .attachments()
                                .get(
                                    id=attachment_id, userId="me", messageId=message_id
                                )
                                .execute()
                            )
                            file_data = base64.urlsafe_b64decode(
                                attachment["data"].encode("utf-8")
                            )
                            break
                    except:
                        print("did not detect attachment type 2")

            path_with_file_name = os.path.join(output_path, filename)
            print(f"path_with_file_name is {path_with_file_name}")
            with open(path_with_file_name, "wb") as f:
                f.write(file_data)
            ls_paths_with_file_names.append(path_with_file_name)

            if not force_download:
                with open(
                    os.path.join(file_dir, "done_message_ids.txt"), "a+"
                ) as message_log:
                    message_log.write(str(message_id) + "\n")
            else:
                print("force download so not logging message id")

        return ls_paths_with_file_names

    except HttpError as error:
        print(f"An error occurred: {error}")


# %%
## If Main ##

if __name__ == "__main__":

    print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    today_date = datetime.datetime.now().strftime("%Y-%m-%d")

    ############################### Download Roster Files ##############################
    gmail_search_string = f'(from:peopleinsights@hellofresh.com OR kevin.huebschman@hellofresh.com) has:attachment "Daily Roster Report" after:{today_date}'
    print(f" ############ Searching for: {gmail_search_string} ############")

    retries = 3
    while retries > 0:
        try:
            get_attachment_from_search_string(
                gmail_search_string,
                data_dir,
                "roster files",
                force_download=False,
            )
            break
        except Exception as e:
            print(e)
            print("Retrying... " + str(retries) + " retries left")
            sleep(2)
            retries -= 1

    ############################### testing ###############################

    ########################## test 2 ##########################

    # get_attachment_from_search_string(gmail_search_string, file_dir, output_file_name = 'original with date', force_download = True) # enable for testing

    print(f'Done at {datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')

# %%
