# %%
## Imports ##

import sys
import os
import glob

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
    trigger_dir,
)

from utils.display_tools import print_logger, pprint_df


# %%
## Trigger ##


def check_for_trigger(file_name_pattern):

    print_logger("Checking for lock file")
    if os.path.isfile(os.path.join(trigger_dir, f"{file_name_pattern}_lock.txt")):

        print_logger("Lock file found")
        print_logger("Done")
        return False

    else:

        print_logger("Lock file not found")

        ls_files = glob.glob(os.path.join(trigger_dir, f"{file_name_pattern}*.txt"))

        if ls_files:
            print_logger("Files found")

            print_logger("Creating lock file")
            with open(
                os.path.join(trigger_dir, f"{file_name_pattern}_lock.txt"), "w"
            ) as f:
                f.write("lock")

            return True
        else:
            print_logger("No files found")
            print_logger("Done")
            return False


def get_ls_trigger_files(file_name_pattern):

    print_logger("Getting list of files triggering this script")
    ls_files = glob.glob(os.path.join(trigger_dir, f"{file_name_pattern}*.txt"))
    print_logger(f"Found {len(ls_files)} files")
    ls_files_without_lock_file = [
        file_item
        for file_item in ls_files
        if file_item != os.path.join(trigger_dir, f"{file_name_pattern}_lock.txt")
    ]

    return ls_files_without_lock_file


def completed_trigger_run(file_name_pattern):

    print_logger("Script Complete")

    print_logger(
        "Moving all files triggering this script to done folder, except lock file"
    )
    ls_files = glob.glob(os.path.join(trigger_dir, f"{file_name_pattern}*.txt"))

    for file_item in ls_files:

        print_logger(f"Checking file: {file_item}")

        if file_item != os.path.join(trigger_dir, f"{file_name_pattern}_lock.txt"):

            print_logger("This is not the lock file")
            print_logger("Moving file to done folder")
            orig_path_with_name = os.path.join(trigger_dir, os.path.basename(file_item))
            new_path_with_name = os.path.join(
                trigger_dir, "done", os.path.basename(file_item)
            )
            os.rename(
                orig_path_with_name,
                new_path_with_name,
            )

    print_logger("Removing lock file")
    os.remove(os.path.join(trigger_dir, f"{file_name_pattern}_lock.txt"))
    print_logger("Done")


# %%
## How To Use ##

"""

# %%
## Imports ##

from utils.trigger_tools import (
    check_for_trigger,
    get_ls_trigger_files,
    completed_trigger_run,
)


TRIGGER_NAME = "labor_plan_runner"


if check_for_trigger(TRIGGER_NAME):

    import labor_plan_builder
    
    # if need to deal with each file that triggers, content of each file is read into a string
    ls_files = get_ls_trigger_files(TRIGGER_NAME)
    for file in ls_files:
        with open(file, "r") as f:
            dc = f.read()
            print(f"DC: {dc}")
            
    completed_trigger_run(TRIGGER_NAME)


# %%



"""

# %%
## Apps Script to create file for task ##

"""
function create_task_file() {
  
  // ############# change task here #############
  var task_to_run = "ENTER_FILE_NAME_PATTERN_HERE";
  // ############# change task here #############
  
  SpreadsheetApp.getActive().toast("Creating task: " + task_to_run);
  var folder_id = "1opqrOCCiQA_a5Sm63cSwwf4BeD9ckGHn";
  var folder = DriveApp.getFolderById(folder_id);
  
  // get current date and time in YYYY_MM_DD_HH_MM_SS format
  var date = new Date();
  var current_date_time = Utilities.formatDate(date, "GMT", "yyyy_MM_dd_HH_mm_ss");

  var file_name = task_to_run + " " + current_date_time + ".txt";
    
  // use meaningful content for file if needed
  var file = folder.createFile(file_name, "This is the content of my file.");
  
}

"""
"""
// to make a button triggering it with the ability to pass the tab name as a parameter
function create_task_file(tab_triggered) {
  
  // ############# change task here #############
  var task_to_run = "labor_optimization_runner";
  // ############# change task here #############
  
  SpreadsheetApp.getActive().toast("Creating task: " + task_to_run);
  var folder_id = "1opqrOCCiQA_a5Sm63cSwwf4BeD9ckGHn";
  var folder = DriveApp.getFolderById(folder_id);
  
  // get current date and time in YYYY_MM_DD_HH_MM_SS format
  var date = new Date();
  var current_date_time = Utilities.formatDate(date, "GMT", "yyyy_MM_dd_HH_mm_ss");

  var file_name = task_to_run + " " + current_date_time + ".txt";
    
  // use meaningful content for file if needed
  var file = folder.createFile(file_name, tab_triggered);
  
}

function onButtonClick() {
  var activeSheet = SpreadsheetApp.getActive().getActiveSheet();
  var sheetName = activeSheet.getName();
  Logger.log(sheetName);
  // SpreadsheetApp.getUi().alert(sheetName);
  SpreadsheetApp.getActive().toast(sheetName);
  create_task_file(sheetName);
}
"""
