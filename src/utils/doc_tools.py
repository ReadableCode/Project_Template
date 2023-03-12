# %%
## Imports ##

import os
import pandas
import datetime
import sys
import json
import numpy as np

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

from utils.display_tools import pprint_df, pprint_dict, pprint_ls


# %%
## Variables ##

file_dir = os.path.dirname(os.path.realpath(__file__))
docs_dir = os.path.join(grandparent_dir, "docs")

# get repo_owner, repo_name, branch_name from json file
dict_config = json.load(open(os.path.join(file_dir, "docs_configuration.json")))
repo_owner = dict_config["repo_owner"]
repo_name = dict_config["repo_name"]
branch_name = dict_config["branch_name"]

print(
    f"Found docs configuration file, repo_owner: {repo_owner}, repo_name: {repo_name}, branch_name: {branch_name}"
)


# %%
## Functions ##


def get_git_link(
    script_path, repo_owner=repo_owner, repo_name=repo_name, branch_name=branch_name
):
    """
    Get the link to the git repository
    :return: link to the git repository
    """

    return (
        f"https://github.com/{repo_owner}/{repo_name}/blob/{branch_name}/{script_path}"
    )


def get_git_link_formula(
    script_path, repo_owner=repo_owner, repo_name=repo_name, branch_name=branch_name
):
    """
    Get the link to the git repository
    :return: link to the git repository
    """

    git_link = get_git_link(
        script_path, repo_owner=repo_owner, repo_name=repo_name, branch_name=branch_name
    )
    return f'=hyperlink("{git_link}","Link")'


def get_sheet_link(sheet_id):
    if (sheet_id == "") or (sheet_id == None) or (len(sheet_id) != 44):
        return ""
    return f"https://docs.google.com/spreadsheets/d/{sheet_id}"


def convert_to_sheets_link(sheet_id):
    sheet_link = get_sheet_link(sheet_id)
    if sheet_link == "":
        return ""
    return f'=hyperlink("{sheet_link}","Link")'


def get_domo_link(domo_table_id):
    if (domo_table_id == "") or (domo_table_id == None):
        return ""
    return f"https://hellofresh.domo.com/datasources/{domo_table_id}/details/data/table"


def convert_to_domo_link(domo_table_id):
    domo_link = get_domo_link(domo_table_id)
    if domo_link == "":
        return ""
    return f'=hyperlink("{domo_link}","Link")'


def strip_characters_for_mermaid(string):
    return string.replace(" ", "_").replace("(", "_").replace(")", "_")


def get_mermaid_chunks(row):

    # if row["script_path"] is a float
    if isinstance(row["script_path"], float):

        print(f"issue with row: {row}")

    # Add values to the "mermaid" column based on the value of "input_output" column
    left = ""
    right = ""

    if row["input_output"] == "output":
        if row["resource_type"] == "google_sheet":
            left = f'{row["script_path"]}'
            left_link = get_git_link(row["script_path"])
            right = f'{row["spreadsheet_name"]}'
            right_link = get_sheet_link(row["spreadsheet_id"])
        elif row["resource_type"] == "domo_table":
            left = f'{row["script_path"]}'
            left_link = get_git_link(row["script_path"])
            right = f'{row["domo_table_name"]}'
            right_link = get_domo_link(row["domo_table_id"])
        elif row["resource_type"] == "file":
            left = f'{row["script_path"]}'
            left_link = get_git_link(row["script_path"])
            right = f'{row["file_name"]}'
            right_link = ""
        elif row["resource_type"] == "database_table":
            left = f'{row["script_path"]}'
            left_link = get_git_link(row["script_path"])
            right = f'{row["database_table_path"]}'
            right_link = ""
        elif row["resource_type"] == "s3_bucket":
            left = f'{row["script_path"]}'
            left_link = get_git_link(row["script_path"])
            right = f'{row["s3_bucket"]}'
            right_link = ""
    elif row["input_output"] == "input":
        if row["resource_type"] == "google_sheet":
            left = f'{row["spreadsheet_name"]}'
            left_link = get_sheet_link(row["spreadsheet_id"])
            right = f'{row["script_path"]}'
            right_link = get_git_link(row["script_path"])
        elif row["resource_type"] == "domo_table":
            left = f'{row["domo_table_name"]}'
            left_link = get_domo_link(row["domo_table_id"])
            right = f'{row["script_path"]}'
            right_link = get_git_link(row["script_path"])
        elif row["resource_type"] == "file":
            left = f'{row["file_name"]}'
            left_link = ""
            right = f'{row["script_path"]}'
            right_link = get_git_link(row["script_path"])
        elif row["resource_type"] == "database_table":
            left = f'{row["database_table_path"]}'
            left_link = ""
            right = f'{row["script_path"]}'
            right_link = get_git_link(row["script_path"])
        elif row["resource_type"] == "s3_bucket":
            left = f'{row["s3_bucket"]}'
            left_link = ""
            right = f'{row["script_path"]}'
            right_link = get_git_link(row["script_path"])
    else:
        return ""

    # format to return: A[<a href='https://docs.google.com/spreadsheets/d/15YOVAJosH_fM3zODhgVe0DNux-3vZYZ0KQf7SZvshjE/edit#gid=764617962'>Mock/Live Plan Inputs</a>];

    # replace symbols that mermaid does not like
    left = strip_characters_for_mermaid(left)
    right = strip_characters_for_mermaid(right)

    if left_link != "":
        left = f"<a href='{left_link}'>{left}</a>"
    if right_link != "":
        right = f"<a href='{right_link}'>{right}</a>"

    return left, right


def log_data_pipeline(
    script_path,
    function_name,
    input_output,
    resource_type,
    spreadsheet_id="",
    spreadsheet_name="",
    sheet_name="",
    domo_table_name="",
    domo_table_id="",
    file_path="",
    database_table_path="",
    s3_bucket="",
):
    # if file does not exist write header
    if not os.path.isfile(os.path.join(data_dir, "data_pipelines.csv")):
        with open(os.path.join(data_dir, "data_pipelines.csv"), "w") as f:
            f.write(
                "datestamp,script_path,script_name,function_name,input_output,resource_type,spreadsheet_id,spreadsheet_name,sheet_name,domo_table_name,domo_table_id,file_path,file_name,sheet_link,script_link,domo_link,database_table_path,s3_bucket\n"
            )

    if script_path == "":
        return

    # write row
    if file_path != np.nan:
        file_name = os.path.basename(file_path)
    else:
        file_name = ""

    script_name = os.path.basename(script_path)
    sheet_link = get_sheet_link(spreadsheet_id)
    script_link = get_git_link(script_path)
    domo_link = get_domo_link(domo_table_id)

    with open(os.path.join(data_dir, "data_pipelines.csv"), "a") as f:
        f.write(
            f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')},{script_path},{script_name},{function_name},{input_output},{resource_type},{spreadsheet_id},{spreadsheet_name},{sheet_name},{domo_table_name},{domo_table_id},{file_path},{file_name},{sheet_link},{script_link},{domo_link},{database_table_path},{s3_bucket}\n"
        )


def increment_letter(letter):
    if letter == "":  # handle empty input
        return "A"
    elif letter[-1] != "Z":
        return letter[:-1] + chr(ord(letter[-1]) + 1)
    else:
        return increment_letter(letter[:-1]) + "A"


def data_pipe_outputs_compacted_mermaid():
    # read in csv
    df = pandas.read_csv(os.path.join(data_dir, "data_pipelines.csv"))

    # drop duplicates
    df = df.drop_duplicates(
        subset=[
            "script_path",
            "function_name",
            "input_output",
            "resource_type",
            "spreadsheet_id",
            "spreadsheet_name",
            "domo_table_name",
            "domo_table_id",
            "file_path",
            "database_table_path",
            "s3_bucket",
        ]
    )

    # sort
    df = df.sort_values(by=["script_path", "input_output", "resource_type"])

    running_mermaid_letter = "A"
    dict_defined_mermaid_chunks = (
        {}
    )  # "A[<a href='https://docs.google.com/spreadsheets/d/15YOVAJosH_fM3zODhgVe0DNux-3vZYZ0KQf7SZvshjE/edit#gid=764617962'>Mock/Live Plan Inputs</a>]": "A"
    ls_lines_to_add_to_mermaid = []
    for row in df.iterrows():

        left, right = get_mermaid_chunks(row[1])

        # if left or right contains "Active Roster" then skip
        if "Active_Roster" in left or "Active_Roster" in right:
            continue

        if left in dict_defined_mermaid_chunks.keys():
            left = dict_defined_mermaid_chunks[left]
        else:
            dict_defined_mermaid_chunks[left] = running_mermaid_letter
            left = running_mermaid_letter
            running_mermaid_letter = increment_letter(running_mermaid_letter)

        if right in dict_defined_mermaid_chunks.keys():
            right = dict_defined_mermaid_chunks[right]
        else:
            dict_defined_mermaid_chunks[right] = running_mermaid_letter
            right = running_mermaid_letter
            running_mermaid_letter = increment_letter(running_mermaid_letter)

        if f"{left} --> {right}" not in ls_lines_to_add_to_mermaid:
            ls_lines_to_add_to_mermaid.append(f"{left} --> {right}")

    # output mermaid as a file in the data folder
    with open(
        os.path.join(docs_dir, f"{repo_name}_data_pipelines_mermaid.md"),
        "w",
    ) as f:
        # setup mermaid file
        f.write("# Mermaid\n")
        f.write("\n")
        f.write("```mermaid\n")
        f.write("graph LR;\n")
        f.write("\n")

        # add each entry from the dictionary
        for key, value in dict_defined_mermaid_chunks.items():
            f.write(f"  {value}[{key}]\n")
        f.write("\n")

        # add each line from the list
        for line in ls_lines_to_add_to_mermaid:
            f.write(f"  {line}\n")

        # end mermaid file
        f.write("```\n")

    return df


# %%
## Main ##

if __name__ == "__main__":

    data_pipe_outputs_compacted_mermaid()


# %%
