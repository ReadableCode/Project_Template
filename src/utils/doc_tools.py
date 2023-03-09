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

from utils.google_tools import WriteToSheets
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


def get_mermaid_from_row(row):
    # Add values to the "mermaid" column based on the value of "input_output" column
    if row["input_output"] == "output":
        if row["resource_type"] == "google_sheet":
            return f'{row["script_path"]} --> {row["spreadsheet_name"]}/{row["sheet_name"]}'
        elif row["resource_type"] == "domo_table":
            return f'{row["script_path"]} --> {row["domo_table_name"]}'
        elif row["resource_type"] == "file":
            return f'{row["script_path"]} --> {row["file_name"]}'
    elif row["input_output"] == "input":
        if row["resource_type"] == "google_sheet":
            return f'{row["spreadsheet_name"]}/{row["sheet_name"]} --> {row["script_path"]}'
        elif row["resource_type"] == "domo_table":
            return f'{row["domo_table_name"]} --> {row["script_path"]}'
        elif row["resource_type"] == "file":
            return f'{row["file_name"]} --> {row["script_path"]}'
    else:
        return ""


# example log use:
# log_data_pipeline(
#     script_path=os.path.join(grandparent_dir, "src", "parse_repo.py"),
#     function_name="parse_repo",
#     input_output="output",
#     resource_type="google_sheet",
#     spreadsheet_id="18UojhPgDwIHJECmeNYAx-qao4acsRhy4zwa2Pn0IfYU",
#     spreadsheet_name="Repo_Maps",
#     sheet_name=repo_name,
#     domo_table_name="",
#     domo_table_id="",
#     file_path="",
# )


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
):
    # if file does not exist write header
    if not os.path.isfile(os.path.join(data_dir, "data_pipelines.csv")):
        with open(os.path.join(data_dir, "data_pipelines.csv"), "w") as f:
            f.write(
                "datestamp,script_path,script_name,function_name,input_output,resource_type,spreadsheet_id,spreadsheet_name,sheet_name,domo_table_name,domo_table_id,file_path,file_name,sheet_link,script_link,domo_link,mermaid\n"
            )

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
            f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')},{script_path},{script_name},{function_name},{input_output},{resource_type},{spreadsheet_id},{spreadsheet_name},{sheet_name},{domo_table_name},{domo_table_id},{file_path},{file_name},{sheet_link},{script_link},{domo_link},\n"
        )


def data_pipe_outputs():
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
            "sheet_name",
            "domo_table_name",
            "domo_table_id",
            "file_path",
        ]
    )

    df["mermaid"] = df.apply(get_mermaid_from_row, axis=1)

    # output mermaid as a file in the data folder
    with open(
        os.path.join(data_dir, f"{repo_name}_data_pipelines_mermaid.md"), "w"
    ) as f:
        f.write("# Mermaid\n")
        f.write("\n")
        f.write("```mermaid\n")
        f.write("graph LR;\n")
        f.write("\n")

        for script in df["script_name"].unique():
            for row in df[df["script_name"] == script].iterrows():
                f.write(f'  {row[1]["mermaid"]}')
                f.write("\n")

        f.write("```\n")

    # WriteToSheets
    WriteToSheets(
        "Repo_Maps",
        repo_name,
        df,
    )


# %%
## Main ##

if __name__ == "__main__":

    data_pipe_outputs()


# %%
