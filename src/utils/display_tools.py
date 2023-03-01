from tabulate import tabulate
import datetime
import json
from pprint import pprint


def pprint_df(dframe, showindex=False, num_cols=None):
    """
    Pretty print a pandas dataframe
    :param dframe: pandas dataframe
    :param showindex: boolean to show the index
    :param num_cols: number of columns to print
    :return: None
    """
    if num_cols is not None:
        print(
            tabulate(
                dframe.iloc[:, :num_cols],
                headers="keys",
                tablefmt="psql",
                showindex=showindex,
            )
        )
    else:
        print(tabulate(dframe, headers="keys", tablefmt="psql", showindex=showindex))


def print_logger(message):
    """
    Print a message with a timestamp
    :param message: message to print
    :return: None
    """
    print(f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - {message}")


def pprint_ls(ls, ls_title="List"):
    """
    Pretty print a list
    :param ls: list to print
    :param ls_title: title of the list
    :return: None
    """
    # print a title box and a box that centers the title and left aligns each item of the list on a new line

    # if list is empty return
    if len(ls) == 0:
        item_max_len = 0
    else:
        item_max_len = max([len(item) for item in ls])

    # get the longest item in the list
    max_len = max(item_max_len, len(ls_title)) + 8

    # print the top of the box
    print(f"{'-' * (max_len + 4)}")

    # print the title with padding
    print(f"| {ls_title.center(max_len)} |")

    # print the bottom of the title box
    print(f"{'-' * (max_len + 4)}")

    if item_max_len == 0:
        print(f"| {'Empty'.ljust(max_len)} |")
    else:
        for item in ls:
            print(f"| {item.ljust(max_len)} |")

    # print the bottom of the list box
    print(f"{'-' * (max_len + 4)}")


def pprint_dict(dict):
    """
    Pretty print a dictionary
    :param dict: dictionary to print
    :return: None
    """
    pprint(json.dumps(dict, indent=4, sort_keys=True, default=str))
