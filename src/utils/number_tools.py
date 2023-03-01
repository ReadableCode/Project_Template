# %%
## Imports ##

if __name__ != "__main__":
    print(f"Importing {__name__}")

import random
import pygsheets
import sys
from statsmodels.tsa.arima.model import ARIMA
import pandas as pd
import numpy as np


# %%
## Repair and Convert Functions ##


def remove_percent_from_val(value, max_expected_value=100):
    if value == "":
        value = np.nan
        return value
    if value == "-%":
        value = 0
        return value
    try:
        value = float(value.strip("%"))
    except Exception as e:
        print(f"Error: {e} when trying to remove % from {value}")
        pass
    if value > 0.5 and value > max_expected_value:
        value = value / 100
    return value


def remove_percent_from_val_safe(value):
    if value == "" or value == "-%":
        return 0

    # if value is already a float or int then return it
    if isinstance(value, (float, int)):
        return value
    if "%" in value:
        value = value.replace("%", "")
        value = float(value)
        value = value / 100
    else:
        value = float(value)
    return value


def remove_percent_from_val_no_div(value):
    if value == "-%":
        value = 0
        return value
    try:
        value = float(value.strip("%"))
    except:
        pass
    return value


def force_to_number(value):
    if value == "":
        return 0
    try:
        if "," in value:
            value = value.replace(",", "")
        if "$" in value:
            value = value.replace("$", "")
    except:
        pass
    try:
        value = float(value)
    except:
        pass
    return value


def divide_blank(x, y):
    if y == 0:
        return 0
    else:
        return x / y


# %%
