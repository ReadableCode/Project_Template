# %%
## Paths ##

if __name__ != "__main__":
    print(f"Importing {__name__}")

import os
from os.path import expanduser
import sys

home_dir = expanduser("~")

file_dir = os.path.dirname(os.path.realpath(__file__))
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
grandparent_dir = os.path.dirname(parent_dir)
great_grandparent_dir = os.path.dirname(grandparent_dir)

data_dir = os.path.join(grandparent_dir, "data")
trigger_dir = os.path.join(grandparent_dir, "triggers")
data_dir_db_mirror = os.path.join(
    great_grandparent_dir, "Labor_Planning", "data_db_mirror"
)

src_dir = os.path.join(grandparent_dir, "src")

sys.path.append(src_dir)

if __name__ == "__main__":
    print(f"home_dir: {home_dir}")
    print(f"file_dir: {file_dir}")
    print(f"parent_dir: {parent_dir}")
    print(f"grandparent_dir: {grandparent_dir}")
    print(f"data_dir: {data_dir}")

# %%
