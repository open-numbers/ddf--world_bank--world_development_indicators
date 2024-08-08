"""a script to produce the data in economy_entity_domain.xlsx"""

import pandas as pd
import polars as pl

input_file = "../source/CLASS.xlsx"


data = pd.read_excel(input_file, sheet_name="Groups")

data = pl.from_pandas(data)

data.select(
    pl.col(["GroupCode", "GroupName"]).unique()
)

groups = data.select(
    pl.col(["GroupCode", "GroupName"]).unique(maintain_order=True)
)

groups.write_csv('./groups.csv')

"""
And then, copy the data into economy_entity_domain.xlsx, and use vlookup to update the data.
"""
