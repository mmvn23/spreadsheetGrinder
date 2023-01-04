import pandas as pd
import datetime
import glb_var.addresses
import json


class BaseDataset:
    def __init__(self, name, any_directory, column_list, mytimestamp=datetime.datetime.now()):
        self.name = name # string
        self.column_list = column_list # list of strings
        self.directory = any_directory # directory
        self.mytimestamp = mytimestamp # timestamp
        self.dataframe = pd.DataFrame() # Dataframe

    def __str__(self):
        out_str = "\n\n*******************************\n" \
                  "Name: {name} \n" \
                  "Column_list: {column_list}\n" \
                  "Directory: {directory}\n"\
                  "Timestamp: {mytimestamp}\n" \
            .format(name=self.name,
                    column_list=self.column_list,
                    directory=self.directory,
                    mytimestamp=self.mytimestamp)

        return out_str
