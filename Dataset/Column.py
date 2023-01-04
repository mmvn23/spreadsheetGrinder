import copy
import json
import Dataset.utils
import glb_var.dict_names, glb_var.addresses
import pandas as pd
import os


class MyColumn:
    def __init__(self, var_name, standard_name, column_type, nomenclature_to_be_applied,
                 conversion_to_be_applied, space_cleaning_to_be_applied):
        self.var_name = var_name # string
        self.standard_name = standard_name # string
        self.data_type = column_type # string
        self.uom_conversion_to_be_applied = conversion_to_be_applied # bool
        self.nomenclature_to_be_applied = nomenclature_to_be_applied # bool
        self.string_cleaning_to_be_applied = space_cleaning_to_be_applied # bool

    def __str__(self):
        any_dict = self.to_dict()

        return json.dumps(any_dict)

    def to_dict(self):
        any_dict = {glb_var.dict_names.VAR_NAME: self.var_name,
                    glb_var.dict_names.STD_NAME: self.standard_name,
                    glb_var.dict_names.DATA_TYPE: str(self.data_type),
                    glb_var.dict_names.CONVERSION_TO_BE_APPLIED: self.uom_conversion_to_be_applied,
                    glb_var.dict_names.NOMENCLATURE_TO_BE_APPLIED: self.nomenclature_to_be_applied,
                    glb_var.dict_names.STRING_CLEANING_TO_BE_APPLIED: self.string_cleaning_to_be_applied
                    }
        return any_dict

    def write_as_json(self):
        address = glb_var.addresses.CLMN_JSON_ADDRESS + '/' + self.var_name + glb_var.addresses.JSON_FORMAT
        with open(address, "w") as outfile:
            json.dump(self.to_dict(), outfile, indent=glb_var.addresses.JSON_INDENT)
        return

    @staticmethod
    def load_json_list_from_spreadsheet(address):
        df_clmn_list = pd.read_excel(address)

        for ii in range(0,len(df_clmn_list)):
            MyColumn.load_from_dataframe_row(df_clmn_list.iloc[[ii]])

        return

    @staticmethod
    def load_from_dataframe_row(row):
        any_clmn = MyColumn(var_name=row[glb_var.dict_names.VAR_NAME_CLMN].to_list()[0],
                            standard_name=row[glb_var.dict_names.STANDARD_NAME_CLMN].to_list()[0],
                            column_type=row[glb_var.dict_names.DATA_TYPE].to_list()[0],
                            nomenclature_to_be_applied=row[glb_var.dict_names.NOMENCLATURE_TO_BE_APPLIED].to_list()[0],
                            conversion_to_be_applied=row[glb_var.dict_names.CONVERSION_TO_BE_APPLIED].to_list()[0],
                            space_cleaning_to_be_applied=
                            row[glb_var.dict_names.STRING_CLEANING_TO_BE_APPLIED].to_list()[0])
        any_clmn.write_as_json()

        return

    @staticmethod
    def load_from_json(var_name):
        address = glb_var.addresses.CLMN_JSON_ADDRESS + '/' + var_name + glb_var.addresses.JSON_FORMAT
        with open(address, "r") as outfile:
            content = outfile.read()

        any_dict = json.loads(content)
        any_clmn = MyColumn.from_dict(any_dict)

        return any_clmn

    @staticmethod
    def from_dict(any_dict):
        any_column = MyColumn(var_name=any_dict[glb_var.dict_names.VAR_NAME],
                              standard_name=any_dict[glb_var.dict_names.STD_NAME],
                              column_type=any_dict[glb_var.dict_names.DATA_TYPE],
                              nomenclature_to_be_applied=any_dict[glb_var.dict_names.NOMENCLATURE_TO_BE_APPLIED],
                              conversion_to_be_applied=any_dict[glb_var.dict_names.CONVERSION_TO_BE_APPLIED],
                              space_cleaning_to_be_applied=any_dict[glb_var.dict_names.STRING_CLEANING_TO_BE_APPLIED])

        return any_column

    @staticmethod
    def to_dict_list(mycolumn_dict_list):
        any_dict_list = []

        for any_column_dict in mycolumn_dict_list:
            dict_to_append = copy.deepcopy(any_column_dict)
            dict_to_append[glb_var.dict_names.MYCOLUMN] = any_column_dict[glb_var.dict_names.MYCOLUMN].to_dict()
            any_dict_list.append(dict_to_append)

        return any_dict_list

