import copy
import pandas as pd
import numpy as np
import utils.general as ut
import variables.setup_column as stp_clmn
import variables.general
import variables.type as tp
# import variables.DIAGEO_setup.file as dg_stp
import variables.var_column as clmn
import variables.dict as dct
import variables.error_message as err_msg
import variables.general as gen


class BaseDataset:
    def __init__(self, name, any_filepath, df_setup_for_column, mytimestamp=pd.Timestamp(year=1900, month=1, day=1),
                 date_parser=variables.general.date_parser_to_save):
        self.name = ut.assign_type(name, tp.my_string) # string OK
        self.filepath = ut.treat_filepath(any_filepath) # directory  OK
        self.mytimestamp = ut.assign_type(mytimestamp, tp.my_date)
        # timestamp should be updated when dataframe is loaded OK
        self.df_setup_for_column = df_setup_for_column  # dataframe with input info
        # Var_NAME Input Input_strategy multiplier standard_name Type UoM_conversion Nomenclature String_cleaning
        # X
        # Y
        # Z
        self.dataframe = pd.DataFrame() # Dataframe
        # datetime.datetime.now()
        # self.mytimestamp = pd.Timestamp(year=mytimestamp.year, month=mytimestamp.month, day=mytimestamp.day)

    def __str__(self):
        out_str = "\n\n BASE->\n" \
                  "Name: {name} \n" \
                  "Filepath: {filepath}\n"\
                  "Timestamp: {mytimestamp}\n" \
            .format(name=self.name,
                    filepath=self.filepath,
                    mytimestamp=self.mytimestamp)

        return out_str

    def get_any_column_list(self, target_column_list, add_column_list_to_filter=[], add_value_list_to_filter=[]):

        column_list_to_filter = []
        value_list_to_filter = []
        column_list = ut.get_value_from_dataframe(input_dataframe=self.df_setup_for_column,
                                                  target_column_list=target_column_list,
                                                  column_list_to_filter=column_list_to_filter +
                                                                        add_column_list_to_filter,
                                                  value_list_to_filter=value_list_to_filter + add_value_list_to_filter,
                                                  return_list=True)
        return column_list

    def get_var_index_list(self):
        index_list = self.get_any_column_list(target_column_list=[stp_clmn.clmn_var_name],
                                              add_column_list_to_filter=[stp_clmn.index],
                                              add_value_list_to_filter=[True])
        return index_list

    def get_var_column_list(self):
        var_column_list = self.get_any_column_list(target_column_list=[stp_clmn.clmn_var_name],
                                                   add_column_list_to_filter=[],
                                                   add_value_list_to_filter=[])
        return var_column_list

    def get_var_column_not_index_list(self):
        var_column_list = self.get_any_column_list(target_column_list=[stp_clmn.clmn_var_name],
                                                   add_column_list_to_filter=[stp_clmn.index],
                                                   add_value_list_to_filter=[False])
        return var_column_list

    def assure_column_integrity(self):
        self.dataframe = self.dataframe[self.get_var_column_not_index_list()]
        return

    def prepare_string_clmn_for_merge(self, any_column, reset_index, assure_column_integrity=True):
        if reset_index:
            self.remove_index()

        self.dataframe[any_column] = self.dataframe.apply(lambda row: ut.trim_and_lower_string(row[any_column]), axis=1)

        if reset_index:
            self.apply_standard_index()

        if assure_column_integrity:
            self.assure_column_integrity()
        return

    def convert_uom(self, any_clmn, numerator_dict, denominator_dict):
        # numerator_dict = {dct.any_mtx_conversion: any_mtx_uom_conversion,
        #                   dct.old_uom: 'a',
        #                   dct.new_uom: 'b'}
        temp_multiplier_numerator = 'temp numerator'
        temp_multiplier_denominator = 'temp denominator'
        self.add_uom_multiplier(any_mtx_conversion=numerator_dict[dct.any_mtx_conversion],
                                multiplier_clmn=temp_multiplier_numerator,
                                original_clmn=numerator_dict[dct.old_uom],
                                new_clmn=numerator_dict[dct.new_uom])

        if len(denominator_dict) > 0:
            self.add_uom_multiplier(any_mtx_conversion=denominator_dict[dct.any_mtx_conversion],
                                    multiplier_clmn=temp_multiplier_denominator,
                                    original_clmn=denominator_dict[dct.old_uom],
                                    new_clmn=denominator_dict[dct.new_uom])
        else:
            self.dataframe[temp_multiplier_denominator] = 1

        df_error_to_concat, self.dataframe = self.filter_nan_and_update_error(any_column=temp_multiplier_numerator,
                                                                              error_message=err_msg.uom_conversion)

        self.df_error = pd.concat([self.df_error, df_error_to_concat])

        df_error_to_concat, self.dataframe = self.filter_nan_and_update_error(any_column=temp_multiplier_denominator,
                                                                              error_message=err_msg.uom_conversion)
        self.df_error = pd.concat([self.df_error, df_error_to_concat])

        self.multiply_column_by_another(multiplier_clmn=any_clmn, multiplicand_clmn=temp_multiplier_numerator,
                                        product_clmn=any_clmn)
        self.divide_column_by_another(dividend_clmn=any_clmn, divisor_clmn=temp_multiplier_denominator,
                                      result_clmn=any_clmn)

        df_error_to_concat, self.dataframe = self.filter_nan_and_update_error(any_column=any_clmn,
                                                                              error_message=err_msg.uom_conversion)

        self.df_error = pd.concat([self.df_error, df_error_to_concat])

        return

    def add_uom_multiplier(self, any_mtx_conversion, multiplier_clmn, original_clmn, new_clmn):

        self.dataframe[multiplier_clmn] = self.dataframe.apply(lambda row: ut.get_multiplier_from_mtx_conversion(
                                                                               any_mtx_conversion=any_mtx_conversion,
                                                                               original=row[original_clmn],
                                                                               new=row[new_clmn],
                                                                               part_number=row[clmn.part_number_code]),
                                                                            axis=1)
        return

# NEO PANDAS
    def drop_column_list(self, column_list=[], drop_column_list=True, keep_column_list=False, reset_index=False):
        if reset_index:
            self.remove_index()

        if len(column_list) == 0 & keep_column_list:
            column_list = self.get_any_column_list(target_column_list=[stp_clmn.clmn_var_name],
                                                   add_column_list_to_filter=[],
                                                   add_value_list_to_filter=[])
        if drop_column_list & keep_column_list:
            drop_column_list = False

        if drop_column_list:
            self.dataframe.drop(column_list, axis=1, inplace=True)

        if keep_column_list:
            self.dataframe = self.dataframe[column_list]

        if reset_index:
            self.apply_standard_index()
        return

    def reset_index(self, new_index_list=[]):
        self.dataframe.reset_index(inplace=True)

        if len(new_index_list) > 0:
            self.dataframe.set_index(new_index_list, inplace=True)
        return

    def remove_index(self):
        self.reset_index()
        return

    def apply_standard_index(self):
        self.reset_index(new_index_list=self.get_var_index_list())
        return

    def concat_base_dataset(self, any_basedataset):
        self.concat_dataframe(any_dataframe=any_basedataset.dataframe)
        return

    def concat_dataframe(self, any_dataframe):
        self.dataframe = pd.concat([self.dataframe, any_dataframe])
        return

    def trim_date(self, initial_date, end_date, date_clmn, reset_index=False):
        if reset_index:
            self.remove_index()

        cond_initial = self.dataframe[date_clmn] >= initial_date
        cond_end = self.dataframe[date_clmn] <= end_date
        cond = cond_initial & cond_end

        self.dataframe = self.dataframe.loc[cond]
        if reset_index:
            self.apply_standard_index()
        return

    def print(self, dataframe_and_header=False):
        if dataframe_and_header:
            print(self)

        print(self.dataframe)

        return

    def filter_based_on_column(self, any_column, value_list, keep_value_in=True):
        cond = self.dataframe[any_column].isin(value_list)

        self.dataframe = self.dataframe[cond]

        return

# ARITHMETIC
    def sum_columns(self, result_clmn, summand_clmn_list, reset_index=False):
        if reset_index:
            pass
            # implement index reset

        for any_summand_clmn in summand_clmn_list:
            self.dataframe[result_clmn] = self.dataframe[result_clmn] + self.dataframe[any_summand_clmn]

        if reset_index:
            pass
            # implement index reset
        return

    def assign_constant_to_column(self, any_clmn, value):
        self.dataframe[any_clmn] = value
        return

    def divide_column_by_another(self, dividend_clmn, divisor_clmn, result_clmn):
        self.dataframe[result_clmn] = self.dataframe[dividend_clmn] / self.dataframe[divisor_clmn]
        return

    def multiply_column_by_another(self, multiplier_clmn, multiplicand_clmn, product_clmn):
        self.dataframe[product_clmn] = self.dataframe[multiplier_clmn] * self.dataframe[multiplicand_clmn]
        return

    def check_if_column_list_part_of_dataframe(self, original_column_list):
        new_column_list = []

        for any_clmn in original_column_list:
            if any_clmn in self.dataframe.columns:
                new_column_list = new_column_list + [any_clmn]
        return new_column_list

    def filter_nan_and_update_error(self, any_column, error_message=''):
        # refactor later
        # self.dataframe['temp'] = self.dataframe[any_column]
        # self.dataframe['temp'].fillna(value=gen.void, inplace=True)

        cond_nan_pd = pd.isnull(self.dataframe[any_column])
        cond_nan = cond_nan_pd
        # cond_float_nan = self.dataframe['temp'].isin(['<NA>', 'NaN', 'None', '', 'nan', gen.void])
        # cond_nan = cond_nan_pd | cond_float_nan

        df_error = self.dataframe.loc[cond_nan]

        if len(error_message) > 1:
            df_error[stp_clmn.error_message] = error_message

        dataframe = self.dataframe.loc[~cond_nan]
        # self.drop_column_list(column_list=['temp'], keep_column_list=False)
        return df_error, dataframe

    def merge_dataframe(self, original_right_dataset, desired_column_list, left_on_list, right_on_list,
                        reset_left_index=False, reset_right_index=False, drop_right_on_list=True):
        if reset_left_index:
            self.reset_index()
        right_dataframe = copy.deepcopy(original_right_dataset.dataframe)
        if reset_right_index:
            right_dataframe.reset_index(inplace=True)
        right_dataframe = right_dataframe[desired_column_list + right_on_list]
        self.dataframe = self.dataframe.merge(right_dataframe, how='left', left_on=left_on_list,
                                              right_on=right_on_list)
        if drop_right_on_list:
            self.drop_column_list(column_list=right_on_list, drop_column_list=True)
        if reset_left_index:
            self.apply_standard_index()
        return

    def rename_column_list(self, old_clmn_list, new_clmn_list, reset_index=False):
        if reset_index:
            self.remove_index()
        my_dict = dict(zip(old_clmn_list, new_clmn_list))
        self.dataframe.rename(my_dict, inplace=True, axis=1)
        if reset_index:
            self.apply_standard_index()
        return

    def get_row_number(self):
        return len(self.dataframe.index)

    @staticmethod
    def create_list_of_copied_base_datasets(any_base_dataset, n_elements):
        base_dataset_list = []
        for ii in range(0, n_elements):
            base_dataset_to_append = copy.deepcopy(any_base_dataset)
            base_dataset_list = base_dataset_list + [base_dataset_to_append]
        return base_dataset_list


def get_dataframe_filepath(name, any_stp_dict, is_for_setup_clmn=True,
                           is_for_error=False, is_for_archive=False):
    if is_for_setup_clmn:
        any_filepath = ut.get_filepath(root=any_stp_dict[dct.root_folder], folder=any_stp_dict[dct.df_setup_folder],
                                       file=name, any_format=variables.general.csv)
    elif is_for_error:
        any_filepath = ut.get_filepath(root=any_stp_dict[dct.root_folder], folder=any_stp_dict[dct.df_error_folder],
                                       file=name, any_format=variables.general.csv)
    elif is_for_archive:
        any_filepath = ut.get_filepath(root=any_stp_dict[dct.root_folder], folder=any_stp_dict[dct.archive_folder],
                                       file=name, any_format=variables.general.csv)
    else:
        any_filepath = ut.get_filepath(root=any_stp_dict[dct.root_folder], folder=any_stp_dict[dct.dataframe_folder],
                                       file=name, any_format=variables.general.csv)
    return any_filepath
