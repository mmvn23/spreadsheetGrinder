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

    def get_current_column_list(self):
        column_list = list(self.dataframe.columns.values.tolist())

        return column_list

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

    def apply_uom_conversion_to_column(self, any_column, numerator_dict, denominator_dict={}):
        # numerator_dict = {dct.any_mtx_item: any_mtx_item, empty_dataframe if new uom already in the dataframe
        #                   dct.any_mtx_conversion: any_mtx_uom_conversion,
        #                   dct.key_clmn: clmn.part_number_code;
        #                   dct.old_uom: 'a',
        #                   dct.new_uom: 'b'}
        numerator_dict[dct.any_mtx_item].remove_index()
        temp_uom_numerator_clmn = 'temp_uom_numerator_clmn'
        temp_uom_denominator_clmn = 'temp_uom_denominator_clmn'

        self.remove_index()
        if len(numerator_dict[dct.any_mtx_item].dataframe.index) > 0:
            self.merge_dataframe(original_right_dataset=numerator_dict[dct.any_mtx_item],
                                 desired_column_list=[numerator_dict[dct.new_uom]],
                                 left_on_list=[numerator_dict[dct.key_clmn]], right_on_list=[numerator_dict[dct.key_clmn]],
                                 reset_left_index=False, reset_right_index=False, drop_right_on_list=False)
            self.rename_column_list(old_clmn_list=[numerator_dict[dct.old_uom]],
                                    new_clmn_list=[temp_uom_numerator_clmn])
            self.rename_column_list(old_clmn_list=[numerator_dict[dct.new_uom]],
                                    new_clmn_list=[numerator_dict[dct.old_uom]])
            numerator_dict[dct.new_uom] = numerator_dict[dct.old_uom]
            numerator_dict[dct.old_uom] = temp_uom_numerator_clmn

        if len(denominator_dict) > 0:
            denominator_dict[dct.any_mtx_item].remove_index()
            if len(denominator_dict[dct.any_mtx_item].dataframe.index) > 0:
                self.merge_dataframe(original_right_dataset=denominator_dict[dct.any_mtx_item],
                                     desired_column_list=[denominator_dict[dct.new_uom]],
                                     left_on_list=[denominator_dict[dct.key_clmn]],
                                     right_on_list=[denominator_dict[dct.key_clmn]],
                                     reset_left_index=False, reset_right_index=False, drop_right_on_list=False)
                self.rename_column_list(old_clmn_list=[denominator_dict[dct.old_uom]],
                                        new_clmn_list=[temp_uom_denominator_clmn])
                self.rename_column_list(old_clmn_list=[denominator_dict[dct.new_uom]],
                                        new_clmn_list=[denominator_dict[dct.old_uom]])

                denominator_dict[dct.new_uom] = denominator_dict[dct.old_uom]
                denominator_dict[dct.old_uom] = temp_uom_denominator_clmn

        self.convert_uom(any_clmn=any_column, numerator_dict=numerator_dict, denominator_dict=denominator_dict)
        self.apply_standard_index()
        # self.assure_column_integrity()

        return

    def convert_uom(self, any_clmn, numerator_dict, denominator_dict):
        # numerator_dict = {dct.any_mtx_item: any_mtx_item, empty_dataframe if new uom already in the dataframe
        #                   dct.any_mtx_conversion: any_mtx_uom_conversion,
        #                   dct.old_uom: 'a',
        #                   dct.new_uom: 'b'}
        temp_multiplier_numerator = 'temp numerator'
        temp_multiplier_denominator = 'temp denominator'

        self.add_uom_multiplier(any_mtx_conversion=numerator_dict[dct.any_mtx_conversion],
                                multiplier_clmn=temp_multiplier_numerator,
                                original_clmn=numerator_dict[dct.old_uom],
                                new_clmn=numerator_dict[dct.new_uom],
                                key_clmn=numerator_dict[dct.key_clmn])

        if len(denominator_dict) > 0:
            self.add_uom_multiplier(any_mtx_conversion=denominator_dict[dct.any_mtx_conversion],
                                    multiplier_clmn=temp_multiplier_denominator,
                                    original_clmn=denominator_dict[dct.old_uom],
                                    new_clmn=denominator_dict[dct.new_uom],
                                    key_clmn=denominator_dict[dct.key_clmn])
        else:
            self.dataframe[temp_multiplier_denominator] = 1

        # df_error_to_concat, self.dataframe = self.filter_nan_dataframe(any_column=temp_multiplier_numerator,
        #                                                                error_message=err_msg.uom_conversion)
        #
        # self.df_error = pd.concat([self.df_error, df_error_to_concat])
        #
        # df_error_to_concat, self.dataframe = self.filter_nan_dataframe(any_column=temp_multiplier_denominator,
        #                                                                error_message=err_msg.uom_conversion)
        # self.df_error = pd.concat([self.df_error, df_error_to_concat])

        self.multiply_column_by_another(multiplier_clmn=any_clmn, multiplicand_clmn=temp_multiplier_numerator,
                                        product_clmn=any_clmn)
        self.divide_column_by_another(dividend_clmn=any_clmn, divisor_clmn=temp_multiplier_denominator,
                                      result_clmn=any_clmn)

        # df_error_to_concat, self.dataframe = self.filter_nan_dataframe(any_column=any_clmn,
        #                                                                error_message=err_msg.uom_conversion)
        #
        # self.df_error = pd.concat([self.df_error, df_error_to_concat])
        self.drop_column_list(column_list=[temp_multiplier_numerator, temp_multiplier_denominator])
        return

    # refactor hard coded
    def add_uom_multiplier(self, any_mtx_conversion, multiplier_clmn, original_clmn, new_clmn,
                           key_clmn=clmn.part_number_code):

        self.dataframe[multiplier_clmn] = self.dataframe.apply(lambda row: ut.get_multiplier_from_mtx_conversion(
                                                                               any_mtx_conversion=any_mtx_conversion,
                                                                               original=row[original_clmn],
                                                                               new=row[new_clmn],
                                                                               part_number=row[key_clmn]),
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

        drop_column_list = ['index', 'level_l0']

        for any_clmn_to_drop in drop_column_list:
            if any_clmn_to_drop in self.dataframe.columns:
                self.drop_column_list(column_list=[any_clmn_to_drop])

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

    def concat_base_dataset_list(self, any_basedataset_list):
        for any_basedataset in any_basedataset_list:
            self.concat_base_dataset(any_basedataset)
        return

    def concat_dataframe(self, any_dataframe):
        self.dataframe = pd.concat([self.dataframe, any_dataframe])
        return

    def concat_dataframe_list(self, any_dataframe_list):
        for any_dataframe in any_dataframe_list:
            self.concat_dataframe(any_dataframe)
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

    def filter_nan_on_column(self, any_column, keep_nan=True):
        cond_nan = pd.isnull(self.dataframe[any_column])

        if keep_nan:
            self.dataframe = self.dataframe.loc[cond_nan]
        else:
            self.dataframe = self.dataframe.loc[~cond_nan]
        return

    def get_terms_of_a_column(self, any_column, reset_index=False, remove_repetitive_terms=False):
        if reset_index:
            self.remove_index()

        if remove_repetitive_terms:
            term_list = list(set(self.dataframe[any_column]))
        else:
            term_list = list(self.dataframe[any_column])

        if reset_index:
            self.apply_standard_index()
        return term_list

    def split_based_on_column_categories(self, any_column):
        term_list = self.get_terms_of_a_column(any_column)
        any_base_dataset_list = self.create_list_of_copied_base_datasets(n_elements=len(term_list))
        ii = 0

        any_base_dataset_list = BaseDataset.adjust_name_on_base_dataset_list(
            any_base_dataset_list=any_base_dataset_list, term_list=term_list, separator=' - ')

        for any_base_dataset in any_base_dataset_list:
            any_base_dataset.filter_based_on_column(any_column=any_column, value_list=[term_list[ii]])
            ii = ii + 1

        return any_base_dataset_list

    def melt_dataframe(self, id_vars, value_vars, var_name, value_name, reset_index=False):
        if reset_index:
            self.remove_index()

        self.dataframe = pd.melt(self.dataframe, id_vars=id_vars, value_vars=value_vars, var_name=var_name,
                                 value_name=value_name, ignore_index=True)

        if reset_index:
            self.apply_standard_index()
        return

    def sort_column(self, any_column, ascending=True, reset_index=False):
        if reset_index:
            self.remove_index()

        self.dataframe = self.dataframe.sort_values(by=any_column, ascending=ascending)

        if reset_index:
            self.apply_standard_index()
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

    def filter_nan_dataframe(self, any_column, error_message='', update_df_error=True):
        # refactor later

        cond_nan_pd = pd.isnull(self.dataframe[any_column])
        cond_nan = cond_nan_pd

        if update_df_error:
            df_error = self.dataframe.loc[cond_nan]

            if len(error_message) > 1:
                df_error[stp_clmn.error_message] = error_message
        else:
            df_error = self.df_error

        dataframe = self.dataframe.loc[~cond_nan]

        return df_error, dataframe

    def filter_nan_base_dataset(self, any_column, error_message, reset_index=False, update_df_error=True):

        if reset_index:
            self.remove_index()

        self.df_error, self.dataframe = self.filter_nan_dataframe(any_column, error_message=error_message,
                                                                  update_df_error=update_df_error)
        if reset_index:
            self.apply_standard_index()
        return

    def merge_dataframe(self, original_right_dataset, desired_column_list, left_on_list, right_on_list,
                        reset_left_index=False, reset_right_index=False, drop_right_on_list=True,
                        multilevel_datamatrix=False):
        if reset_left_index:
            self.reset_index()
        right_dataframe = copy.deepcopy(original_right_dataset.dataframe)
        if reset_right_index:
            right_dataframe.reset_index(inplace=True)

        if not multilevel_datamatrix:
            right_dataframe = right_dataframe[desired_column_list + right_on_list]

        self.dataframe = self.dataframe.merge(right_dataframe, how='left', left_on=left_on_list,
                                              right_on=right_on_list)
        if drop_right_on_list and not multilevel_datamatrix:
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

    # def create_list_of_copied_base_datasets(self, n_elements):
    #     base_dataset_list = BaseDataset.create_list_of_copied_base_datasets(self, n_elements)
    #     return base_dataset_list

    def create_list_of_copied_base_datasets(self, n_elements):
        base_dataset_list = []
        for ii in range(0, n_elements):
            base_dataset_to_append = copy.deepcopy(self)
            base_dataset_list = base_dataset_list + [base_dataset_to_append]
        return base_dataset_list

    def get_date_interval_from_fiscal_year(self, initial_date_clmn, end_date_clmn, fiscal_year_clmn):
        self.dataframe[initial_date_clmn] = self.dataframe.apply(lambda row:
                                                                 ut.get_initial_date_from_period_fiscal_year(period=1,
                                                                                    fiscal_year=row[fiscal_year_clmn]),
                                                                 axis=1)
        self.dataframe[end_date_clmn] = self.dataframe.apply(lambda row:
                                                                 ut.get_end_date_from_period_fiscal_year(period=1,
                                                                                    fiscal_year=row[fiscal_year_clmn]),
                                                                 axis=1)
        return

    def is_index_standard(self):
        is_standard = ut.are_lists_equal(self.dataframe.index.names, self.get_var_index_list())
        return is_standard

    def search_string_column_for_pattern(self, target_column, pattern, any_column, value):
        self.dataframe[any_column] = self.dataframe.apply(lambda row: ut.search_pattern_on_string(pattern=pattern,
                                                                                          term=row[target_column],
                                                                                          value=value),
                                                                 axis=1)

        return

    @staticmethod
    def adjust_name_on_basedataset_dataset_list(any_base_dataset_list, term_list,
                                                separator=variables.general.folder_separator):
        ii = 0
        original_filepath = copy.deepcopy(any_base_dataset_list[ii].filepath)
        for any_base_dataset in any_base_dataset_list:
            try:
                any_base_dataset.source_dict = ut.assign_type_to_dict({dct.name: term_list[ii],
                                                                      dct.my_timestamp: any_base_dataset.source_dict[
                                                                          dct.my_timestamp]},
                                                          [tp.my_string, tp.my_date], date_parser=[
                        variables.general.date_parser_to_save])
            except AttributeError:
                pass

            # create new function to adjust filepath depending on separator
            any_base_dataset.filepath = ut.append_filepath(original_filepath, separator, term=term_list[ii])
            # any_base_dataset.filepath = ut.treat_filepath(original_filepath + separator
            #                                               + term_list[ii])
            ii = ii + 1
        return any_base_dataset_list

    @staticmethod
    def adjust_name_on_base_dataset_list(any_base_dataset_list, term_list,
                                         separator=variables.general.folder_separator):
        ii = 0
        original_name = any_base_dataset_list[0].name

        for any_base_dataset in any_base_dataset_list:
            try:
                any_base_dataset.source_dict = ut.assign_type_to_dict({dct.name: term_list[ii],
                                                                      dct.my_timestamp: any_base_dataset.source_dict[
                                                                          dct.my_timestamp]},
                                                          [tp.my_string, tp.my_date], date_parser=[
                        variables.general.date_parser_to_save])
            except AttributeError:
                pass

            any_base_dataset.name = original_name + separator + term_list[ii]

            ii = ii + 1

        return any_base_dataset_list

    @staticmethod
    def adjust_filepath_on_base_dataset_list(any_base_dataset_list, term_list,
                                         separator=variables.general.folder_separator):
        ii = 0
        original_filepath = copy.deepcopy(any_base_dataset_list[ii].filepath)

        for any_base_dataset in any_base_dataset_list:
            try:
                any_base_dataset.source_dict = ut.assign_type_to_dict({dct.name: term_list[ii],
                                                                      dct.my_timestamp: any_base_dataset.source_dict[
                                                                          dct.my_timestamp]},
                                                          [tp.my_string, tp.my_date], date_parser=[
                        variables.general.date_parser_to_save])
            except AttributeError:
                pass

            any_base_dataset.filepath = ut.append_filepath(original_filepath, separator, term=term_list[ii])

            ii = ii + 1

        return any_base_dataset_list

    @staticmethod
    def adjust_sheet_on_base_dataset_list(any_base_dataset_list, term_list):
        ii = 0
        original_filepath = copy.deepcopy(any_base_dataset_list[ii].filepath)

        for any_base_dataset in any_base_dataset_list:
            try:
                any_base_dataset.source_dict = ut.assign_type_to_dict({dct.name: term_list[ii],
                                                                      dct.my_timestamp: any_base_dataset.source_dict[
                                                                          dct.my_timestamp]},
                                                          [tp.my_string, tp.my_date], date_parser=[
                        variables.general.date_parser_to_save])
            except AttributeError:
                pass

            any_base_dataset.sheet = term_list[ii]

            ii = ii + 1

        return any_base_dataset_list

    @staticmethod
    def create_empty_dataframe():
        any_base_dataset = BaseDataset(name='empty', any_filepath="C:", df_setup_for_column=pd.DataFrame())
        return any_base_dataset


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
