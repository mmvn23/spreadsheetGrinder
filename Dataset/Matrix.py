import pandas as pd
import variables.setup.column
import variables.date_treatment
from Dataset.Base import BaseDataset, get_dataframe_filepath
from Dataset.Raw import RawDataset
import variables.var_column as clmn
import utils.general as ut
import utils.setup
import variables.setup.file as stp
import variables.format as fmt
import variables.dict as dct
import variables.general as var_gen
import variables.error_message as err_msg
import variables.type as tp
import json
import copy


class DataMatrix(BaseDataset):
    def __init__(self, name, output_filepath, df_setup_for_column, mytimestamp):
        super().__init__(name, output_filepath, df_setup_for_column, mytimestamp)
        self.df_error = pd.DataFrame()  # dataframe with error

    def __str__(self):  # to be developed
        # out_str = "\n\nRAW ->\n" \
        #           "Format: {format} \n" \
        #           "Source dict: {source_dict}\n" \
        #           "Transpose?: {to_be_transposed}\n" \
        #           "use cols: {usecols}\n" \
        #           "skip row dict: {skip_row_dict}\n" \
        #           "enconding: {encoding}\n" \
        #           "decimal: {decimal}\n" \
        #     .format(format=self.format,
        #             source_dict=self.source_dict,
        #             to_be_transposed=self.to_be_transposed,
        #             usecols=self.usecols,
        #             skip_row_dict=self.skip_row_dict,
        #             encoding=self.encoding,
        #             decimal=self.decimal)
        out_str = ''
        return super(DataMatrix, self).__str__() + out_str

    @staticmethod
    def load_list_of_datasets(df_setup, df_column):
        df_setup.reset_index(inplace=True)
        df_setup.dropna(subset=[variables.setup.column.dataset_name], inplace=True)
        dataset_name_list = set(list(df_setup[variables.setup.column.dataset_name]))
        any_datamatrix_list = []

        for any_dataset_name in dataset_name_list:
            any_datamatrix_to_append = DataMatrix.load_attributes_from_spreadsheet(dataset_name=any_dataset_name,
                                                                                   any_df_setup=df_setup,
                                                                                   setup_clmn_list_for_clmn_info=
                                                                         [variables.setup.column.dataset_name,
                                                                          variables.setup.column.row,
                                                                          variables.setup.column.clmn1,
                                                                          variables.setup.column.clmn2,
                                                                          variables.setup.column.clmn3,
                                                                          variables.setup.column.clmn4,
                                                                          variables.setup.column.clmn5,
                                                                          variables.setup.column.clmn6,
                                                                          variables.setup.column.clmn7,
                                                                          variables.setup.column.clmn8,
                                                                          variables.setup.column.clmn9,
                                                                          variables.setup.column.clmn10,
                                                                          variables.setup.column.clmn11,
                                                                          variables.setup.column.clmn12,
                                                                          variables.setup.column.clmn13,
                                                                          variables.setup.column.clmn14,
                                                                          variables.setup.column.clmn15,
                                                                          variables.setup.column.clmn16,
                                                                          variables.setup.column.clmn17,
                                                                          variables.setup.column.clmn18,
                                                                          variables.setup.column.clmn19,
                                                                          variables.setup.column.clmn20],
                                                                                   any_df_column=df_column)
            any_datamatrix_list = any_datamatrix_list + [any_datamatrix_to_append]

        return any_datamatrix_list
    
    @staticmethod
    def load_attributes_from_spreadsheet(dataset_name, any_df_setup, setup_clmn_list_for_clmn_info, any_df_column):
        root_folder = ut.get_value_from_dataframe(input_dataframe=any_df_setup,
                                                  target_column_list=variables.setup.column.root,
                                                  column_list_to_filter=[variables.setup.column.dataset_name,
                                                                         variables.setup.column.row],
                                                  value_list_to_filter=[dataset_name, stp.row_main])
        folder = ut.get_value_from_dataframe(input_dataframe=any_df_setup,
                                             target_column_list=variables.setup.column.folder,
                                             column_list_to_filter=[variables.setup.column.dataset_name,
                                                                    variables.setup.column.row],
                                             value_list_to_filter=[dataset_name, stp.row_main])
        any_filepath = ut.get_filepath(root=root_folder, folder=folder, file=dataset_name, any_format=fmt.csv)
        df_column_setup = utils.setup.prepare_df_column_setup(dataset_name, any_df_setup,
                                                              setup_clmn_list_for_clmn_info,
                                                              any_df_column)
        any_datamatrix = DataMatrix(name=dataset_name, output_filepath=any_filepath,
                                    df_setup_for_column=df_column_setup,
                                    mytimestamp=ut.get_now_timestamp())
        return any_datamatrix

    def write(self):
        self.write_as_json()
        self.write_dataframe_as_csv(save_df_setup_clmn=True)
        self.write_dataframe_as_csv(save_df_setup_clmn=False)
        self.write_dataframe_as_csv(save_df_setup_clmn=False, save_df_error=True)

        return

    def write_dataframe_as_csv(self, save_df_setup_clmn=False, save_df_error=False):

        if save_df_setup_clmn:
            any_dataframe = copy.deepcopy(self.df_setup_for_column)
            filepath = get_dataframe_filepath(self.name, is_for_setup_clmn=True)
        elif save_df_error:
            any_dataframe = copy.deepcopy(self.df_error)
            filepath = get_dataframe_filepath(self.name, is_for_setup_clmn=False, is_for_error=True)

        else:
            any_dataframe = copy.deepcopy(self.dataframe)
            filepath = get_dataframe_filepath(self.name, is_for_setup_clmn=False)

        any_dataframe.to_csv(filepath, encoding=stp.encoding_to_save, date_format=stp.date_parser_to_save)
        return

    def write_as_json(self):
        address = ut.get_filepath(root=stp.root_folder, folder=stp.json_folder, file=self.name, any_format=stp.json)
        with open(address, "w") as outfile:
            json.dump(self.convert_to_dict(), outfile, indent=stp.json_indent)
        return

    def convert_to_dict(self):
        any_dict = {dct.name: self.name,
                    dct.filepath: self.filepath,
                    dct.my_timestamp: ut.convert_timestamp_to_str(self.mytimestamp, stp.date_parser_to_save),
                    dct.df_setup_clmn_filepath: get_dataframe_filepath(self.name, is_for_setup_clmn=True),
                    # df_setup_for_column_address combined with save csv
                    }

        return any_dict

    def load_dataframe_from_raw_dataset_list(self, any_raw_dataset_name_list):
        self.dataframe = pd.DataFrame()

        for any_raw_dataset_name in any_raw_dataset_name_list:
            any_raw_dataset = RawDataset.load_from_json(any_raw_dataset_name)
            any_raw_dataset.load_dataframe()
            self.dataframe = pd.concat([self.dataframe, any_raw_dataset.dataframe])

        return

    def load_dataframe(self, any_raw_dataset_name_list, any_mtx_nomenclature=var_gen.void,
                       any_mtx_uom_conversion=var_gen.void, any_mtx_part_number=var_gen.void):
        self.load_dataframe_from_raw_dataset_list(any_raw_dataset_name_list)
        self.consolidate_date()
        # clean string for material codes
        self.apply_nomenclature(any_mtx_nomenclature)

        self.apply_uom_conversion_to_si(any_mtx_uom_conversion, any_mtx_part_number)
        self.apply_standard_index()
        self.add_timestamp()
        self.assure_column_integrity()
        self.remove_duplicated_index()
        return

    def remove_duplicated_index(self):
        temp_duplicated = 'duplicated index'
        self.dataframe[temp_duplicated] = self.dataframe.index.duplicated(keep='first')

        df_error = self.dataframe[self.dataframe[temp_duplicated]]

        self.dataframe = self.dataframe[~self.dataframe[temp_duplicated]]
        self.drop_column_list(column_list=[temp_duplicated], drop_column_list=True)

        df_error[variables.setup.column.error_message] = err_msg.duplicated_index
        df_error.drop(columns=temp_duplicated, inplace=True)
        df_error.reset_index(inplace=True)
        self.df_error = pd.concat([self.df_error, df_error])

        return

    def apply_uom_conversion_to_si(self, any_mtx_uom_conversion, any_mtx_part_number):
        column_list = self.get_any_column_list(target_column_list=[variables.setup.column.clmn_var_name],
                                               add_column_list_to_filter=[variables.setup.column.
                                               uom_conversion_to_be_applied],
                                               add_value_list_to_filter=[True])

        if any_mtx_part_number != var_gen.void:
            self.merge_dataframe(original_right_dataset=any_mtx_part_number, desired_column_list=[clmn.si_uom],
                                 left_on_list=[clmn.part_number_code], right_on_list=[clmn.part_number_code],
                                 reset_left_index=False, reset_right_index=True, drop_right_on_list=False)
        if any_mtx_uom_conversion != var_gen.void:
            for any_column in column_list:
                self.apply_uom_conversion_to_si_to_column(any_mtx_uom_conversion, any_column)
        return

    def apply_uom_conversion_to_si_to_column(self, any_mtx_uom_conversion, any_column):
        self.convert_uom(any_clmn=any_column, numerator_dict={dct.any_mtx_conversion: any_mtx_uom_conversion,
                                                              dct.old_uom: clmn.uom,
                                                              dct.new_uom: clmn.si_uom},
                         denominator_dict={})
        self.rename_column_list(old_clmn_list=[clmn.uom, clmn.si_uom], new_clmn_list=['input uom', clmn.uom])
        return

    def apply_types(self, standard_column_list=[], type_list=[]):
        self.remove_index()

        if len(standard_column_list) == 0:
            standard_column_list = self.get_any_column_list(target_column_list=[variables.setup.column.clmn_var_name],
                                                            add_column_list_to_filter=[],
                                                            add_value_list_to_filter=[])
            type_list = self.get_any_column_list(target_column_list=[variables.setup.column.type],
                                                 add_column_list_to_filter=[],
                                                 add_value_list_to_filter=[])
        ii = 0
        for any_standard_clmn in standard_column_list:
            self.apply_type_to_column(any_standard_clmn, type_list[ii])
            ii = ii + 1
        self.apply_standard_index()
        self.assure_column_integrity()
        # self.handle_nan()
        return

    def apply_type_to_column(self, any_standard_clmn, desired_type):
        if desired_type == tp.my_int:
            self.dataframe[any_standard_clmn] = self.dataframe[any_standard_clmn].astype('int64')
            # any_value = int(any_value)
        elif desired_type == tp.my_float:
            self.dataframe[any_standard_clmn] = self.dataframe[any_standard_clmn].astype('float64')
            # any_value = float(any_value)
        elif desired_type == tp.my_string:
            # self.dataframe[any_standard_clmn] = self.dataframe[any_standard_clmn].fillna(-1)
            if self.dataframe[any_standard_clmn].dtype == float:
                self.dataframe[any_standard_clmn] = self.dataframe[any_standard_clmn].astype('int64')
            self.dataframe[any_standard_clmn] = self.dataframe[any_standard_clmn].astype(str)
            # any_value = str(any_value)
        elif desired_type == tp.my_bool:
            # if self.dataframe[any_standard_clmn].dtype != str:
            self.dataframe[any_standard_clmn] = self.dataframe.apply(lambda row: str(row[any_standard_clmn]).lower().
                                                                     capitalize() == "True", axis=1)
            # else:
            #     self.dataframe[any_standard_clmn] = self.dataframe[any_standard_clmn].astype('bool')
            # any_value = bool(any_value)
        elif desired_type == tp.my_date:
            self.dataframe[any_standard_clmn] = self.dataframe.apply(lambda row:
                                                                     ut.parse_date_as_timestamp(row[any_standard_clmn],
                                                                                                [variables.date_treatment
                                                                                                .parser]),
                                                                     axis=1)
        else:
            print(any_standard_clmn, desired_type)
            raise ('ERROR: type not implemented')

        self.filter_nan_and_update_error(any_standard_clmn, error_message=err_msg.type_conversion)

        return

    def apply_nomenclature(self, any_mtx_nomenclature):
        column_list = self.get_any_column_list(target_column_list=[variables.setup.column.clmn_var_name],
                                               add_column_list_to_filter=[variables.setup.column.
                                               nomenclature_to_be_applied],
                                               add_value_list_to_filter=[True])
        if any_mtx_nomenclature != var_gen.void:
            for any_column in column_list:
                self.apply_nomenclature_to_column(any_mtx_nomenclature, any_column)
        return

    def apply_nomenclature_to_column(self, any_mtx_nomenclature, any_column):
        self.prepare_string_clmn_for_merge(any_column=any_column, reset_index=False, assure_column_integrity=False)
        self.merge_dataframe(original_right_dataset=any_mtx_nomenclature,
                             desired_column_list=[clmn.term_after_nomenclature], left_on_list=[any_column],
                             right_on_list=[clmn.term_before_nomenclature],
                             reset_left_index=False, reset_right_index=True)
        self.drop_column_list(column_list=[any_column], drop_column_list=True, reset_index=False)
        self.rename_column_list(old_clmn_list=[clmn.term_after_nomenclature], new_clmn_list=[any_column])
        df_error_to_concat, self.dataframe = self.filter_nan_and_update_error(any_column=any_column,
                                                                              error_message=
                                                                              err_msg.nomenclature + any_column)
        self.df_error = pd.concat([self.df_error, df_error_to_concat])
        return

    def add_timestamp(self):
        self.dataframe[clmn.timestamp] = ut.get_now_timestamp()
        return

    def consolidate_date(self):
        var_column_list = list(self.df_setup_for_column[variables.setup.column.clmn_var_name])

        if clmn.date in var_column_list:
            df_date, self.dataframe = self.filter_nan_and_update_error(any_column=clmn.date)

            if len(df_date) > 0:
                df_date[clmn.date] = df_date.apply(lambda row: ut.get_date_from_month_and_period(row[clmn.month_day_as_date],
                                                                                                 row[clmn.period],
                                                                                                 row[clmn.fiscal_year]), axis=1)

                self.dataframe = pd.concat([self.dataframe, df_date])
                df_error_to_concat, self.dataframe = self.filter_nan_and_update_error(any_column=clmn.date,
                                                                                  error_message=
                                                                                  err_msg.date_consolidation)
                self.df_error = pd.concat([self.df_error, df_error_to_concat])

        return

    def consolidate_volume(self):
        var_column_list = list(self.df_setup_for_column[variables.setup.column.clmn_var_name])

        if clmn.volume in var_column_list:
            self.consolidate_volume_columns()
            df_error_to_concat, self.dataframe = self.filter_nan_and_update_error(any_column=clmn.volume,
                                                                                  error_message=
                                                                                  err_msg.volume_consolidation)
            self.df_error = pd.concat([self.df_error, df_error_to_concat])

        return

    def consolidate_volume_columns(self):
        # implement fillna as BaseDataset
        clmn_list = self.check_if_column_list_part_of_dataframe(original_column_list=clmn.volume_list)
        self.dataframe.fillna(dict(zip(clmn_list + [clmn.volume],
                                       [0] * len(clmn_list + [clmn.volume]))), inplace=True)
        self.sum_columns(result_clmn=clmn.volume, summand_clmn_list=clmn_list)
        return

# load in json
    @staticmethod
    def load_from_json(name):
        address = ut.get_filepath(root=stp.root_folder, folder=stp.json_folder, file=name, any_format=stp.json)

        with open(address, "r") as outfile:
            content = outfile.read()

        any_dict = json.loads(content)
        any_datamatrix = DataMatrix.load_header_from_dict(any_dict)

        return any_datamatrix

    @staticmethod
    def load_header_from_dict(any_dict):
        df_column_setup_filepath = any_dict[dct.df_setup_clmn_filepath]
        df_column_setup = utils.setup.load_csv(df_column_setup_filepath)
        df_column_setup.fillna(value={variables.setup.column.index: False}, inplace=True)
        any_datamatrix = DataMatrix(name=any_dict[dct.name], output_filepath=any_dict[dct.filepath],
                                    mytimestamp=any_dict[dct.my_timestamp],
                                    df_setup_for_column=df_column_setup)
        return any_datamatrix

    @staticmethod
    def load_old_object(name, is_for_archive=False):
        any_datamatrix = DataMatrix.load_from_json(name)
        filepath = get_dataframe_filepath(any_datamatrix.name, is_for_setup_clmn=False, is_for_archive=is_for_archive)
        any_datamatrix.dataframe = utils.setup.load_csv(filepath)

        any_datamatrix.apply_standard_index()
        any_datamatrix.assure_column_integrity()
        any_datamatrix.apply_types()
        return any_datamatrix

    @staticmethod
    def load_old_object_list(name_list):
        any_datamatrix_list = []
        for any_name in name_list:
            any_datamatrix = copy.deepcopy(DataMatrix.load_old_object(any_name))
            any_datamatrix_list.append(any_datamatrix)
        return any_datamatrix_list