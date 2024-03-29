import pandas as pd
import variables.setup_column as stp_clmn
import variables.date_treatment
import variables.general
from Dataset.Base import BaseDataset, get_dataframe_filepath
from Dataset.Raw import RawDataset
import variables.var_column as clmn
import utils.general as ut
import utils.setup
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
        df_setup.dropna(subset=[variables.setup_column.dataset_name], inplace=True)
        dataset_name_list = set(list(df_setup[variables.setup_column.dataset_name]))
        any_datamatrix_list = []

        for any_dataset_name in dataset_name_list:
            any_datamatrix_to_append = DataMatrix.load_attributes_from_spreadsheet(dataset_name=any_dataset_name,
                                                                                   any_df_setup=df_setup,
                                                                                   setup_clmn_list_for_clmn_info=
                                                                         [variables.setup_column.dataset_name,
                                                                          variables.setup_column.row,
                                                                          stp_clmn.clmn1,
                                                                          stp_clmn.clmn2,
                                                                          stp_clmn.clmn3,
                                                                          stp_clmn.clmn4,
                                                                          stp_clmn.clmn5,
                                                                          stp_clmn.clmn6,
                                                                          stp_clmn.clmn7,
                                                                          stp_clmn.clmn8,
                                                                          stp_clmn.clmn9,
                                                                          stp_clmn.clmn10,
                                                                          stp_clmn.clmn11,
                                                                          stp_clmn.clmn12,
                                                                          stp_clmn.clmn13,
                                                                          stp_clmn.clmn14,
                                                                          stp_clmn.clmn15,
                                                                          stp_clmn.clmn16,
                                                                          stp_clmn.clmn17,
                                                                          stp_clmn.clmn18,
                                                                          stp_clmn.clmn19,
                                                                          stp_clmn.clmn20,
                                                                          stp_clmn.clmn21,
                                                                          stp_clmn.clmn22,
                                                                          stp_clmn.clmn23,
                                                                          stp_clmn.clmn24,
                                                                          stp_clmn.clmn25,
                                                                          stp_clmn.clmn26,
                                                                          stp_clmn.clmn27,
                                                                          stp_clmn.clmn28,
                                                                          stp_clmn.clmn29,
                                                                          stp_clmn.clmn30],
                                                                                   any_df_column=df_column)
            any_datamatrix_list = any_datamatrix_list + [any_datamatrix_to_append]

        return any_datamatrix_list
    
    @staticmethod
    def load_attributes_from_spreadsheet(dataset_name, any_df_setup, setup_clmn_list_for_clmn_info, any_df_column):
        root_folder = ut.get_value_from_dataframe(input_dataframe=any_df_setup,
                                                  target_column_list=variables.setup_column.root,
                                                  column_list_to_filter=[variables.setup_column.dataset_name,
                                                                         variables.setup_column.row],
                                                  value_list_to_filter=[dataset_name, variables.general.row_main])
        folder = ut.get_value_from_dataframe(input_dataframe=any_df_setup,
                                             target_column_list=variables.setup_column.folder,
                                             column_list_to_filter=[variables.setup_column.dataset_name,
                                                                    variables.setup_column.row],
                                             value_list_to_filter=[dataset_name, variables.general.row_main])
        any_filepath = ut.get_filepath(root=root_folder, folder=folder, file=dataset_name, any_format=fmt.csv)
        df_column_setup = utils.setup.prepare_df_column_setup(dataset_name, any_df_setup,
                                                              setup_clmn_list_for_clmn_info,
                                                              any_df_column)
        any_datamatrix = DataMatrix(name=dataset_name, output_filepath=any_filepath,
                                    df_setup_for_column=df_column_setup,
                                    mytimestamp=ut.get_now_timestamp())
        return any_datamatrix

    # def write(self, root_json=dg_stp.root_folder, folder_json=dg_stp.json_folder,
    #           folder_dataframe=dg_stp.dataframe_folder):
    def write(self, any_stp_dict, save_dataframe=True, save_error=True):
        self.write_as_json(any_stp_dict)
        if save_dataframe:
            self.write_dataframe_as_csv(any_stp_dict=any_stp_dict)
        if save_error:
            self.write_dataframe_as_csv(any_stp_dict=any_stp_dict, save_df_setup_clmn=False, save_df_error=True)
        self.write_dataframe_as_csv(any_stp_dict=any_stp_dict, save_df_setup_clmn=True, save_df_error=False)
        print('MTX 111 - writing successfully ', self.name)
        return

    def write_dataframe_as_csv(self, any_stp_dict, save_df_setup_clmn=False, save_df_error=False):

        if save_df_setup_clmn:
            any_dataframe = copy.deepcopy(self.df_setup_for_column)
            filepath = get_dataframe_filepath(self.name, any_stp_dict, is_for_setup_clmn=True)
        elif save_df_error:
            any_dataframe = copy.deepcopy(self.df_error)
            filepath = get_dataframe_filepath(self.name, any_stp_dict, is_for_setup_clmn=False, is_for_error=True)

        else:
            any_dataframe = copy.deepcopy(self.dataframe)
            filepath = get_dataframe_filepath(self.name, any_stp_dict, is_for_setup_clmn=False)

        any_dataframe.to_csv(filepath, encoding=variables.general.encoding_to_save,
                             date_format=variables.general.date_parser_to_save)
        return

    # write_as_json(self, root=dg_stp.root_folder, folder=dg_stp.json_folder):
    def write_as_json(self, any_stp_dict):
        address = ut.get_filepath(root=any_stp_dict[dct.root_folder], folder=any_stp_dict[dct.json_folder],
                                  file=self.name, any_format=variables.general.json)
        with open(address, "w") as outfile:
            json.dump(self.convert_to_dict(any_stp_dict), outfile, indent=variables.general.json_indent)
        return

    # def convert_to_dict(self, root=dg_stp.root_folder, folder=dg_stp.json_folder):
    def convert_to_dict(self, any_stp_dict):
        any_dict = {dct.name: self.name,
                    dct.filepath: self.filepath,
                    dct.my_timestamp: ut.convert_timestamp_to_str(self.mytimestamp,
                                                                  variables.general.date_parser_to_save),
                    dct.df_setup_clmn_filepath: get_dataframe_filepath(self.name, any_stp_dict=any_stp_dict,
                                                                       is_for_setup_clmn=True),
                    # df_setup_for_column_address combined with save csv
                    }

        return any_dict

    def load_dataframe_from_raw_dataset_list(self, any_raw_dataset_name_list, root_json, folder_json, treat_date=True):
        self.dataframe = pd.DataFrame()

        for any_raw_dataset_name in any_raw_dataset_name_list:
            any_raw_dataset = RawDataset.load_from_json(any_raw_dataset_name, root=root_json, folder=folder_json)
            any_raw_dataset.load_dataframe(treat_date=treat_date)
            self.dataframe = pd.concat([self.dataframe, any_raw_dataset.dataframe])

        return

    def load_dataframe(self, any_raw_dataset_name_list, root_json, folder_json, key_clmn='',
                       any_mtx_nomenclature=var_gen.void,
                       any_mtx_uom_conversion=var_gen.void, any_mtx_part_number=var_gen.void,
                       treat_date=True, any_raw_dataset_list=[]):
        self.load_dataframe_from_raw_dataset_list(any_raw_dataset_name_list, treat_date=treat_date,
                                                  root_json=root_json, folder_json=folder_json)
        if len(any_raw_dataset_list) > 0:
            self.concat_base_dataset_list(any_basedataset_list=any_raw_dataset_list)

        self.consolidate_date()
        # clean string for material codes
        self.apply_nomenclature(any_mtx_nomenclature)
        self.apply_uom_conversion_to_si(any_mtx_uom_conversion, any_mtx_part_number, key_clmn)
        self.apply_standard_index()
        self.add_timestamp()
        self.assure_column_integrity()
        self.remove_duplicated_index()
        return

    def load_dataframe_from_family(self, base_dataset_family_name, any_stp_dict, any_mtx_nomenclature=var_gen.void,
                                   any_mtx_uom_conversion=var_gen.void, any_mtx_part_number=var_gen.void,
                                   treat_date=True, load_all_files_within_folder=True,
                                   load_all_sheets_on_spreadsheet=False, key_clmn='', run_auto_etl=True):
        any_raw_dataset = RawDataset.load_from_json(base_dataset_family_name, root=any_stp_dict[dct.root_folder],
                                                    folder=any_stp_dict[dct.json_folder])

        if load_all_files_within_folder:
            any_raw_dataset.filepath = ut.remove_file_name_from_filepath(filepath=any_raw_dataset.filepath,
                                                                         sub_str=var_gen.folder_separator)
            file_name_list = ut.get_file_list_from_directory(any_raw_dataset.filepath)
            n_elements = len(file_name_list)
            any_raw_dataset_list = any_raw_dataset.create_list_of_copied_base_datasets(n_elements=n_elements)
            any_raw_dataset_list = BaseDataset.adjust_name_on_base_dataset_list(any_raw_dataset_list, file_name_list)
            any_raw_dataset_list = BaseDataset.adjust_filepath_on_base_dataset_list(any_raw_dataset_list, file_name_list)

        elif load_all_sheets_on_spreadsheet:
            sheet_list = ut.get_sheet_list_from_spreadsheet(any_raw_dataset.filepath)
            n_elements = len(sheet_list)

            any_raw_dataset_list = any_raw_dataset.create_list_of_copied_base_datasets(n_elements=n_elements)
            any_raw_dataset_list = BaseDataset.adjust_sheet_on_base_dataset_list(any_raw_dataset_list, sheet_list)

        for any_raw_dataset in any_raw_dataset_list:
            any_raw_dataset.load_dataframe(treat_date)
            self.dataframe = pd.concat([self.dataframe, any_raw_dataset.dataframe])

        # self.consolidate_date()
        if run_auto_etl:
            self.apply_nomenclature(any_mtx_nomenclature)
            self.apply_uom_conversion_to_si(any_mtx_uom_conversion, any_mtx_part_number, key_clmn)
            self.apply_standard_index()
            self.add_timestamp()
            self.assure_column_integrity()
            self.remove_duplicated_index()

        return

    def assign_datamatrix_to_empty_header(self, input_datamatrix):
        any_datamatrix = copy.deepcopy(input_datamatrix)
        any_datamatrix.add_name_to_second_level_column()
        self.dataframe = any_datamatrix.dataframe

        return

    def merge_datamatrix(self, right_datamatrix, desired_column_list, left_on_list,
                         right_on_list, multilevel_datamatrix=False, reset_left_index=False, reset_right_index=False,
                         drop_right_on_list=True):
        any_datamatrix = copy.deepcopy(right_datamatrix)
        # any_datamatrix.add_name_to_second_level_column()
        self.merge_dataframe(original_right_dataset=any_datamatrix, desired_column_list=desired_column_list,
                             left_on_list=left_on_list, right_on_list=right_on_list,
                             reset_left_index=reset_left_index, reset_right_index=reset_right_index,
                             drop_right_on_list=drop_right_on_list, multilevel_datamatrix=multilevel_datamatrix)
        return

    def add_name_to_second_level_column(self):
        self.dataframe.columns = pd.MultiIndex.from_product([[ut.get_datamatrix_name(self.name)],
                                                             self.dataframe.columns])
        return

    def remove_duplicated_index(self):
        temp_duplicated = 'duplicated index'
        self.dataframe[temp_duplicated] = self.dataframe.index.duplicated(keep='first')

        df_error = self.dataframe[self.dataframe[temp_duplicated]]

        self.dataframe = self.dataframe[~self.dataframe[temp_duplicated]]
        self.drop_column_list(column_list=[temp_duplicated], drop_column_list=True)

        df_error[stp_clmn.error_message] = err_msg.duplicated_index
        df_error.drop(columns=temp_duplicated, inplace=True)
        df_error.reset_index(inplace=True)
        self.df_error = pd.concat([self.df_error, df_error])

        return

    def concat_datamatrix(self, any_datamatrix):
        self.concat_dataframe(any_datamatrix.dataframe)
        self.concat_error_dataframe(any_datamatrix.df_error)
        return

    def concat_error_dataframe(self, any_df_error):
        self.df_error = pd.concat([self.df_error, any_df_error])
        return

    def apply_uom_conversion_to_numerator_and_denominator(self, numerator_code_clmn, numerator_value_clmn,
                                                          numerator_uom_clmn, mtx_numerator_uom, new_uom_clmn,
                                                          denominator_code_clmn, denominator_value_clmn,
                                                          denominator_uom_clmn, mtx_denominator_uom,
                                                          any_mtx_uom_conversion):
        mtx_numerator = copy.deepcopy(self)
        mtx_denominator = copy.deepcopy(self)
        mtx_numerator.drop_column_list(column_list=[denominator_value_clmn, denominator_uom_clmn],
                                       drop_column_list=True, reset_index=True)
        mtx_numerator.remove_index()
        mtx_numerator_uom.remove_index()
        mtx_numerator.merge_dataframe(original_right_dataset=mtx_numerator_uom,
                                      desired_column_list=[new_uom_clmn], left_on_list=[numerator_code_clmn],
                                      right_on_list=[numerator_code_clmn],
                                      reset_left_index=False, reset_right_index=False, drop_right_on_list=False)

        mtx_numerator.convert_uom(any_clmn=numerator_value_clmn, numerator_dict={dct.any_mtx_conversion: any_mtx_uom_conversion,
                                                                                 dct.old_uom: numerator_uom_clmn,
                                                                                 dct.new_uom: new_uom_clmn}, denominator_dict={})

        mtx_denominator.drop_column_list(column_list=[numerator_code_clmn, denominator_code_clmn,
                                                      denominator_value_clmn, denominator_uom_clmn],
                                         drop_column_list=False, keep_column_list=True, reset_index=True)
        mtx_denominator.remove_index()
        mtx_denominator_uom.remove_index()
        # numerator_dict = {dct.any_mtx_conversion: any_mtx_uom_conversion,
        #                   dct.old_uom: 'a',
        #                   dct.new_uom: 'b'}


        # drop_column_list(column_list=[], drop_column_list=True, keep_column_list=False, reset_index=False)

        # split into two mtx
        # other clmn stay on numerator
        # apply uom conversion on both
        # include sweeteners and gns
        # remove nan
        return

    def apply_uom_conversion_to_si(self, any_mtx_uom_conversion, any_mtx_part_number, key_clmn):
        column_list = self.get_any_column_list(target_column_list=[stp_clmn.clmn_var_name],
                                               add_column_list_to_filter=[stp_clmn.
                                               uom_conversion_to_be_applied],
                                               add_value_list_to_filter=[True])

        if any_mtx_part_number != var_gen.void:
            self.merge_dataframe(original_right_dataset=any_mtx_part_number, desired_column_list=[clmn.si_uom],
                                 left_on_list=[clmn.part_number_code], right_on_list=[clmn.part_number_code],
                                 reset_left_index=False, reset_right_index=True, drop_right_on_list=False)

        if any_mtx_uom_conversion != var_gen.void:
            for any_column in column_list:
                self.apply_uom_conversion_to_si_to_column(any_mtx_uom_conversion, any_column, key_clmn)

        return

    def apply_uom_conversion_to_si_to_column(self, any_mtx_uom_conversion, any_column, key_clmn):
        self.convert_uom(any_clmn=any_column, numerator_dict={dct.any_mtx_conversion: any_mtx_uom_conversion,
                                                              dct.old_uom: clmn.uom,
                                                              dct.new_uom: clmn.si_uom,
                                                              dct.key_clmn: key_clmn},
                         denominator_dict={})

        self.rename_column_list(old_clmn_list=[clmn.uom, clmn.si_uom], new_clmn_list=['input uom', clmn.uom])

        return

    def apply_types(self, standard_column_list=[], type_list=[]):
        self.remove_index()

        if len(standard_column_list) == 0:
            standard_column_list = self.get_any_column_list(target_column_list=[stp_clmn.clmn_var_name],
                                                            add_column_list_to_filter=[],
                                                            add_value_list_to_filter=[])
            type_list = self.get_any_column_list(target_column_list=[stp_clmn.type],
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

        self.filter_nan_dataframe(any_standard_clmn, error_message=err_msg.type_conversion)

        return

    def apply_nomenclature(self, any_mtx_nomenclature):
        column_list = self.get_any_column_list(target_column_list=[stp_clmn.clmn_var_name],
                                               add_column_list_to_filter=[stp_clmn.
                                               nomenclature_to_be_applied],
                                               add_value_list_to_filter=[True])
        if any_mtx_nomenclature != var_gen.void:
            for any_column in column_list:
                self.apply_nomenclature_to_column(any_mtx_nomenclature, any_column)
        return

    def apply_nomenclature_to_column(self, any_mtx_nomenclature, any_column):
        reset_left_index=self.is_index_standard()
        self.prepare_string_clmn_for_merge(any_column=any_column, reset_index=False, assure_column_integrity=False)
        self.merge_dataframe(original_right_dataset=any_mtx_nomenclature,
                             desired_column_list=[clmn.term_after_nomenclature], left_on_list=[any_column],
                             right_on_list=[clmn.term_before_nomenclature],
                             reset_left_index=reset_left_index, reset_right_index=True)

        self.drop_column_list(column_list=[any_column], drop_column_list=True, reset_index=False)
        self.rename_column_list(old_clmn_list=[clmn.term_after_nomenclature], new_clmn_list=[any_column])

        # df_error_to_concat, self.dataframe = self.filter_nan_dataframe(any_column=any_column,
        #                                                                error_message=
        #                                                                       err_msg.nomenclature + any_column)
        #
        # if len(df_error_to_concat) > 0:
        #     try:
        #         self.df_error = pd.concat([self.df_error, df_error_to_concat])
        #     except pd.errors.InvalidIndexError:
        #         print('ERROR in concat df_error')

        return

    def add_timestamp(self):
        self.dataframe[clmn.timestamp] = ut.get_now_timestamp()
        return

    def consolidate_date(self):
        var_column_list = list(self.df_setup_for_column[stp_clmn.clmn_var_name])

        if clmn.date in var_column_list:
            df_date, self.dataframe = self.filter_nan_dataframe(any_column=clmn.date)

            if len(df_date) > 0:
                df_date[clmn.date] = df_date.apply(lambda row: ut.get_date_from_month_and_period(row[clmn.month_day_as_date],
                                                                                                 row[clmn.period],
                                                                                                 row[clmn.fiscal_year]), axis=1)

                self.dataframe = pd.concat([self.dataframe, df_date])
                df_error_to_concat, self.dataframe = self.filter_nan_dataframe(any_column=clmn.date,
                                                                               error_message=
                                                                                  err_msg.date_consolidation)
                self.df_error = pd.concat([self.df_error, df_error_to_concat])

        return

    def consolidate_volume(self):
        var_column_list = list(self.df_setup_for_column[stp_clmn.clmn_var_name])

        if clmn.volume in var_column_list:
            self.consolidate_volume_columns()
            df_error_to_concat, self.dataframe = self.filter_nan_dataframe(any_column=clmn.volume,
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

    # def multiply_multilevel_column_by_another(self, multiplier_tuple_clmn, multiplicand_tuple_clmn, product_clmn):
    #     self.dataframe[product_clmn] = self.dataframe[multiplier_clmn] * self.dataframe[multiplicand_clmn]
    #     return

# load in json
    @staticmethod
    def load_from_json(name, root, folder):
        address = ut.get_filepath(root=root, folder=folder, file=name, any_format=variables.general.json)

        with open(address, "r") as outfile:
            content = outfile.read()

        any_dict = json.loads(content)
        any_datamatrix = DataMatrix.load_header_from_dict(any_dict)

        return any_datamatrix

    @staticmethod
    def load_header_from_dict(any_dict):
        df_column_setup_filepath = any_dict[dct.df_setup_clmn_filepath]
        df_column_setup = utils.setup.load_csv(df_column_setup_filepath)
        df_column_setup.fillna(value={stp_clmn.index: False}, inplace=True)
        any_datamatrix = DataMatrix(name=any_dict[dct.name], output_filepath=any_dict[dct.filepath],
                                    mytimestamp=any_dict[dct.my_timestamp],
                                    df_setup_for_column=df_column_setup)
        return any_datamatrix

    @staticmethod
    def load_old_object(name, any_stp_dict, is_for_archive=False):
        any_datamatrix = DataMatrix.load_from_json(name, root=any_stp_dict[dct.root_folder],
                                                   folder=any_stp_dict[dct.json_folder])
        filepath = get_dataframe_filepath(any_datamatrix.name, any_stp_dict, is_for_setup_clmn=False,
                                          is_for_archive=is_for_archive)
        any_datamatrix.dataframe = utils.setup.load_csv(filepath)

        any_datamatrix.apply_standard_index()
        any_datamatrix.assure_column_integrity()
        any_datamatrix.apply_types()
        return any_datamatrix

    @staticmethod
    def load_old_object_list(name_list, any_stp_dict):
        any_datamatrix_list = []
        for any_name in name_list:
            any_datamatrix = copy.deepcopy(DataMatrix.load_old_object(any_name, any_stp_dict))
            any_datamatrix_list.append(any_datamatrix)
        return any_datamatrix_list


def write_base_dataset_list(any_base_dataset_list, any_stp_dict):

    for any_base_dataset in any_base_dataset_list:
        any_base_dataset.write(any_stp_dict, save_dataframe=True, save_error=True)

    return any_base_dataset_list