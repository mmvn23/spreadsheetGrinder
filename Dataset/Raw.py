import copy
import pandas as pd
import variables.DIAGEO_setup.file
import variables.setup_column as stp_clmn
import variables.general
from Dataset.Base import BaseDataset, get_dataframe_filepath
import utils.general as ut
import utils.setup
# import variables.DIAGEO_setup.file as dg_stp
import variables.var_column as clmn
import variables.type as tp
import variables.dict as dct
import variables.format as fmt
import variables.input_strategy as in_stg
import variables.nan_handling_strategy as nan_hd
import variables.date_treatment as date_treat
import json
import datetime

pd.set_option('display.max_columns', 100)


class RawDataset(BaseDataset):
    def __init__(self, name, input_filepath, sheet, source_name, source_timestamp, to_be_transposed, use_cols, nrows,
                 date_parser_list,
                 skip_row_bf_header, skip_row_af_header, spreadsheet_encoding, spreadsheet_decimal, format,
                 df_setup_for_column, mytimestamp=datetime.datetime.now()):
        super().__init__(name, input_filepath, df_setup_for_column, mytimestamp)
        self.sheet = sheet
        self.format = ut.assign_type(format, tp.my_string) # string OK
        self.source_dict = ut.assign_type_to_dict({dct.name: source_name, dct.my_timestamp: source_timestamp},
                                                  [tp.my_string, tp.my_date], date_parser=[
                variables.general.date_parser_to_save])
        # {name (string):  , timestamp (timestamp): } OK
        self.to_be_transposed = ut.assign_type(to_be_transposed, tp.my_bool) # bool OK
        self.usecols = ut.assign_type(use_cols, tp.my_string) # string OK
        self.nrows = ut.assign_type(nrows, tp.my_int) # int OK
        self.date_parser_list = ut.assign_type_for_list(date_parser_list, tp.my_string)
        # ut.assign_type(date_parser_list, tp.my_string) # int OK
        self.skip_row_dict = ut.assign_type_to_dict({dct.bf_header: skip_row_bf_header,
                                                     dct.af_header: skip_row_af_header},
                                                  [tp.my_int, tp.my_int]) # {bf_header (int):  , af_header (int): } OK
        self.encoding = ut.assign_type(spreadsheet_encoding, tp.my_string) # string ok
        # self.index_col = index_col # string
        self.decimal = ut.assign_type(spreadsheet_decimal, tp.my_string) # string

    def __str__(self): # to be developed
        out_str = "\n\nRAW ->\n" \
                  "Format: {format} \n" \
                  "Source dict: {source_dict}\n" \
                  "Transpose?: {to_be_transposed}\n"\
                  "use cols: {usecols}\n" \
                  "skip row dict: {skip_row_dict}\n" \
                  "enconding: {encoding}\n" \
                  "decimal: {decimal}\n" \
            .format(format=self.format,
                    source_dict=self.source_dict,
                    to_be_transposed=self.to_be_transposed,
                    usecols=self.usecols,
                    skip_row_dict=self.skip_row_dict,
                    encoding=self.encoding,
                    decimal=self.decimal)
        return super(RawDataset, self).__str__() + out_str

# pre-loading
    @staticmethod
    def load_attributes_from_spreadsheet(dataset_name, any_df_setup, setup_clmn_list_for_clmn_info, any_df_column):
        # name, input_directory, column_list,
        # source_name, source_timestamp, to_be_transposed, use_cols,
        # skip_row_bf_header, skip_row_af_header, spreadsheet_encoding, spreadsheet_decimal
        source_name = ut.get_value_from_dataframe(input_dataframe=any_df_setup, target_column_list=stp_clmn.source_name,
                                                  column_list_to_filter=[stp_clmn.dataset_name,
                                                                         stp_clmn.row],
                                                  value_list_to_filter=[dataset_name, variables.general.row_main])
        sheet = ut.get_value_from_dataframe(input_dataframe=any_df_setup, target_column_list=stp_clmn.sheet,
                                            column_list_to_filter=[stp_clmn.dataset_name,
                                                                   stp_clmn.row],
                                            value_list_to_filter=[dataset_name, variables.general.row_main])
        source_last_update = ut.get_value_from_dataframe(input_dataframe=any_df_setup,
                                                         target_column_list=stp_clmn.source_last_update,
                                                         column_list_to_filter=[stp_clmn.dataset_name,
                                                                                stp_clmn.row],
                                                         value_list_to_filter=[dataset_name,
                                                                               variables.general.row_main])
        to_be_transposed = ut.get_value_from_dataframe(input_dataframe=any_df_setup,
                                                       target_column_list=stp_clmn.to_transpose,
                                                       column_list_to_filter=[stp_clmn.dataset_name,
                                                                              stp_clmn.row],
                                                       value_list_to_filter=[dataset_name, variables.general.row_main])
        use_cols = ut.get_value_from_dataframe(input_dataframe=any_df_setup,
                                               target_column_list=stp_clmn.usecols,
                                               column_list_to_filter=[stp_clmn.dataset_name,
                                                                      stp_clmn.row],
                                               value_list_to_filter=[dataset_name, variables.general.row_main])
        nrows = ut.get_value_from_dataframe(input_dataframe=any_df_setup,
                                            target_column_list=stp_clmn.nrows,
                                            column_list_to_filter=[stp_clmn.dataset_name,
                                                                   stp_clmn.row],
                                            value_list_to_filter=[dataset_name, variables.general.row_main])
        date_parser_str = ut.get_value_from_dataframe(input_dataframe=any_df_setup,
                                                      target_column_list=stp_clmn.date_parser,
                                                      column_list_to_filter=[stp_clmn.dataset_name,
                                                                             stp_clmn.row],
                                                      value_list_to_filter=[dataset_name, variables.general.row_main])
        date_parser_list = date_parser_str.split(variables.general.split_char)

        skip_row_bf_header = ut.get_value_from_dataframe(input_dataframe=any_df_setup,
                                                         target_column_list=stp_clmn.skiprow_bf_header,
                                                         column_list_to_filter=[stp_clmn.dataset_name,
                                                                                stp_clmn.row],
                                                         value_list_to_filter=[dataset_name,
                                                                               variables.general.row_main])
        skip_row_af_header = ut.get_value_from_dataframe(input_dataframe=any_df_setup,
                                                         target_column_list=stp_clmn.skiprow_af_header,
                                                         column_list_to_filter=[stp_clmn.dataset_name,
                                                                                stp_clmn.row],
                                                         value_list_to_filter=[dataset_name,
                                                                               variables.general.row_main])
        enconding = ut.get_value_from_dataframe(input_dataframe=any_df_setup,
                                                target_column_list=stp_clmn.encoding,
                                                column_list_to_filter=[stp_clmn.dataset_name,
                                                                       stp_clmn.row],
                                                value_list_to_filter=[dataset_name, variables.general.row_main])
        decimal = ut.get_value_from_dataframe(input_dataframe=any_df_setup,
                                              target_column_list=stp_clmn.decimal,
                                              column_list_to_filter=[stp_clmn.dataset_name,
                                                                     stp_clmn.row],
                                              value_list_to_filter=[dataset_name, variables.general.row_main])
        root_folder = ut.get_value_from_dataframe(input_dataframe=any_df_setup,
                                                  target_column_list=stp_clmn.root,
                                                  column_list_to_filter=[stp_clmn.dataset_name,
                                                                         stp_clmn.row],
                                                  value_list_to_filter=[dataset_name, variables.general.row_main])
        folder = ut.get_value_from_dataframe(input_dataframe=any_df_setup,
                                             target_column_list=stp_clmn.folder,
                                             column_list_to_filter=[stp_clmn.dataset_name,
                                                                    stp_clmn.row],
                                             value_list_to_filter=[dataset_name, variables.general.row_main])
        file = ut.get_value_from_dataframe(input_dataframe=any_df_setup,
                                           target_column_list=stp_clmn.file,
                                           column_list_to_filter=[stp_clmn.dataset_name,
                                                                  stp_clmn.row],
                                           value_list_to_filter=[dataset_name, variables.general.row_main])
        format = ut.get_value_from_dataframe(input_dataframe=any_df_setup,
                                             target_column_list=stp_clmn.format,
                                             column_list_to_filter=[stp_clmn.dataset_name,
                                                                    stp_clmn.row],
                                             value_list_to_filter=[dataset_name, variables.general.row_main])
        any_filepath = ut.get_filepath(root=root_folder, folder=folder, file=file, any_format=format)

        df_column_setup = utils.setup.prepare_df_column_setup(dataset_name, any_df_setup, setup_clmn_list_for_clmn_info,
                                                              any_df_column)
        # get dataframe for column information
        any_raw_dataset = RawDataset(name=dataset_name, input_filepath=any_filepath, sheet=sheet,
                                     source_name=source_name, source_timestamp=source_last_update,
                                     to_be_transposed=to_be_transposed, use_cols=use_cols, nrows=nrows,
                                     date_parser_list=date_parser_list,
                                     skip_row_bf_header=skip_row_bf_header, skip_row_af_header=skip_row_af_header,
                                     spreadsheet_encoding=enconding, spreadsheet_decimal=decimal, format=format,
                                     df_setup_for_column=df_column_setup)
        return any_raw_dataset

# write in json
#     def convert_to_dict(self, root=dg_stp.root_folder, folder=dg_stp.json_folder)
    def convert_to_dict(self, any_stp_dict):
        any_dict = {dct.name: self.name,
                    dct.filepath: self.filepath,
                    dct.sheet: self.sheet,
                    dct.my_timestamp: ut.convert_timestamp_to_str(self.mytimestamp,
                                                                  variables.general.date_parser_to_save),
                    dct.df_setup_clmn_filepath: get_dataframe_filepath(self.name, any_stp_dict,
                                                                       is_for_setup_clmn=True),
                    # df_setup_for_column_address combined with save csv
                    dct.format: self.format,
                    dct.source_dict: {dct.name: self.source_dict[dct.name],
                                      dct.my_timestamp: ut.convert_timestamp_to_str(self.source_dict[dct.my_timestamp],
                                                                                    variables.general.date_parser_to_save)},
                    dct.to_be_transposed: str(self.to_be_transposed),
                    dct.use_cols: self.usecols,
                    dct.nrows: self.nrows,
                    dct.date_parser: self.date_parser_list,
                    dct.skip_row: self.skip_row_dict,
                    dct.encoding: self.encoding,
                    dct.decimal: self.decimal
                    }

        return any_dict

    # def write_as_json(self, root=dg_stp.root_folder, folder=dg_stp.json_folder):
    def write_as_json(self, any_stp_dict):
        address = ut.get_filepath(root=any_stp_dict[dct.root_folder], folder=any_stp_dict[dct.json_folder],
                                  file=self.name, any_format=variables.general.json)
        with open(address, "w") as outfile:
            json.dump(self.convert_to_dict(any_stp_dict), outfile, indent=variables.general.json_indent)
        return

    def write_dataframe_as_csv(self, any_stp_dict, save_df_setup_clmn=False):
        if save_df_setup_clmn:
            any_dataframe = self.df_setup_for_column
            filepath = get_dataframe_filepath(self.name, any_stp_dict, is_for_setup_clmn=True)
        else:
            any_dataframe = self.dataframe
            filepath = get_dataframe_filepath(self.name, any_stp_dict, is_for_setup_clmn=False)

        any_dataframe.to_csv(filepath, encoding=variables.general.encoding_to_save, date_format=variables.general.date_parser_to_save)
        return

    # def write(self, root_json=dg_stp.root_folder, folder_json=dg_stp.json_folder):
    def write(self, any_stp_dict):
        self.write_as_json(any_stp_dict)
        self.write_dataframe_as_csv(any_stp_dict, save_df_setup_clmn=True)

        return

# load in json
    @staticmethod
    def load_from_json(name, root, folder):
        address = ut.get_filepath(root=root, folder=folder, file=name, any_format=variables.general.json)

        with open(address, "r") as outfile:
            content = outfile.read()

        any_dict = json.loads(content)
        any_raw_dataset = RawDataset.load_header_from_dict(any_dict)

        return any_raw_dataset

    @staticmethod
    def load_header_from_dict(any_dict):
        df_column_setup_filepath = any_dict[dct.df_setup_clmn_filepath]
        df_column_setup = utils.setup.load_csv(df_column_setup_filepath)

        any_raw_dataset = RawDataset(name=any_dict[dct.name], input_filepath=any_dict[dct.filepath],
                                     sheet=any_dict[dct.sheet],
                                     mytimestamp=any_dict[dct.my_timestamp],
                                     source_name=any_dict[dct.source_dict][dct.name],
                                     source_timestamp=any_dict[dct.source_dict][dct.my_timestamp],
                                     to_be_transposed=any_dict[dct.to_be_transposed], use_cols=any_dict[dct.use_cols],
                                     nrows=any_dict[dct.nrows], date_parser_list=any_dict[dct.date_parser],
                                     skip_row_bf_header=any_dict[dct.skip_row][dct.bf_header],
                                     skip_row_af_header=any_dict[dct.skip_row][dct.af_header],
                                     spreadsheet_encoding=any_dict[dct.encoding],
                                     spreadsheet_decimal=any_dict[dct.decimal],
                                     format=any_dict[dct.format],
                                     df_setup_for_column=df_column_setup)
        return any_raw_dataset

    @staticmethod
    def load_list_of_datasets(df_setup, df_column):
        df_setup.reset_index(inplace=True)
        df_setup.dropna(subset=[stp_clmn.dataset_name], inplace=True)
        dataset_name_list = set(list(df_setup[stp_clmn.dataset_name]))
        any_raw_dataset_list = []

        for any_dataset_name in dataset_name_list:
            any_raw_dataset_to_append = RawDataset.load_attributes_from_spreadsheet(dataset_name=any_dataset_name,
                                                                                    any_df_setup=df_setup,
                                                                                    setup_clmn_list_for_clmn_info=
                                                                         [stp_clmn.dataset_name,
                                                                          stp_clmn.row,
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
                                                                          stp_clmn.clmn20],
                                                                                    any_df_column=df_column)

            any_raw_dataset_list = any_raw_dataset_list + [any_raw_dataset_to_append]

        return any_raw_dataset_list

# load dataframe
    def load_dataframe(self, treat_date=True):
        self.load_dataframe_from_spreadsheet()
        self.fill_missing_information()

        if treat_date:
            self.prepare_string_to_mask_date_parsing()
        self.handle_nan()
        self.apply_types()
        self.apply_multiplication()
        self.clean_string()
        self.drop_column_list(keep_column_list=True)
        self.fill_source_info()
        return

    def prepare_string_to_mask_date_parsing(self):
        self.delete_terms_from_string()
        self.translate_terms()
        self.trim_terms()
        return

    def trim_terms(self):
        standard_column_list = self.get_any_column_list(target_column_list=[stp_clmn.clmn_var_name],
                                                        add_column_list_to_filter=[stp_clmn.delete_terms],
                                                        add_value_list_to_filter=[True])
        ii = 0
        for any_standard_clmn in standard_column_list:
            self.trim_terms_per_column(any_standard_clmn)
            ii = ii + 1
        return

    def trim_terms_per_column(self, any_standard_clmn, ):
        self.dataframe[any_standard_clmn] = self.dataframe.apply(lambda row: ut.trim_string(row[any_standard_clmn]),
                                                                 axis=1)
        return

    def translate_terms(self):
        standard_column_list = self.get_any_column_list(target_column_list=[stp_clmn.clmn_var_name],
                                                        add_column_list_to_filter=[stp_clmn.delete_terms],
                                                        add_value_list_to_filter=[True])
        df_translation = utils.setup.load_xlsx(filepath=variables.DIAGEO_setup.file.date_treatment,
                                               sheet_name=date_treat.translation_sheet,
                                               header=0, new_column_list=[clmn.original],
                                               key_clmn_list=[],
                                               index_col=date_treat.index_col,
                                               usecols=date_treat.usecols, skiprows=0)
        terms_to_translate = list(df_translation[clmn.original])
        ii = 0
        for any_standard_clmn in standard_column_list:
            self.translate_terms_per_column(any_standard_clmn, terms_to_translate, df_translation)
            ii = ii + 1
        return

    def translate_terms_per_column(self, any_standard_clmn, terms_to_translate, df_translation):
        self.dataframe[any_standard_clmn] = self.dataframe.apply(lambda row:
                                                                 ut.substitute_term_from_string(row[any_standard_clmn],
                                                                                                terms_to_translate,
                                                                                                df_translation,
                                                                                                clmn.new,
                                                                                                clmn.original),
                                                                 axis=1)
        return

    def delete_terms_from_string(self, filepath=variables.DIAGEO_setup.file.date_treatment):
        standard_column_list = self.get_any_column_list(target_column_list=[stp_clmn.clmn_var_name],
                                                        add_column_list_to_filter=[stp_clmn.delete_terms],
                                                        add_value_list_to_filter=[True])
        df_terms_to_delete = utils.setup.load_xlsx(filepath=filepath,
                                                   sheet_name=date_treat.deletion_sheet,
                                                   header=0, new_column_list=[date_treat.terms_to_delete],
                                                   key_clmn_list=[],
                                                   index_col=date_treat.index_col,
                                                   usecols=date_treat.usecols, skiprows=0)
        terms_to_delete = list(df_terms_to_delete[date_treat.terms_to_delete])
        ii = 0
        for any_standard_clmn in standard_column_list:
            self.delete_terms_from_string_per_column(any_standard_clmn, terms_to_delete)
            ii = ii + 1
        return

    def delete_terms_from_string_per_column(self, any_standard_clmn, terms_to_delete):

        self.dataframe[any_standard_clmn] = self.dataframe.apply(lambda row:
                                                                 ut.substitute_term_from_string(row[any_standard_clmn],
                                                                                                terms_to_delete),
                                                                 axis=1)

        return

    def handle_nan(self):
        standard_column_list = self.get_any_column_list(target_column_list=[stp_clmn.clmn_var_name],
                                                        add_column_list_to_filter=[],
                                                        add_value_list_to_filter=[])
        fill_strategy_list = self.get_any_column_list(target_column_list=[variables.general.row_nan_handling_strategy],
                                                      add_column_list_to_filter=[],
                                                      add_value_list_to_filter=[])
        type_list = self.get_any_column_list(target_column_list=[stp_clmn.type],
                                             add_column_list_to_filter=[],
                                             add_value_list_to_filter=[])

        ii = 0
        for any_standard_clmn in standard_column_list:
            self.handle_nan_by_column(any_standard_clmn, fill_strategy_list[ii], type_list[ii])
            ii = ii + 1

        return

    def handle_nan_by_column(self, any_standard_column, fill_strategy, type):
        if fill_strategy == nan_hd.drop_row:
            self.dataframe.dropna(subset=any_standard_column, inplace=True)
        elif fill_strategy == nan_hd.filler_by_type:
            if type == tp.my_float or type == tp.my_int:
                filler = variables.general.nan_filler_for_numeric
            else:
                filler = variables.general.nan_filler_for_non_numeric
            self.dataframe.fillna(value=filler, inplace=True)
        elif fill_strategy == nan_hd.ffill:
            self.dataframe.fillna(method='ffill', inplace=True)
        elif fill_strategy == nan_hd.bfill:
            self.dataframe.fillna(method='bfill', inplace=True)
        else:
            raise(fill_strategy + 'not implemented')
        return

    def clean_string(self):
        standard_column_list = self.get_any_column_list(target_column_list=[stp_clmn.clmn_var_name],
                                                        add_column_list_to_filter=[stp_clmn.type],
                                                        add_value_list_to_filter=[tp.my_string])
        clean_list = self.get_any_column_list(target_column_list=[stp_clmn.string_cleaning_to_be_applied],
                                              add_column_list_to_filter=[stp_clmn.type],
                                              add_value_list_to_filter=[tp.my_string])
        ii = 0
        for any_standard_clmn in standard_column_list:
            self.clean_string_to_column(any_standard_clmn, clean_list[ii])
            ii = ii + 1

        return

    def clean_string_to_column(self, any_standard_clmn, clean):

        if clean:
            self.dataframe[any_standard_clmn] = self.dataframe.apply(lambda row:
                                                                     ut.clean_string(row[any_standard_clmn]),
                                                                                        axis=1)
        return

    def apply_multiplication(self):
        standard_column_list = self.get_any_column_list(target_column_list=[stp_clmn.clmn_var_name],
                                                        add_column_list_to_filter=[stp_clmn.type],
                                                        add_value_list_to_filter=[tp.my_float])
        multiplier_list = self.get_any_column_list(target_column_list=[stp_clmn.multiplier],
                                                   add_column_list_to_filter=[stp_clmn.type],
                                                   add_value_list_to_filter=[tp.my_float])
        ii = 0
        for any_standard_clmn in standard_column_list:
            self.dataframe[any_standard_clmn] = self.dataframe[any_standard_clmn] * multiplier_list[ii]
            ii = ii + 1
        return

    def apply_types(self, standard_column_list=[], type_list=[]):
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
        self.handle_nan()
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
                                                                                                self.date_parser_list),
                                                                     axis=1)
        else:
            print(any_standard_clmn, desired_type)
            raise ('ERROR: type not implemented')

        return

    def fill_missing_information(self):
        self.fill_with_constant(in_stg.fill_with_constant)
        self.fill_with_range(in_stg.fill_with_range_1_to_12)
        return

    def fill_source_info(self):
        self.dataframe[clmn.source_name] = self.source_dict[dct.name]
        self.dataframe[clmn.source_last_update] = self.source_dict[dct.my_timestamp]

        dict = {stp_clmn.clmn_var_name: [clmn.source_name,
                                                       clmn.source_last_update],
                stp_clmn.clmn_rep_name: [clmn.source_name,
                                                       clmn.source_last_update],
                stp_clmn.type: [tp.my_string, tp.my_date],
                stp_clmn.uom_conversion_to_be_applied: [False, False],
                stp_clmn.nomenclature_to_be_applied: [False, False],
                stp_clmn.string_cleaning_to_be_applied: [False, False],
                }

        self.df_setup_for_column = pd.concat([self.df_setup_for_column, pd.DataFrame(dict)], ignore_index=True)
        self.mytimestamp = ut.get_now_timestamp()

        return

    def fill_with_constant(self, input_strategy):
        value_list = self.get_any_column_list(target_column_list=[variables.general.row_input],
                                              add_column_list_to_filter=[variables.general.row_input_strategy],
                                              add_value_list_to_filter=[input_strategy])
        standard_column_list = self.get_any_column_list(target_column_list=[stp_clmn.clmn_var_name],
                                                        add_column_list_to_filter=[
                                                            variables.general.row_input_strategy],
                                                        add_value_list_to_filter=[input_strategy])

        if len(standard_column_list) > 0:
            ii = 0
            for any_standard_clmn in standard_column_list:
                self.dataframe[any_standard_clmn] = value_list[ii]
                ii = ii + 1

        return

    def fill_with_range(self, input_strategy):
        standard_column_list = self.get_any_column_list(target_column_list=[stp_clmn.clmn_var_name],
                                                        add_column_list_to_filter=[
                                                            variables.general.row_input_strategy],
                                                        add_value_list_to_filter=[input_strategy])
        index_temp = 'index'

        if len(standard_column_list) > 0:
            ii = 0
            for any_standard_clmn in standard_column_list:
                value_list = range(1, len(self.dataframe.index) + 1)
                index_list = list(self.dataframe.index)
                data_dict = {any_standard_clmn: value_list,
                             index_temp: index_list}
                df_to_merge = pd.DataFrame.from_dict(data_dict)
                df_to_merge.set_index(index_temp, inplace=True)
                ii = ii + 1
                self.dataframe = self.dataframe.merge(df_to_merge, left_index=True, right_index=True, how='left')

        return

    def load_dataframe_from_spreadsheet(self):
        self.load_dataframe_from_spreadsheet_names(input_strategy=in_stg.spreadsheet_clmn_names, transpose=False)
        self.load_dataframe_from_spreadsheet_position(input_strategy=in_stg.spreadsheet_clmn_position, transpose=False)
        self.load_dataframe_from_spreadsheet_names(input_strategy=in_stg.spreadsheet_row_names, transpose=True)
        self.load_dataframe_from_spreadsheet_position(input_strategy=in_stg.spreadsheet_row_position, transpose=True)
        return

    def get_input_and_standard_column_lists(self, input_strategy):
        input_column_list = self.get_any_column_list(target_column_list=[variables.general.row_input],
                                                     add_column_list_to_filter=[variables.general.row_input_strategy],
                                                     add_value_list_to_filter=[input_strategy])
        standard_column_list = self.get_any_column_list(target_column_list=[stp_clmn.clmn_var_name],
                                                        add_column_list_to_filter=[
                                                            variables.general.row_input_strategy],
                                                        add_value_list_to_filter=[input_strategy])
        position_list = self.get_any_column_list(target_column_list=[variables.general.row_input],
                                                 add_column_list_to_filter=[variables.general.row_input_strategy],
                                                 add_value_list_to_filter=[input_strategy])
        rename_dict = dict(zip(input_column_list, standard_column_list))

        return input_column_list, standard_column_list, position_list, rename_dict

    def load_dataframe_from_spreadsheet_names(self, input_strategy, transpose):
        if self.format == fmt.csv:
            print('csv not yet developed')
        elif self.format == fmt.xlsx:
            self.load_from_spreadsheet_names_excel(transpose, input_strategy)
        else:
            print(self.format + ' not yet developed')

        return

    def load_dataframe_from_spreadsheet_position(self, input_strategy, transpose):
        if self.format == fmt.csv:
            print('csv not yet developed')
        elif self.format == fmt.xlsx:
            self.load_from_spreadsheet_position_excel(input_strategy, transpose)
        else:
            print(self.format + ' not yet developed')
        return

    def load_from_spreadsheet_names_excel(self, transpose, input_strategy):
        input_column_list, standard_column_list, position_clmn_list, rename_dict = \
            self.get_input_and_standard_column_lists(input_strategy)

        if len(input_column_list) > 0:
            if transpose:
                header = None
            else:
                header = 0

            self.dataframe = pd.read_excel(io=self.filepath, sheet_name=self.sheet, usecols=self.usecols,
                                           engine='openpyxl', skiprows=self.skip_row_dict[dct.bf_header],
                                           nrows=self.nrows, date_parser=self.date_parser_list, decimal=self.decimal,
                                           header=header)
            if transpose:
                # self.dataframe.set_index(1, inplace=True)
                self.dataframe = self.dataframe.T
                self.dataframe.columns = self.dataframe.iloc[0]
                self.dataframe = self.dataframe[1:]

            self.dataframe = self.dataframe[input_column_list]
            self.dataframe = self.dataframe.rename(columns=rename_dict)

        return

    def load_from_spreadsheet_position_excel(self, input_strategy, transpose):
        input_column_list, standard_column_list, position_column_list, rename_dict = \
            self.get_input_and_standard_column_lists(input_strategy)
        if len(standard_column_list) > 0:
            ii = 0
            for any_position in position_column_list:
                input_column = standard_column_list[ii]
                df_to_append = self.load_from_spreadsheet_position_excel_by_position(any_position, input_column,
                                                                                     transpose)
                if len(self.dataframe.index) == 0:
                    self.dataframe = copy.deepcopy(df_to_append)
                else:
                    self.dataframe = self.dataframe.merge(df_to_append, how='left', left_index=True,
                                                          right_index=True)
                ii = ii + 1

        return

    def load_from_spreadsheet_position_excel_by_position(self, any_position, input_column, transpose):
        if transpose:
            usecols = self.usecols
            skiprows = int(any_position) - 1
            names = None
            header = None
            nrows = 1
        else:
            usecols = any_position
            skiprows = self.skip_row_dict[dct.bf_header]
            names = [input_column]
            header = 0
            nrows = self.nrows

        df_to_append = pd.read_excel(io=self.filepath, sheet_name=self.sheet, usecols=usecols,
                                     engine='openpyxl', skiprows=skiprows,
                                     names=names, header=header,
                                     nrows=nrows, date_parser=self.date_parser_list, decimal=self.decimal)

        if transpose:
            df_to_append.set_index(1, inplace=True)
            df_to_append = df_to_append.T
            df_to_append.rename({list(df_to_append.columns)[0]: input_column}, axis=1, inplace=True)
        return df_to_append

    # @staticmethod
    # def adjust_name_and_filepath_on_raw_dataset_family(any_raw_dataset_list, file_name_list):
    #     ii = 0
    #     original_filepath = copy.deepcopy(any_raw_dataset_list[ii].filepath)
    #     for any_raw_dataset in any_raw_dataset_list:
    #         any_raw_dataset.source_dict = ut.assign_type_to_dict({dct.name: file_name_list[ii],
    #                                                               dct.my_timestamp: any_raw_dataset.source_dict[
    #                                                                   dct.my_timestamp]},
    #                                                   [tp.my_string, tp.my_date], date_parser=[
    #                 variables.general.date_parser_to_save])
    #         any_raw_dataset.filepath = ut.treat_filepath(original_filepath +
    #                                                      variables.general.folder_separator + file_name_list[ii])
    #         ii = ii + 1
    #     return any_raw_dataset_list

    # def get_column_list(self, target_column_list, add_column_list_to_filter=[], add_value_list_to_filter=[]):
    #
    #     column_list_to_filter = []
    #     value_list_to_filter = []
    #     column_list = ut.get_value_from_dataframe(input_dataframe=self.df_setup_for_column,
    #                                               target_column_list=target_column_list,
    #                                               column_list_to_filter=column_list_to_filter +
    #                                                                     add_column_list_to_filter,
    #                                               value_list_to_filter=value_list_to_filter + add_value_list_to_filter,
    #                                               return_list=True)
    #     return column_list

# # neo pandas
#     def drop_columns(self, column_list=[], drop_column_list=True, keep_column_list=False):
#         if len(column_list) == 0 & keep_column_list:
#             column_list = self.get_column_list(target_column_list=[clmn.clmn_var_name],
#                                                add_column_list_to_filter=[],
#                                                add_value_list_to_filter=[])
#         if drop_column_list & keep_column_list:
#             drop_column_list = False
#
#         if drop_column_list:
#             self.dataframe.drop(column_list, axis=1, inplace=True)
#
#         if keep_column_list:
#             self.dataframe = self.dataframe[column_list]
#         return
