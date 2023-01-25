import copy
import variables.dict as dct
import pandas as pd
import utils.general as ut
import variables.type as tp
from Dataset import Raw, Matrix
import variables.var_column as clmn
import variables.DIAGEO_setup.my_dict as stp_dct


def add_new_terms(original_datamatrix):
    any_datamatrix = copy.deepcopy(original_datamatrix)

    any_datamatrix.reset_index()
    new_terms_list = list(set(list(any_datamatrix.dataframe[clmn.term_after_nomenclature])))
    my_dict = {clmn.term_before_nomenclature: new_terms_list,
               clmn.term_after_nomenclature: new_terms_list}

    ## REFACTOR
    df_to_concat = pd.DataFrame.from_dict(my_dict)
    any_datamatrix.concat_dataframe(df_to_concat)

    any_datamatrix.apply_standard_index()
    any_datamatrix.assure_column_integrity()
    return any_datamatrix


def load_nomenclature(any_stp_dict):
    mtx_nomenclature = Matrix.DataMatrix.load_from_json(name='nomenclature',
                                                        root=any_stp_dict[dct.root_folder],
                                                        folder=any_stp_dict[dct.json_folder])
    mtx_nomenclature.load_dataframe(any_raw_dataset_name_list=['nomenclature_location', 'nomenclature_uom',
                                                               'nomenclature_category'],
                                    root_json=any_stp_dict[dct.root_folder],
                                    folder_json=any_stp_dict[dct.json_folder])
    mtx_nomenclature = add_new_terms(mtx_nomenclature)
    mtx_nomenclature.prepare_string_clmn_for_merge(any_column=clmn.term_before_nomenclature, reset_index=True)
    mtx_nomenclature.remove_duplicated_index()
    mtx_nomenclature.write(any_stp_dict, save_dataframe=True, save_error=True)

    return


if __name__ == "__main__":
    pd.set_option('display.max_columns', 10)
    pd.set_option('display.max_rows', 500)
    load_nomenclature(stp_dct.setup_dict)

