import copy
import variables.dict as dct
import pandas as pd
import utils.general as ut
import variables.type as tp
from Dataset import Raw, Matrix
import variables.var_column as clmn
import variables.DIAGEO_setup.my_dict as stp_dct


def load_ppv(any_stp_dict):
    mtx_ppv = Matrix.DataMatrix.load_from_json('ppv', root=any_stp_dict[dct.root_folder],
                                               folder=any_stp_dict[dct.json_folder])
    mtx_nomenclature = Matrix.DataMatrix.load_old_object('nomenclature', any_stp_dict)
    mtx_ppv.load_dataframe_from_family('ppv_family', any_stp_dict, any_mtx_nomenclature=mtx_nomenclature)
    mtx_ppv.write(any_stp_dict, save_dataframe=True, save_error=True)

    return


if __name__ == "__main__":
    load_ppv(stp_dct.setup_dict)