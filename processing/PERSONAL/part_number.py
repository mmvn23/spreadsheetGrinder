import copy
import variables.dict as dct
import pandas as pd
import utils.general as ut
import variables.type as tp
from Dataset import Raw, Matrix
import variables.var_column as clmn


def load_part_number():
    mtx_part_number = Matrix.DataMatrix.load_from_json('part_number')
    mtx_nomenclature = Matrix.DataMatrix.load_old_object('nomenclature')
    mtx_part_number.load_dataframe(any_raw_dataset_name_list=['part_number_grain'],
                                   any_mtx_nomenclature=mtx_nomenclature)

    mtx_part_number.write()

    return


load_part_number()