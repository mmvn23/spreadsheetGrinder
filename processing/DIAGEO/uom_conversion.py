import copy
import variables.dict as dct
import pandas as pd
import utils.general as ut
import variables.type as tp
from Dataset import Raw, Matrix
import variables.var_column as clmn


def invert_uom(any_mtx_uom):
    any_mtx_uom_inverted = copy.deepcopy(any_mtx_uom)
    index_list = any_mtx_uom.get_var_index_list()
    new_index_list = [index_list[1], index_list[0], index_list[2]]
    temp = 'temp'
    any_mtx_uom_inverted.assign_constant_to_column(value=1, any_clmn=temp)
    any_mtx_uom_inverted.divide_column_by_another(dividend_clmn=temp, divisor_clmn=clmn.multiplier, result_clmn=clmn.multiplier)
    any_mtx_uom_inverted.rename_column_list(old_clmn_list=index_list, new_clmn_list=new_index_list, reset_index=True)
    any_mtx_uom.concat_dataframe(any_mtx_uom_inverted.dataframe)
    any_mtx_uom.assure_column_integrity()
    return any_mtx_uom


def load_uom_conversion():
    mtx_uom_conversion = Matrix.DataMatrix.load_from_json('uom_conversion')
    mtx_nomenclature = Matrix.DataMatrix.load_old_object('nomenclature')
    mtx_uom_conversion.load_dataframe(any_raw_dataset_name_list=['uom_length', 'uom_area', 'uom_volume',
                                                                 'uom_fuel_efficiency', 'uom_mass', 'uom_density',
                                                                 'uom_mass_concentration', 'uom_energy',
                                                                 'uom_material_specific_grain',
                                                                 'uom_material_specific_container'],
                                      any_mtx_nomenclature=mtx_nomenclature)
    mtx_uom_conversion = invert_uom(mtx_uom_conversion)
    mtx_uom_conversion.remove_duplicated_index()
    mtx_uom_conversion.write()

    return


if __name__ == "__main__":
    pd.set_option('display.max_columns', 10)
    pd.set_option('display.max_rows', 500)
    load_uom_conversion()