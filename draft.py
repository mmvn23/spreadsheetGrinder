# from pre_loading import pre_load_objects
# from processing.nomenclature import load_nomenclature
# from processing.uom_conversion import load_uom_conversion
# from processing.volume import load_volume
# from processing.part_number import load_part_number
import utils.general as ut
import variables.var_column as clmn
import variables.general as var_gen
import pandas as pd
from Dataset import Raw, Matrix

# pre_load_objects()
# load_nomenclature()
# load_uom_conversion()
# load_part_number()

# load_volume()
original = 'foot'
new = 'meter'
mtx_uom_conversion = Matrix.DataMatrix.load_old_object('uom_conversion')
print(mtx_uom_conversion.dataframe)
value = ut.get_value_from_dataframe(input_dataframe=mtx_uom_conversion.dataframe,
                                         target_column_list=clmn.multiplier,
                                         column_list_to_filter=[clmn.original, clmn.new, clmn.strategy],
                                         value_list_to_filter=[original, new, var_gen.uom_general],
                                         reset_index=True)

print(value)
# value = ut.get_multiplier_from_mtx_conversion(mtx_uom_conversion, original='conventional bushel', new='kilogram', part_number='6200000000')
# print('>>>>>>>>>>>>')
# print(value)


# mtx_nomenclature = Matrix.DataMatrix.load_old_object('nomenclature')
# print(mtx_nomenclature.dataframe)
# mtx_uom_conversion = Matrix.DataMatrix.load_old_object('uom_conversion')
# print('DRAFT 24')
# print(mtx_uom_conversion.dataframe)
# mtx_part_number = Matrix.DataMatrix.load_old_object('part_number')
# print('DRAFT 24')
# print(mtx_part_number.dataframe.index.dtype)



