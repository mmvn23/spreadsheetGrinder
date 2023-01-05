import pandas as pd
import utils.general as ut
import variables.type as tp
from Dataset import Raw, Matrix


def load_volume():
    mtx_volume = Matrix.DataMatrix.load_from_json('volume')
    mtx_nomenclature = Matrix.DataMatrix.load_old_object('nomenclature')
    mtx_uom_conversion = Matrix.DataMatrix.load_old_object('uom_conversion')
    mtx_part_number = Matrix.DataMatrix.load_old_object('part_number')

    mtx_volume.load_dataframe(any_raw_dataset_name_list=['volume_historical', 'electricity_20', 'electricity_21',
                                                         'electricity_22', 'enablon', 'used_barrels_kbb_repairs',
                                                         'used_barrels_kbb_selection', 'used_barrels_sps_repairs',
                                                         'used_barrels_sps_selection', 'tullahoma_corn', 'tullahoma_rye',
                                                         'shelbyville_corn', 'shelbyville_rye', 'shelbyville_barrels',
                                                         'lebanon_corn', 'lebanon_barrels', 'gimli_grain',
                                                         'valleyfield_grain', 'valleyfield_barrels'],
                              any_mtx_nomenclature=mtx_nomenclature, any_mtx_uom_conversion=mtx_uom_conversion,
                              any_mtx_part_number=mtx_part_number)
    mtx_volume.write()
    return


pd.set_option('display.max_columns', 10)
pd.set_option('display.max_rows', 3000)
load_volume()

# Substitute Each for Conv. Bushels
# 3301008098
# 6200000000
# 6200000001
# 6200000002
# 3301010616

# Split forecast for vendors
# Trim dates for lagged dataframe, new historical, forecast
# Date sampling





