import pandas as pd
import utils.general as ut
import variables.type as tp
import variables.var_column as clmn
from Dataset import Raw, Matrix
import copy


def load_volume(historical_new_initial_date, old_volume_to_be_loaded=False,
                historical_old_initial_date=pd.Timestamp(year=1900, month=1, day=1),
                historical_new_end_date=pd.Timestamp(year=2099, month=12, day=31)):
    [mtx_nomenclature, mtx_uom_conversion, mtx_part_number] = Matrix.DataMatrix.load_old_object_list(
                                                                                ['nomenclature', 'uom_conversion',
                                                                                 'part_number'])

    mtx_volume = Matrix.DataMatrix.load_from_json('volume')

    mtx_volume_new = copy.deepcopy(mtx_volume)
    mtx_volume_new.load_dataframe(any_raw_dataset_name_list=['volume_historical'],
                                  any_mtx_nomenclature=mtx_nomenclature, any_mtx_uom_conversion=mtx_uom_conversion,
                                  any_mtx_part_number=mtx_part_number)
    mtx_volume_new.trim_date(initial_date=historical_new_initial_date,
                             end_date=historical_new_end_date,
                             date_clmn=clmn.date, reset_index=True)
    mtx_volume.concat_base_dataset(mtx_volume_new)

    if old_volume_to_be_loaded:
        mtx_volume_old = Matrix.DataMatrix.load_old_object('volume', is_for_archive=True)
        mtx_volume_old.trim_date(initial_date=historical_old_initial_date,
                                 end_date=historical_new_initial_date - pd.DateOffset(days=1),
                                 date_clmn=clmn.date, reset_index=True)
        mtx_volume.concat_base_dataset(mtx_volume_old)

    if mtx_volume.get_row_number() > 0:
        mtx_volume.write()

    return


if __name__ == "__main__":
    pd.set_option('display.max_columns', 10)
    pd.set_option('display.max_rows', 3000)
    load_volume(old_volume_to_be_loaded=True, historical_old_initial_date=pd.Timestamp(year=1900, month=1, day=1),
                historical_new_initial_date=pd.Timestamp(year=2022, month=4, day=1),
                historical_new_end_date=pd.Timestamp(year=2099, month=12, day=31))

# Trim dates for lagged dataframe, new historical, forecast
# Date sampling
# Add forecast





