import pandas as pd
import utils.general as ut
import variables.type as tp
import variables.var_column as clmn
import variables.setup.dashboard as dash
from Dataset import Raw, Matrix
import copy


def load_volume(refresh_initial_date, archive_volume_to_be_loaded=False,
                archive_initial_date=pd.Timestamp(year=1900, month=1, day=1),
                refresh_end_date=pd.Timestamp(year=2099, month=12, day=31)):
    [mtx_nomenclature, mtx_uom_conversion, mtx_part_number] = Matrix.DataMatrix.load_old_object_list(
                                                                                ['nomenclature', 'uom_conversion',
                                                                                 'part_number'])
    mtx_volume = Matrix.DataMatrix.load_from_json('volume')
    mtx_volume_new = copy.deepcopy(mtx_volume)

    mtx_volume_new.load_dataframe(any_raw_dataset_name_list=['volume_f20', 'volume_f21', 'volume_f22',  'volume_f23',
                                                             'volume_f23_p05', 'volume_f23_p06'],
                                  any_mtx_nomenclature=mtx_nomenclature, any_mtx_uom_conversion=mtx_uom_conversion,
                                  any_mtx_part_number=mtx_part_number)

    mtx_volume_new.trim_date(initial_date=refresh_initial_date,
                             end_date=refresh_end_date,
                             date_clmn=clmn.date, reset_index=True)
    mtx_volume.concat_datamatrix(mtx_volume_new)

    if archive_volume_to_be_loaded:
        mtx_volume_old = Matrix.DataMatrix.load_old_object('volume', is_for_archive=True)
        mtx_volume_old.filter_based_on_column(any_column=clmn.is_forecast, value_list=[False],
                                              keep_value_in=True)
        mtx_volume_old.trim_date(initial_date=archive_initial_date,
                                 end_date=refresh_initial_date - pd.DateOffset(days=1),
                                 date_clmn=clmn.date, reset_index=True)
        mtx_volume.concat_base_dataset(mtx_volume_old)

    if mtx_volume.get_row_number() > 0:
        mtx_volume.write()

    return


if __name__ == "__main__":
    pd.set_option('display.max_columns', 30)
    pd.set_option('display.max_rows', 70000)
    load_volume(archive_volume_to_be_loaded=dash.archive_volume_to_be_loaded,
                archive_initial_date=dash.archive_initial_date,
                refresh_initial_date=dash.refresh_initial_date,
                refresh_end_date=dash.refresh_end_date)

# Trim dates for lagged dataframe, new historical, forecast
# Date sampling
# Add forecast





