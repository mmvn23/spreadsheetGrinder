import pandas as pd
import utils.general as ut
import variables.type as tp
import variables.var_column as clmn
import variables.DIAGEO_setup.dashboard as dash
import variables.DIAGEO_setup.my_dict as stp_dct
from Dataset import Raw, Matrix
import variables.dict as dct
import copy


def load_volume(actual_initial_date, any_stp_dict, previous_archive_volume_to_be_loaded=False,
                archive_initial_date=pd.Timestamp(year=1900, month=1, day=1),
                actual_end_date=pd.Timestamp(year=2099, month=12, day=31)):
    [mtx_nomenclature, mtx_uom_conversion, mtx_part_number] = Matrix.DataMatrix.load_old_object_list(
                                                                                ['nomenclature', 'uom_conversion',
                                                                                 'part_number'], any_stp_dict)
    mtx_volume = Matrix.DataMatrix.load_from_json('volume', root=any_stp_dict[dct.root_folder],
                                                  folder=any_stp_dict[dct.json_folder])
    mtx_volume_actual = copy.deepcopy(mtx_volume)
    mtx_volume_actual.load_dataframe(any_raw_dataset_name_list=['volume_actual'],
                                     any_mtx_nomenclature=mtx_nomenclature, any_mtx_uom_conversion=mtx_uom_conversion,
                                     any_mtx_part_number=mtx_part_number, root_json=any_stp_dict[dct.root_folder],
                                     folder_json=any_stp_dict[dct.json_folder], key_clmn=clmn.part_number_code)
    mtx_volume_actual.trim_date(initial_date=actual_initial_date,
                                end_date=actual_end_date,
                                date_clmn=clmn.date, reset_index=True)
    mtx_volume.concat_datamatrix(mtx_volume_actual)

    if previous_archive_volume_to_be_loaded:
        print('volume 31')
    else:
        mtx_volume_archive = copy.deepcopy(mtx_volume)
        mtx_volume_archive.load_dataframe_from_family(base_dataset_family_name='volume_legacy',
                                                      any_stp_dict=any_stp_dict, any_mtx_nomenclature=mtx_nomenclature,
                                                      any_mtx_uom_conversion=mtx_uom_conversion,
                                                      any_mtx_part_number=mtx_part_number,
                                                      treat_date=False, load_all_files_within_folder=True,
                                                      load_all_sheets_on_spreadsheet=False,
                                                      key_clmn=clmn.part_number_code)
        mtx_volume_archive.trim_date(initial_date=actual_initial_date,
                                    end_date=actual_end_date,
                                    date_clmn=clmn.date, reset_index=True)
        mtx_volume.concat_datamatrix(mtx_volume_archive)


    # if archive_volume_to_be_loaded:
    #     mtx_volume_archive = Matrix.DataMatrix.load_old_object('volume',  any_stp_dict= any_stp_dict, is_for_archive=True)
    #     mtx_volume_archive.filter_based_on_column(any_column=clmn.is_forecast, value_list=[False],
    #                                           keep_value_in=True)
    #     mtx_volume_archive.trim_date(initial_date=archive_initial_date,
    #                              end_date=refresh_initial_date - pd.DateOffset(days=1),
    #                              date_clmn=clmn.date, reset_index=True)
    #     mtx_volume.concat_base_dataset(mtx_volume_archive)

    if mtx_volume.get_row_number() > 0:
        mtx_volume.assure_column_integrity()
        mtx_volume.write(any_stp_dict, save_dataframe=True, save_error=True)

    return


if __name__ == "__main__":
    pd.set_option('display.max_columns', 30)
    pd.set_option('display.max_rows', 70000)
    load_volume(any_stp_dict=stp_dct.setup_dict,
                previous_archive_volume_to_be_loaded=dash.previous_archive_volume_to_be_loaded,
                archive_initial_date=dash.archive_initial_date,
                actual_initial_date=dash.actual_initial_date,
                actual_end_date=dash.actual_end_date)

# Trim dates for lagged dataframe, new historical, forecast
# Date sampling
# Add forecast





