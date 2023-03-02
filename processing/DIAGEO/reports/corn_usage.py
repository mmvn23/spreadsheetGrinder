from Dataset import Base,Raw, Matrix
import pandas as pd
import variables.dict as dct
import variables.DIAGEO_setup.my_dict as stp_dct
import variables.var_column as clmn
import variables.datamatrix as mtx

mtx_volume = Matrix.DataMatrix.load_old_object('volume',  any_stp_dict=stp_dct.setup_dict, is_for_archive=False)
mtx_part_number_to_activity = Matrix.DataMatrix.load_old_object('part_number_to_activity',
                                                                any_stp_dict=stp_dct.setup_dict, is_for_archive=False)
mtx_part_number = Matrix.DataMatrix.load_old_object('part_number',
                                                    any_stp_dict=stp_dct.setup_dict, is_for_archive=False)
mtx_corn_usage = Matrix.DataMatrix.load_from_json('corn_usage', root=stp_dct.setup_dict[dct.root_folder],
                                                  folder=stp_dct.setup_dict[dct.json_folder])
mtx_corn_usage.assign_datamatrix_to_empty_header(mtx_volume)

mtx_corn_usage.merge_datamatrix(right_datamatrix=mtx_part_number_to_activity,
                                desired_column_list=[clmn.activity_code, clmn.activity_usage, clmn.activity_uom,
                                                     clmn.part_number_uom],
                                left_on_list=[clmn.part_number_code], right_on_list=[clmn.part_number_code],
                                reset_left_index=True, reset_right_index=True, drop_right_on_list=True,
                                multilevel_datamatrix=True)
mtx_corn_usage.multiply_column_by_another(multiplier_clmn=(mtx.volume, clmn.volume),
                                          multiplicand_clmn=(mtx.part_number_to_activity, clmn.activity_usage),
                                          product_clmn=clmn.volume)
mtx_corn_usage.filter_nan_base_dataset(any_column=clmn.activity_code,
                                       error_message='no activity assigned to part number',
                                       reset_index=True, update_df_error=True)
mtx_corn_usage.trim_date(initial_date=pd.Timestamp(year=2019, month=7, day=1),
                         end_date=pd.Timestamp(year=2022, month=12, day=31), date_clmn=clmn.date, reset_index=False)
mtx_corn_usage.merge_dataframe(original_right_dataset=mtx_part_number,
                               desired_column_list=[clmn.part_number_description],
                               right_on_list=[clmn.part_number_code], left_on_list=[clmn.part_number_code],
                               reset_left_index=False, reset_right_index=True, drop_right_on_list=True,
                               multilevel_datamatrix=False)
mtx_corn_usage.write(any_stp_dict=stp_dct.setup_dict)
# mtx_volume.print()
# mtx_part_number_to_activity.print()