import variables.dict as dct
from Dataset import Raw, Matrix
import variables.DIAGEO_setup.my_dict as stp_dct
import variables.var_column as clmn


def load_part_number_to_activity(any_stp_dict):
    mtx_part_number_to_activity = Matrix.DataMatrix.load_from_json('part_number_to_activity',
                                                    root=any_stp_dict[dct.root_folder],
                                                    folder=any_stp_dict[dct.json_folder])
    [mtx_nomenclature, mtx_uom_conversion, mtx_part_number, mtx_activity] = Matrix.DataMatrix.load_old_object_list(
        ['nomenclature', 'uom_conversion',
         'part_number', 'activity'], any_stp_dict)
    mtx_part_number_to_activity.load_dataframe(any_raw_dataset_name_list=['part_number_to_activity_raw'],
                                               any_mtx_nomenclature=mtx_nomenclature,
                                               root_json=any_stp_dict[dct.root_folder],
                                               folder_json=any_stp_dict[dct.json_folder])
    # mtx_part_number_to_activity.apply_uom_conversion_to_column(any_column=clmn.activity_usage,
    #                                                            numerator_dict={dct.any_mtx_conversion:
    #                                                                            mtx_uom_conversion,
    #                                                                            dct.any_mtx_item: mtx_activity,
    #                                                                            dct.key_clmn: clmn.activity_code,
    #                                                                            dct.old_uom: clmn.activity_uom,
    #                                                                            dct.new_uom: clmn.si_uom},
    #                                                            denominator_dict={})
    mtx_part_number_to_activity.print()
    mtx_part_number_to_activity.apply_uom_conversion_to_column(any_column=clmn.activity_usage,
                                                               numerator_dict={dct.any_mtx_conversion:
                                                                               mtx_uom_conversion,
                                                                               dct.any_mtx_item: mtx_activity,
                                                                               dct.key_clmn: clmn.activity_code,
                                                                               dct.old_uom: clmn.activity_uom,
                                                                               dct.new_uom: clmn.si_uom},
                                                               denominator_dict={dct.any_mtx_conversion:
                                                                                 mtx_uom_conversion,
                                                                                 dct.any_mtx_item: mtx_part_number,
                                                                                 dct.key_clmn: clmn.part_number_code,
                                                                                 dct.old_uom: clmn.part_number_uom,
                                                                                 dct.new_uom: clmn.si_uom})
    # mtx_part_number_to_activity.apply_uom_conversion_to_numerator_and_denominator(
    #     numerator_code_clmn=clmn.part_number_code,
    #     numerator_value_clmn=clmn.yield_part_number,
    #     numerator_uom_clmn=clmn.part_number_uom,
    #     mtx_numerator_uom=mtx_part_number,
    #     new_uom_clmn=clmn.si_uom,
    #     denominator_code_clmn=clmn.activity_code,
    #     denominator_value_clmn=clmn.yield_activity,
    #     denominator_uom_clmn=clmn.activity_uom,
    #     mtx_denominator_uom=mtx_activity,
    #     any_mtx_uom_conversion=mtx_uom_conversion)
    mtx_part_number_to_activity.print()
    mtx_part_number_to_activity.write(any_stp_dict, save_dataframe=True, save_error=True)
    # print('pn to act')
    # mtx_part_number_to_activity.print()
    return


if __name__ == "__main__":
    load_part_number_to_activity(stp_dct.setup_dict)