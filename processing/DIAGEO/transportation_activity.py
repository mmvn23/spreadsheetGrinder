import pandas as pd
from Dataset import Base, Raw, Matrix
import copy
import variables.dict as dct
import variables.DIAGEO_setup.my_dict as stp_dct
import variables.DIAGEO_setup.smartway_emission_factor as hard_code
import variables.var_column as clmn
import variables.datamatrix as mtx
import variables.general as var_gen
import utils.setup as ut_stp
import utils.general as ut_gen
import time


def load_transportation_activity(any_stp_dict):
    start_time = time.time()
    # 'dhl_shipment_detail23'
    [mtx_nomenclature, mtx_uom_conversion] = Matrix.DataMatrix.load_old_object_list(['nomenclature', 'uom_conversion'],
                                                                                    any_stp_dict)

    # multilayer not implemented
    mtx_transportation_activity = Matrix.DataMatrix.load_from_json('transportation_activity',
                                                                   root=stp_dct.setup_dict[dct.root_folder],
                                                                   folder=stp_dct.setup_dict[dct.json_folder])

    # load
        # dhl shipment - 'dhl_shipment_detail23'
        # dhl fleet - 'dhl_fleet_shuttle23' NOT WORKING
        # dhl non-dhl - 'dhl_non_dhl_managed23'
        # date parser from excel - OK
        # ryder beer 'ryder_beer23'
        # ryder tequila 'ryder_tequila23'
        # glass moves 'inbound_glass23'
    # define check protocol
    # load from old and load from new
    print('---------------------------------------------------------------------- TRANSPORTATION')

    mtx_transportation_activity.load_dataframe_from_family_list(base_dataset_family_name_list=['dhl_shipment_detail23',
                                                                                               'dhl_non_dhl_managed23',
                                                                                               'ryder_tequila23',
                                                                                               'ryder_beer23',
                                                                                               'inbound_glass23'],
                                                                any_stp_dict=any_stp_dict,
                                                                any_mtx_nomenclature=mtx_nomenclature,
                                                                any_mtx_uom_conversion=mtx_uom_conversion,
                                                                treat_date=False, load_all_files_within_folder=True,
                                                                load_all_sheets_on_spreadsheet=False,
                                                                run_auto_etl=False)
    # mtx_transportation_activity.load_dataframe_from_family_list(base_dataset_family_name_list=['inbound_glass23'],
    #                                                             any_stp_dict=any_stp_dict,
    #                                                             any_mtx_nomenclature=mtx_nomenclature,
    #                                                             any_mtx_uom_conversion=mtx_uom_conversion,
    #                                                             treat_date=False, load_all_files_within_folder=True,
    #                                                             load_all_sheets_on_spreadsheet=False,
    #                                                             run_auto_etl=False)
    # mtx_transportation_activity.load_dataframe_from_family_list(base_dataset_family_name_list=['ryder_tequila22'],
    #                                                             any_stp_dict=any_stp_dict,
    #                                                             any_mtx_nomenclature=mtx_nomenclature,
    #                                                             any_mtx_uom_conversion=mtx_uom_conversion,
    #                                                             treat_date=False, load_all_files_within_folder=True,
    #                                                             load_all_sheets_on_spreadsheet=False,
    #                                                             run_auto_etl=False)
    mtx_transportation_activity.apply_standard_index()

    mtx_transportation_activity.apply_nomenclature(any_mtx_nomenclature=mtx_nomenclature)
    mtx_transportation_activity.apply_nomenclature_to_column(any_mtx_nomenclature=mtx_nomenclature,
                                                             any_column=clmn.distance_si_uom)
    mtx_transportation_activity.apply_nomenclature_to_column(any_mtx_nomenclature=mtx_nomenclature,
                                                             any_column=clmn.weight_si_uom)

    mtx_transportation_activity.apply_uom_conversion_to_column(any_column=clmn.distance,
                                                               numerator_dict={dct.any_mtx_item:
                                                                            Base.BaseDataset.create_empty_dataframe(),
                                                                               dct.key_clmn: clmn.activity_code,
                                                                               dct.any_mtx_conversion:
                                                                               mtx_uom_conversion,
                                                                               dct.old_uom: clmn.distance_uom,
                                                                               dct.new_uom: clmn.distance_si_uom},
                                                               denominator_dict={}, constant_uom_multiplier=True)
    mtx_transportation_activity.apply_uom_conversion_to_column(any_column=clmn.weight,
                                                               numerator_dict={dct.any_mtx_item:
                                                                            Base.BaseDataset.create_empty_dataframe(),
                                                                               dct.key_clmn: clmn.activity_code,
                                                                               dct.any_mtx_conversion:
                                                                               mtx_uom_conversion,
                                                                               dct.old_uom: clmn.weight_uom,
                                                                               dct.new_uom: clmn.weight_si_uom},
                                                               denominator_dict={}, constant_uom_multiplier=True)
    mtx_transportation_activity.rename_column_list(old_clmn_list=[clmn.distance_uom, clmn.distance_si_uom,
                                                                  clmn.weight_uom, clmn.weight_si_uom],
                                                   new_clmn_list=['input uom', clmn.distance_uom,
                                                                  'input uom', clmn.weight_uom])
    # self.rename_column_list(old_clmn_list=[clmn.uom, clmn.si_uom], new_clmn_list=['input uom', clmn.uom])
    mtx_transportation_activity.apply_standard_index()
    mtx_transportation_activity.add_timestamp()

    mtx_transportation_activity.assure_column_integrity()
    mtx_transportation_activity.remove_duplicated_index()

    mtx_transportation_activity.write(any_stp_dict, save_dataframe=True, save_error=True)
    end_time = time.time()
    print("Processing time: ", end_time - start_time, "seconds.")
    return


if __name__ == "__main__":
    pd.set_option('display.max_columns', 50)
    pd.set_option('display.max_rows', 70000)
    load_transportation_activity(any_stp_dict=stp_dct.setup_dict)

