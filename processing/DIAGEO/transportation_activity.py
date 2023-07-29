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


def load_transportation_activity(any_stp_dict):
    # 'dhl_shipment_detail23'
    [mtx_nomenclature, mtx_uom_conversion] = Matrix.DataMatrix.load_old_object_list(['nomenclature', 'uom_conversion'],
                                                                                    any_stp_dict)

    # multilayer not implemented
    mtx_transportation_activity = Matrix.DataMatrix.load_from_json('transportation_activity',
                                                                   root=stp_dct.setup_dict[dct.root_folder],
                                                                   folder=stp_dct.setup_dict[dct.json_folder])

    mtx_transportation_activity.load_dataframe_from_family(base_dataset_family_name='dhl_shipment_detail23',
                                                           any_stp_dict=any_stp_dict,
                                                           any_mtx_nomenclature=mtx_nomenclature,
                                                           any_mtx_uom_conversion=mtx_uom_conversion,
                                                           treat_date=False, load_all_files_within_folder=True,
                                                           load_all_sheets_on_spreadsheet=False, run_auto_etl=False)

    # any_raw_emission_list = load_smartway_report_list(['raw_emission_factor_smartway_20',
    #                                                    'raw_emission_factor_smartway_21',
    #                                                    'raw_emission_factor_smartway_22',
    #                                                    'raw_emission_factor_smartway_23'])
    #
    # any_mtx_emission_factor.concat_base_dataset_list(any_basedataset_list=any_raw_emission_list)
    # any_mtx_emission_factor.get_date_interval_from_fiscal_year(initial_date_clmn=clmn.initial_date,
    #                                                            end_date_clmn=clmn.end_date,
    #                                                            fiscal_year_clmn=clmn.fiscal_year)
    # any_mtx_emission_factor.apply_nomenclature_to_column(any_column=clmn.si_uom, any_mtx_nomenclature=mtx_nomenclature)
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
                                                               denominator_dict={})
    mtx_transportation_activity.apply_uom_conversion_to_column(any_column=clmn.weight,
                                                               numerator_dict={dct.any_mtx_item:
                                                                            Base.BaseDataset.create_empty_dataframe(),
                                                                               dct.key_clmn: clmn.activity_code,
                                                                               dct.any_mtx_conversion:
                                                                               mtx_uom_conversion,
                                                                               dct.old_uom: clmn.weight_uom,
                                                                               dct.new_uom: clmn.weight_si_uom},
                                                               denominator_dict={})
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
    return


if __name__ == "__main__":
    pd.set_option('display.max_columns', 50)
    pd.set_option('display.max_rows', 70000)
    load_transportation_activity(any_stp_dict=stp_dct.setup_dict)