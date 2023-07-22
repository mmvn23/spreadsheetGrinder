import variables.dict as dct
from Dataset import Raw, Matrix
import variables.DIAGEO_setup.my_dict as stp_dct
import variables.var_column as clmn


def load_vendor(any_stp_dict):
    mtx_vendor = Matrix.DataMatrix.load_from_json('vendor',
                                                       root=any_stp_dict[dct.root_folder],
                                                       folder=any_stp_dict[dct.json_folder])
    # mtx_nomenclature = Matrix.DataMatrix.load_old_object('nomenclature', any_stp_dict)
    mtx_vendor.load_dataframe_from_family(base_dataset_family_name='vendor_raw',
                                          any_stp_dict=any_stp_dict,
                                          treat_date=False, load_all_files_within_folder=True,
                                          load_all_sheets_on_spreadsheet=False,
                                          key_clmn=clmn.part_number_code)
    mtx_vendor.write(any_stp_dict, save_dataframe=True, save_error=True)

    return


if __name__ == "__main__":
    load_vendor(stp_dct.setup_dict)