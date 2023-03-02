import variables.dict as dct
from Dataset import Raw, Matrix
import variables.DIAGEO_setup.my_dict as stp_dct


def load_activity(any_stp_dict):
    mtx_activity = Matrix.DataMatrix.load_from_json('activity',
                                                    root=any_stp_dict[dct.root_folder],
                                                    folder=any_stp_dict[dct.json_folder])
    mtx_nomenclature = Matrix.DataMatrix.load_old_object('nomenclature', any_stp_dict)
    mtx_activity.load_dataframe(any_raw_dataset_name_list=['activity_raw'],
                                any_mtx_nomenclature=mtx_nomenclature,
                                root_json=any_stp_dict[dct.root_folder],
                                folder_json=any_stp_dict[dct.json_folder])
    mtx_activity.write(any_stp_dict, save_dataframe=True, save_error=True)

    return


if __name__ == "__main__":
    load_activity(stp_dct.setup_dict)