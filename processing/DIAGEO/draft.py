from processing.DIAGEO.volume import load_volume
import variables.DIAGEO_setup.dashboard as dash
from processing.DIAGEO.pre_loading import pre_load_objects
from processing.DIAGEO.nomenclature import load_nomenclature
from processing.DIAGEO.uom_conversion import load_uom_conversion
from processing.DIAGEO.part_number import load_part_number
from processing.DIAGEO.activity import load_activity
from processing.DIAGEO.part_number_to_activity import load_part_number_to_activity
from processing.DIAGEO.ppv import load_ppv
import variables.dict as dct
import variables.DIAGEO_setup.my_dict as stp_dct
from Dataset import Base,Raw, Matrix
import variables.var_column as clmn

pre_load_objects(stp_dct.setup_dict)
load_nomenclature(stp_dct.setup_dict)
load_uom_conversion(stp_dct.setup_dict)
load_part_number(stp_dct.setup_dict)
# load_activity(stp_dct.setup_dict)
# load_part_number_to_activity(stp_dct.setup_dict)
#
load_volume(any_stp_dict=stp_dct.setup_dict,
            previous_archive_volume_to_be_loaded=dash.previous_archive_volume_to_be_loaded,
            archive_initial_date=dash.archive_initial_date,
            actual_initial_date=dash.actual_initial_date,
            actual_end_date=dash.actual_end_date)



# load_ppv(stp_dct.setup_dict)

# mtx_ppv = Matrix.DataMatrix.load_old_object('ppv',  any_stp_dict=stp_dct.setup_dict, is_for_archive=True)
# mtx_ppv.reset_index(new_index_list=[clmn.part_number_code, clmn.location_l0, clmn.vendor_code])
# mtx_ppv.remove_duplicated_index()
# mtx_ppv_list = mtx_ppv.split_based_on_column_categories(any_column=clmn.category)
# Matrix.write_base_dataset_list(mtx_ppv_list, any_stp_dict=stp_dct.setup_dict)

# load mtx_part_number_to_activity
# cross volume with activity