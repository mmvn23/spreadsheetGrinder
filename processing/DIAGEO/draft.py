from processing.DIAGEO.volume import load_volume
import variables.DIAGEO_setup.dashboard as dash
from processing.DIAGEO.pre_loading import pre_load_objects
from processing.DIAGEO.nomenclature import load_nomenclature
from processing.DIAGEO.uom_conversion import load_uom_conversion
from processing.DIAGEO.part_number import load_part_number
from processing.DIAGEO.ppv import load_ppv
import variables.dict as dct
# import variables.DIAGEO_setup.file as dg_stp
import variables.DIAGEO_setup.my_dict as stp_dct

pre_load_objects(stp_dct.setup_dict)
load_nomenclature(stp_dct.setup_dict)
# load_uom_conversion(stp_dct.setup_dict)
# load_part_number(stp_dct.setup_dict)
# load_volume(any_stp_dict=stp_dct.setup_dict,
#             archive_volume_to_be_loaded=dash.archive_volume_to_be_loaded,
#             archive_initial_date=dash.archive_initial_date,
#             refresh_initial_date=dash.refresh_initial_date,
#             refresh_end_date=dash.refresh_end_date)
load_ppv(stp_dct.setup_dict)

