import variables.PERSONAL_setup.my_dict as stp_dct

from processing.PERSONAL.pre_loading import pre_load_objects
from processing.PERSONAL.cashflow import load_cashflow

pre_load_objects(stp_dct.setup_dict)
load_cashflow(any_stp_dict=stp_dct.setup_dict)