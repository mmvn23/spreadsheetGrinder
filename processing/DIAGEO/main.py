from processing.DIAGEO.volume import load_volume
from Dataset import Raw, Matrix
import pandas as pd

from pre_loading import pre_load_objects
from processing.DIAGEO.nomenclature import load_nomenclature
from processing.DIAGEO.uom_conversion import load_uom_conversion
from processing.DIAGEO.part_number import load_part_number

pre_load_objects()
load_nomenclature()
load_uom_conversion()
load_part_number()

# load_volume(old_volume_to_be_loaded=False, historical_old_initial_date=pd.Timestamp(year=1900, month=1, day=1),
#             historical_new_initial_date=pd.Timestamp(year=2022, month=3, day=1),
#             historical_new_end_date=pd.Timestamp(year=2022, month=3, day=31))


load_volume(old_volume_to_be_loaded=True, historical_old_initial_date=pd.Timestamp(year=1900, month=1, day=1),
            historical_new_initial_date=pd.Timestamp(year=2022, month=4, day=1),
            historical_new_end_date=pd.Timestamp(year=2099, month=12, day=31))
