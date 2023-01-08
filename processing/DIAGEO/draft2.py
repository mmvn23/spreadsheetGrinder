from processing.DIAGEO.volume import load_volume
from Dataset import Raw, Matrix
import pandas as pd
from pre_loading import pre_load_objects

pre_load_objects()
load_volume(old_volume_to_be_loaded=True, historical_old_initial_date=pd.Timestamp(year=1900, month=1, day=1),
            historical_new_initial_date=pd.Timestamp(year=2022, month=4, day=1),
            historical_new_end_date=pd.Timestamp(year=2099, month=12, day=31))

