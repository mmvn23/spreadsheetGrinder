from processing.DIAGEO.volume import load_volume
import pandas as pd
from processing.DIAGEO.pre_loading import pre_load_objects

pre_load_objects()
load_volume(previous_archive_volume_to_be_loaded=True, archive_initial_date=pd.Timestamp(year=1900, month=1, day=1),
            actual_initial_date=pd.Timestamp(year=2022, month=4, day=1),
            actual_end_date=pd.Timestamp(year=2099, month=12, day=31))

