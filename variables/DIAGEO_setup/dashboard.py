import pandas as pd

old_volume_to_be_loaded = False
historical_old_initial_date = pd.Timestamp(year=1900, month=1, day=1)
historical_new_initial_date = pd.Timestamp(year=1900, month=1, day=1)
historical_new_end_date = pd.Timestamp(year=2020, month=6, day=30)

archive_volume_to_be_loaded = False
archive_initial_date = pd.Timestamp(year=2019, month=7, day=1)
# refresh_initial_date = pd.Timestamp(year=2022, month=7, day=1)
refresh_initial_date = pd.Timestamp(year=2019, month=7, day=1)
refresh_end_date = pd.Timestamp(year=2023, month=1, day=31)


