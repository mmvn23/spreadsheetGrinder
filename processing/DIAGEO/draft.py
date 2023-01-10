from processing.DIAGEO.volume import load_volume
from Dataset import Raw, Matrix
import pandas as pd
import variables.setup.dashboard as dash

from pre_loading import pre_load_objects
from processing.DIAGEO.nomenclature import load_nomenclature
from processing.DIAGEO.uom_conversion import load_uom_conversion
from processing.DIAGEO.part_number import load_part_number

pre_load_objects()
load_nomenclature()
load_uom_conversion()
load_part_number()
load_volume(archive_volume_to_be_loaded=dash.archive_volume_to_be_loaded,
            archive_initial_date=dash.archive_initial_date,
            refresh_initial_date=dash.refresh_initial_date,
            refresh_end_date=dash.refresh_end_date)
