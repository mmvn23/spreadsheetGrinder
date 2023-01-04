import glb_var.others
import glb_var.addresses
import Dataset.utils
from Dataset.Column import MyColumn
from Dataset.BaseDataset import BaseDataset
from Dataset.RawDataset import RawDataset
from Dataset.Directory import Directory


# any_raw = RawDataset.load_from_spreadsheet(address=glb_var.addresses.DATASET_SETUP_FILE,
#                                            sheet=glb_var.addresses.RAW_DATASET_SHEET,
#                                            dataset_name='gimli_corn')
# any_raw.write_as_json()
any_raw_dataset = RawDataset.load_from_json('gimli_corn')

# volume_clmn = MyColumn.load_from_json('volume_clmn')
print(any_raw_dataset)

# print(any_raw)