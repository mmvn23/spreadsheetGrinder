import json
from Dataset.Column import MyColumn
import glb_var.addresses

MyColumn.load_json_list_from_spreadsheet(glb_var.addresses.CLMN_SETUP_FILE)

