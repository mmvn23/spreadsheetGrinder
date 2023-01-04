import glb_var.others
import glb_var.dict_names

class Directory:
    def __init__(self, folder, file, myformat=glb_var.others.CSV_FORMAT, sheet=None):
        self.folder = folder # string
        self.file = file # string
        self.format = myformat # string
        self.sheet = sheet # string

    def __str__(self):
        out_str = "\n     Folder: {folder}\n" \
                  "     File: {file}\n" \
                  "     Format: {format}\n" \
                  "     Sheet: {sheet}\n" \
            .format(folder=self.folder,
                    file=self.file,
                    format=self.format,
                    sheet=self.sheet)
        return out_str

    def to_dict(self):
        any_dict = {glb_var.dict_names.FOLDER: self.folder,
                    glb_var.dict_names.FILE: self.file,
                    glb_var.dict_names.FORMAT: self.format,
                    glb_var.dict_names.SHEET: self.sheet
                    }
        return any_dict