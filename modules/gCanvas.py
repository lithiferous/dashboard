from gspread_formatting import format_cell_ranges
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials
import gspread
import time


class Connection:
    def __init__(self, name, creds=None):
        """
        Connector for Google spreadsheets
        -> name: Spreadsheet name
        -> scope: Google APIs to connect to
        -> creditentials: Service account signed JSON file
        """
        self.name = name
        self.creditentials = creds if creds != None else 'config/creds_gs.json'
        self.scope = ['https://spreadsheets.google.com/feeds',
                      'https://www.googleapis.com/auth/drive']
        self.gcloud = self.get_connection()

    def get_connection(self):
        creditentials = ServiceAccountCredentials.from_json_keyfile_name(self.creditentials, self.scope)
        return gspread.authorize(creditentials).open(self.name)

    def get_sheet_by_index(self, sheet):
        return self.gcloud.get_worksheet(sheet)

    def get_sheet_index(self, sheet_name):
        sheets = self.gcloud.worksheets()
        for sheet in sheets:
            if sheet_name in str(sheet):
                return str(sheet).split("id:")[-1].replace('>', "")

    def get_sheet_by_name(self, sheet_name):
        return self.gcloud.worksheet(sheet_name)

    def create_sheet(self, new_sheet):
        return self.gcloud.add_worksheet(new_sheet, rows="100", cols="100")

    def list_sheets(self):
        return self.gcloud.worksheets()

    def duplicate_sheet(self, sheet, new_sheet):
        idx = self.get_sheet_index(sheet)
        return self.gcloud.duplicate_sheet(idx, new_sheet_name=new_sheet)


class gCanvas:
    def __init__(self, sheet, timeout=None):
        """
        Editor for Google spreadsheets
        -> sheet: index of the Spreadsheet
        -> timeout: timeout for separate updates
        """
        self.sheet = sheet
        self.max_cols = self.get_width()
        self.max_rows = self.get_height()
        self.timeout = timeout if timeout != None else None

    def get_as_df(self):
        return get_as_dataframe(self.sheet, index_col=0, evaluate_formulas=True)

    def get_height(self):
        return len(self.sheet.col_values(1)) + 1

    def get_width(self):
        defaultRow = 3  # dates row
        return len(self.sheet.row_values(defaultRow)) + 1

    def find_all(self, pattern):
        time.sleep(self.timeout)
        return self.sheet.findall(pattern)

    def find(self, pattern):
        return self.sheet.find(pattern)

    def format(self, fmt_list):
        format_cell_ranges(self.sheet, fmt_list)

    def update_cell(self, row, col, new_val):
        time.sleep(self.timeout)
        self.sheet.update_cell(row, col, new_val)

    def update_batch(self, cell_list):
        self.sheet.update_cells(cell_list, 'USER_ENTERED')

    def update_with_df(self, df):
        set_with_dataframe(self.sheet, df)
