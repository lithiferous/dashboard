from datetime import datetime, timedelta
from oauth2client.service_account import ServiceAccountCredentials
import gspread
import time

class Connection:
    def __init__(self, name, creditentials=None):
        """
        Connector for Google spreadsheets
        -> name: Spreadsheet name
        -> scope: Google APIs to connect to
        -> creditentials: Service account signed JSON file
        """
        self.name = name
        self.creditentials = creditentials if creditentials != None else 'config/creds_gs.json'
        self.scope = ['https://spreadsheets.google.com/feeds',
                      'https://www.googleapis.com/auth/drive']
        self.gcloud = self.get_connection()

    def get_connection(self):
        creditentials = ServiceAccountCredentials.from_json_keyfile_name(self.creditentials, self.scope)
        return gspread.authorize(creditentials).open(self.name)

    def get_sheet_by_index(self, sheet):
        return self.gcloud.get_worksheet(sheet)

    def get_sheet_by_name(self, sheetname):
        return self.gcloud.worksheet(sheetname)

    def list_sheets(self):
        return self.gcloud.worksheets()

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

    def get_height(self):
        return len(self.sheet.col_values(1)) + 1

    def get_width(self):
        defaultRow = 3 #dates row
        return len(self.sheet.row_values(defaultRow)) + 1

    def find_all(self, pattern):
        time.sleep(self.timeout)
        return self.sheet.findall(pattern)

    def update_cell(self, row, col, new_val):
        time.sleep(self.timeout)
        self.sheet.update_cell(row, col, new_val)

    def update_batch(self, cell_list):
        self.sheet.update_cells(cell_list)
