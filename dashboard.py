from datetime import datetime, timedelta
import argparse
import imp

c = imp.load_source('conf', 'config/conf.py')
g = imp.load_source('gsync', 'modules/gCanvas.py')
s = imp.load_source('patch', 'modules/patcher.py')
r = imp.load_source('report', 'modules/reader.py')

class Dashboard:
    """
    Updates google spreadsheet information
    """
    def __init__(self, report_file,
                       orders_file,
                       clients_file,
                       period):
        self.connector = None
        self.report = report_file
        self.orders =  r.get_order_info(orders_file, clients_file, 'group')
        self.period = self.get_report_period(period)

    def get_report_period(self, delta):
        """
        Return period dates in fmt 'DD.MM - DD.MM'
        -> delta: weeks back (4 weeks for month)
        """
        return (datetime.today() - timedelta(weeks=delta)).strftime("%d.%m") \
                +'-'+ datetime.today().strftime("%d.%m")

    def upload_data(self):
        sheets = {'2. Триггеры — инкр. нед.':1,
                  '3. Триггеры — стат. нед.':2,
                  '4. Триггеры — инкр. месяц':3,
                  '5. Регулярка — статистика':4,
                  '6. Когортный по выручке':5,
                  '7. Когортный по вовлечению':6,
                  '8. Страт. сегментация':7}

        def patch_wrapper(name, df, connector, limit):
            sheet = g.gCanvas(self.connector.get_sheet_by_name(name))
            if limit == 'x':
                patch = s.Patcher(df, sheets.get(name), sheet.max_cols).patch
            else:
                patch = s.Patcher(df, sheets.get(name), sheet.max_rows).patch
            sheet.update_batch(patch)

        for sheet in sheets.keys():
            if '2' in sheet:
                patch_wrapper(sheet, self.orders, self.connector, 'x')
            if '3' in sheet:
                df = r.get_report(self.report, "Свод. данные (online)")
                patch_wrapper(sheet, df, self.connector, 'x')
            elif '5' in sheet:
                df = r.get_report(self.report, "Свод. данные (offline)")
                df = df[df.sent != 0]
                patch_wrapper(sheet, df, self.connector, 'y')

    def run_all(self):
        self.connector = g.Connection(c.name)
        self.upload_data()


if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('-p', dest='period', required=True, type=int,
                    help="Report period in weeks; Flag 1 - one week")
    p.add_argument('-r', dest="report_file", required=True, type=str,
                    help="MindBox campaigns file xlsx")
    p.add_argument('-o', dest="orders_file", required=True, type=str,
                    help="Orders file csv")
    p.add_argument('-c', dest="clients_file", required=True, type=str,
                    help="Clients file feather fmt")
    args = p.parse_args()
    d = Dashboard(args.report_file,
                  args.orders_file,
                  args.clients_file,
                  args.period)
    d.run_all()
