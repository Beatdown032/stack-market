from pprint import pprint
import dataset
import requests
import datetime as dt
import xlwt

DATE_FORMAT = xlwt.easyxf(num_format_str='yyyy-mm--dd')
TIME_FORMAT = xlwt.easyxf(num_format_str='hh:mm')
DATETIME_FORMAT = xlwt.easyxf(num_format_str='yyyy/mm/dd hh:mm')
TABLE_NAME = "stock_data"
ALL_TABLE_NAME = "stock_all_data"

API_KEY = "a2b5f680a25097eea88016bed0298957"
SYMBOLS = ['AAPL', 'SNPS', 'QCOM', 'TSLA', 'GGAAAAG']

# DB connection
print("Connection to DB")
db = dataset.connect('mysql://averyxgr_WP9FU:WgK@oql/nsb(q9@50.87.58.197/averyxgr_WP9FU')
print('Connected: Testing')
try:
    print(db.tables)
    table = db.get_table(TABLE_NAME)
    all_table = db.get_table(ALL_TABLE_NAME)
except Exception as e:
    print('DB connection FAILED!')
    print(e)


def pull3years_data(API_KEY, stock_symbols):
    # get 3 years ago date
    today = dt.date.today()
    years3ago = today - dt.timedelta(days=3 * 365)
    # defining cookies and headers for request call
    cookies = {
        '__cfduid': 'd893cbadd39f2bad91d5a867e2d477fb31611072406',
        '_ga': 'GA1.2.613476765.1611072462',
        '_gid': 'GA1.2.1324890607.1611072462',
        '__stripe_mid': '319f5d43-f2f0-4b50-8fdd-39290f8e173f7cfbaa',
        '__stripe_sid': '0a36e4f1-0bc0-4c8e-9695-1821c5a85e9d18fb28',
    }
    headers = {
        'Connection': 'keep-alive',
        'Cache-Control': 'max-age=0',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.141 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'Accept-Language': 'en-US,en;q=0.9',
    }

    # define params for initial call
    params = (
        ('access_key', API_KEY),
        ('symbols', stock_symbols),
        ('interval', '30min'),
        ('date_from', years3ago.strftime("%Y-%m-%d")),
        ('limit', '1000'),
        ('offset', '0'),
    )
    print('Making initial call to get first 1000 data')
    response = requests.get('http://api.marketstack.com/v1/intraday', headers=headers, params=params, cookies=cookies,
                            verify=False)
    if response.json().get('data') is None:
        print('ERROR!!!!!  ', stock_symbols, ' : ', response.json())
        return None
    print(response.json()['pagination'])
    full_data = response.json()['data']
    call_count = response.json()['pagination'].get('total') // 1000
    print("Making API call %d time to get full data for given stock symbol" % call_count)
    for i in range(call_count):
        offset = (i + 1) * 1000
        print('Fetching %d data' % offset)
        params = (
            ('access_key', API_KEY),
            ('symbols', stock_symbols),
            ('interval', '30min'),
            ('date_from', years3ago.strftime("%Y-%m-%d")),
            ('limit', '1000'),
            ('offset', str(offset)),
        )
        response = requests.get('http://api.marketstack.com/v1/intraday', headers=headers, params=params,
                                cookies=cookies,
                                verify=False)
        full_data += response.json()['data']
    print('Done: Got %d data for %s stock symbol' % (len(full_data), stock_symbols))
    return full_data


def format(value):
    # formating data to make it excell readable format
    if value is None:
        return ("null",)
    if isinstance(value, dt.datetime):
        return value, DATETIME_FORMAT
    if isinstance(value, dt.date):
        return value, DATE_FORMAT
    if isinstance(value, dt.time):
        return value, TIME_FORMAT
    return (value,)


def data2excel(data, stock_symbols):
    # Create workbook
    wb = xlwt.Workbook()
    ws = wb.add_sheet(stock_symbols)

    # Write header
    headers = data[0].keys()
    for column, header in enumerate(headers):
        ws.write(0, column, header)

    # Write data
    for row, row_data in enumerate(data, start=1):
        for column, key in enumerate(headers):
            ws.write(row, column, *format(row_data[key]))

    # Save file
    file_name = "%s_3years_data.xls" % stock_symbols
    wb.save(file_name)
    print('Data saved in a %s file. Please check it!' % file_name)


# Saving dat into db
def data2db(data):
    for d in data:
        d['date'] = dt.datetime.fromisoformat(d['date'].split('+')[0])
        table.insert(d)
        all_table.insert(d)


# running functions
for stock_symbols in SYMBOLS:
    data = pull3years_data(API_KEY, stock_symbols)
    if data is not None:
        data2excel(data, stock_symbols)
        data2db(data)
