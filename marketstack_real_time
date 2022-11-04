from pprint import pprint

import dataset
import requests
import datetime as dt
import xlwt


def realtime_data(API_KEY, stock_symbols):
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
        ('limit', '1'),
        ('offset', '0'),
    )
    print('Making initial call to get first 1000 data')
    response = requests.get('http://api.marketstack.com/v1/intraday', headers=headers, params=params, cookies=cookies,
                            verify=False)
    data = response.json()['data'][0]
    print(data)
    return data



API_KEY = "63a271b2ccba3618028b7cbf97ff7ccc"
stock_symbols = 'DJI.INDX'
data = realtime_data(API_KEY, stock_symbols)

db = dataset.connect('mysql://averyxgr_WP9FU:8@fqk/c/ft(YzTG4i@50.87.58.197/averyxgr_WP9FU')
db = dataset.connect('postgresql://averyxgr_WP9FU:8@fqk/c/ft(YzTG4i@50.87.58.197:5432/averyxgr_WP9FU')
