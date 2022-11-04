import time
from pprint import pprint

import dataset
import requests
import datetime as dt
from bs4 import BeautifulSoup

# Global Variables
API_KEY = "a2b5f680a25097eea88016bed0298957"
SYMBOLS = ['SNPS', 'QCOM', 'TSLA', 'GBPUSD']
TABLE_NAME = "stock_data"

# DB connection
# db = dataset.connect('mysql://averyxgr_WP9FU:8@fqk/c/ft(YzTG4i@50.87.58.197/averyxgr_WP9FU')
# db = dataset.connect('postgresql://averyxgr_WP9FU:8@fqk/c/ft(YzTG4i@50.87.58.197:5432/averyxgr_WP9FU')
db = dataset.connect('sqlite:///db.sqlite3')


# Get realtime data fro given stock
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
    response = requests.get('http://api.marketstack.com/v1/intraday', headers=headers, params=params, cookies=cookies,
                            verify=False)
    try:
        data = response.json()['data'][0]
        data['date'] = dt.datetime.fromisoformat(data['date'].split('+')[0])
        print(data)
        return data
    except:
        print(stock_symbols, ' : ', response.json())
        return None


# Get US Indexes
def get_index_live_data():
    headers = {
        'Connection': 'keep-alive',
        'Cache-Control': 'max-age=0',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.141 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'Accept-Language': 'en-US,en;q=0.9',
    }
    r = requests.get("https://www.investing.com/indices/usa-indices", headers=headers)
    soup = BeautifulSoup(r.text, 'lxml')
    indexes_data = []
    for a in soup.find('table', {'id': 'cr1'}).find_all('a'):
        current_url = 'https://www.investing.com' + a.get('href')
        current_request = requests.get(current_url, headers=headers)
        current_soup = BeautifulSoup(current_request.text, 'lxml')
        current_data = {}
        current_data['last'] = float(current_soup.find('span', {'id': 'last_last'}).text.replace(',', ''))
        div = current_soup.find('div', {'class': 'overviewDataTable'})
        for cdiv in div.find_all('div', {'class': 'inlineblock'}):
            if 'Close' in cdiv.text:
                current_data['close'] = float(cdiv.find_all('span')[1].text.replace(',', ''))
            elif 'Open' in cdiv.text:
                current_data['open'] = float(cdiv.find_all('span')[1].text.replace(',', ''))
            elif 'Volume' in cdiv.text:
                current_data['volume'] = cdiv.find_all('span')[1].text.replace(',', '')
                if current_data['volume'] == 'N/A':
                    current_data['volume'] = None
                else:
                    current_data['volume'] = float(current_data['volume'])
            elif 'Day\'s Range' in cdiv.text:
                current_data['high'] = float(cdiv.find_all('span')[1].text.split('-')[1].replace(',', ''))
                current_data['low'] = float(cdiv.find_all('span')[1].text.split('-')[0].replace(',', ''))

        current_data['date'] = dt.datetime.now()
        current_data['symbol'] = \
        current_soup.find('section', {'id': 'leftColumn'}).find('h1').text.split('(')[-1].split(')')[0]
        current_data['exchange'] = 'INDX'
        indexes_data.append(current_data)
        print(current_data)
    return indexes_data


# Getting all data from SYMBOLS variable and us indexes
def get_whole_data():
    data = []
    for symbol in SYMBOLS:
        temp = realtime_data(API_KEY, symbol)
        if temp is not None:
            data.append(temp)
    data += get_index_live_data()
    print(data)
    return data


# Running infinite function and after each cycle sleep 30 min
while True:
    db.query("DELETE FROM %s WHERE DATE < DATETIME('now', '-3 year')" % TABLE_NAME)
    data = get_whole_data()
    table = db.get_table('TABLE_NAME')
    for d in data:
        table.insert(d)
    time.sleep(30 * 60)
