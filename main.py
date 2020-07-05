import math
import time
from multiprocessing import Pool
from datetime import datetime, timedelta
import os

from bs4 import BeautifulSoup
import requests
import pandas as pd

import xlsxwriter


def get_meetings_rows(url_page):
    r = requests.get(url_page)
    s = BeautifulSoup(r.text, "html.parser")
    table = s.find("table", {"class": "RaceCard THOROUGHBREDS"})
    table_body = table.find("tbody")
    table_rows = table_body.findAll("tr")
    return table_rows


def get_races_urls(meeting_row):
    urls = []
    races_cell = meeting_row.findAll("td", {"class": ["final", "fixed"]})
    for cell in races_cell:
        href_to_race = cell.find("a", href=True)['href']
        urls.append('https://www.topsport.com.au' + href_to_race)
    return urls


def get_race_info(race_url):
    print(race_url)
    data = {'Meeting': [], 'Race': [], 'Trk Cond': [], 'Horse': [], 'Tab Number': [], 'Barrier': [],
            'WPL': [], 'BO3': [], 'STAB': [], 'PLCDIV': [],
            'Open': [], 'TFLUC': [], 'SPR': [], 'Open Rank': [], 'SPR Rank': [], }

    r = requests.get(race_url)
    s = BeautifulSoup(r.text, "html.parser")

    meeting_name = race_url.split('/')[5]
    race_number = race_url.split('/')[6][1:]
    trk_cond = s.find("div", {"class": "raceHeaderTitleBar"}).find("div").text.split(':')[1].strip().split('\r')[0]

    horses_table = s.find("table", {"class": "MarketTable RaceMarket"})
    horses_table_body_rows = horses_table.findAll("tr", recursive=False)
    horses_rows_tuples = list(zip(horses_table_body_rows[::2], horses_table_body_rows[1::2]))

    for horse in horses_rows_tuples:
        data['Meeting'].append(meeting_name)
        data['Race'].append(int(race_number))
        data['Trk Cond'].append(trk_cond)
        get_horse_info(horse, data)

    df = pd.DataFrame.from_dict(data)

    horse_table_winners = s.find("table", {"class": "results"})
    get_horse_winner_info(horse_table_winners, df)

    df = calculate_open_rank(df)
    df = calculate_spr_rank(df)
    return df


def get_horse_info(horse_tuple, data):
    if 'Scratched' in horse_tuple[0].findAll('td')[-1].text:
        data['Meeting'].pop()
        data['Race'].pop()
        data['Trk Cond'].pop()
        return

    tab_number = horse_tuple[0].find("td", {"class": "competitorNumColumn"}).text.strip()
    horse_name = horse_tuple[0].find("span", {"class": "rnnrName"}).text
    barrier = horse_tuple[0].find("span", {"class": "rnnrBarrier"}).text[1:][:-1]

    flucs_body = horse_tuple[1].findAll("tbody")[2]
    open_c = flucs_body.findAll('td')[0].text
    tfluc = flucs_body.findAll('td')[1].text
    spr = flucs_body.findAll('td')[2].text

    data['Horse'].append(horse_name)
    data['Tab Number'].append(tab_number)
    data['Barrier'].append(int(barrier))
    data['WPL'].append('')
    data['BO3'].append('')
    data['STAB'].append('')
    data['PLCDIV'].append('')
    data['Open'].append(float(open_c))
    data['TFLUC'].append(float(tfluc))
    data['SPR'].append(float(spr))
    data['Open Rank'].append(0)
    data['SPR Rank'].append(0)


def get_horse_winner_info(table, df):
    table_body = table.find("tbody")
    table_rows = table_body.findAll("tr")

    first_element_flag = True
    second_element_flag = False
    double_first_flag = (len(table_rows) == 5)
    triple_first_flag = (len(table_rows) == 6)
    tab_number = 0
    for row in table_rows:
        if not first_element_flag and not second_element_flag:
            wpl = row.find('th').text.split(' ')[-1][0]
            tab_number = row.find("span").text.split('.')[0]
            df.loc[df['Tab Number'] == tab_number, 'WPL'] = float(wpl)
            values = row.findAll("td")[-3:]
            plcdiv = values[-2].text
            if plcdiv not in ['ND', 'NSD', 'NTD']:
                df.loc[df['Tab Number'] == tab_number, 'PLCDIV'] = float(plcdiv)

        if second_element_flag:
            values = row.findAll("td")[-3:]
            plcdiv = values[-2].text
            if plcdiv not in ['ND', 'NSD', 'NTD']:
                df.loc[df['Tab Number'] == tab_number, 'PLCDIV'] = float(plcdiv)
            second_element_flag = False
            if double_first_flag:
                first_element_flag = True
                double_first_flag = False
                continue
            if triple_first_flag and not double_first_flag:
                first_element_flag = True
                triple_first_flag = False
                continue

        if first_element_flag:
            wpl = row.find('th').text.split(' ')[-1][0]
            tab_number = row.find("strong").text.split('.')[0]
            df.loc[df['Tab Number'] == tab_number, 'WPL'] = float(wpl)
            values = row.findAll("td")[-3:]
            bo3 = 0
            for v in values:
                if bo3 < float(v.text):
                    bo3 = float(v.text)
            df.loc[df['Tab Number'] == tab_number, 'BO3'] = float(bo3)
            stab = values[-2].text
            df.loc[df['Tab Number'] == tab_number, 'STAB'] = float(stab)
            first_element_flag = False
            second_element_flag = True


def calculate_open_rank(df):
    counter = 1
    convert_dict = {'Tab Number': int, 'Open': float, 'Open Rank': float}
    df = df.astype(convert_dict)

    df = df.sort_values('Open')
    df = df.reset_index(drop=True)

    for index, row in df.iterrows():
        df.at[index, 'Open Rank'] = counter
        if counter > 1:
            if df.at[index - 1, 'Open'] == df.at[index, 'Open']:
                if math.modf(df.at[index - 1, 'Open Rank'])[0] == 0:
                    df.at[index - 1, 'Open Rank'] = df.at[index - 1, 'Open Rank'] + 0.5
                df.at[index, 'Open Rank'] = df.at[index - 1, 'Open Rank']

        counter = counter + 1

    df = df.sort_values('Tab Number')
    return df


def calculate_spr_rank(df):
    counter = 1
    convert_dict = {'Tab Number': int, 'SPR': float, 'SPR Rank': float}
    df = df.astype(convert_dict)

    df = df.sort_values('SPR')
    df = df.reset_index(drop=True)

    for index, row in df.iterrows():
        df.at[index, 'SPR Rank'] = counter
        if counter > 1:
            if df.at[index - 1, 'SPR'] == df.at[index, 'SPR']:
                if math.modf(df.at[index - 1, 'SPR Rank'])[0] == 0:
                    df.at[index - 1, 'SPR Rank'] = df.at[index - 1, 'SPR Rank'] + 0.5
                df.at[index, 'SPR Rank'] = df.at[index - 1, 'SPR Rank']

        counter = counter + 1

    df = df.sort_values('Tab Number')
    return df


if __name__ == "__main__":
    print('Base url = https://www.topsport.com.au/Racing/Results/All/')
    print('Default date = yesterday')
    input_url = input('Enter date (ex.: 2020/07/01): ')
    if not input_url:
        input_url = (datetime.today() - timedelta(days=1)).strftime('%Y/%m/%d')
    url = "https://www.topsport.com.au/Racing/Results/All/" + input_url
    input_url = input_url.replace('/', '-')

    print('Started scrapping')
    start = time.time()

    final_df = pd.DataFrame()
    meetings_rows = get_meetings_rows(url)
    for row in meetings_rows:
        races_urls = get_races_urls(row)

        # pool = Pool(12)
        # p = pool.map(get_race_info, races_urls)
        # for df in p:
        #     final_df = final_df.append(df)
        # pool.terminate()
        # pool.join()

        for race_url in races_urls:
            df = get_race_info(race_url)
            final_df = final_df.append(df)
    # df = get_race_info('https://www.topsport.com.au/Racing/Thoroughbreds/Eagle_Farm/R7/22491423')   #Test
    # final_df = final_df.append(df)      #Test

    end = time.time()
    print(end - start)
    print('Finished scrapping')

    print('Generating excel')
    # To add this again, change the append from '' to 0
    # format_mapping = {'BO3': '${:,.2f}', 'STAB': '${:,.2f}', 'PLCDIV': '${:,.2f}', 'Open': '${:,.2f}',
    #                   'TFLUC': '${:,.2f}', 'SPR': '${:,.2f}'}
    # for key, value in format_mapping.items():
    #     final_df[key] = final_df[key].apply(value.format)
    # final_df = final_df.replace('$0.00', '')

    path = os.getcwd()
    writer = pd.ExcelWriter(path + '/' + input_url + ' TS2' + '.xlsx', engine='xlsxwriter')
    final_df.to_excel(writer, sheet_name='Sheet1', index=False)
    workbook = writer.book
    worksheet = writer.sheets['Sheet1']
    worksheet.freeze_panes(1, 0)
    workbook.close()

    print('Excel exported successfully')
