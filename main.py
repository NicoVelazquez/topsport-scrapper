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
    horses_rows = horses_table.findAll("tr", recursive=False)
    if not horses_rows[1].find("td", {"class": "competitorNumColumn"}):
        horses_rows = horses_rows[::2]

    for horse in horses_rows:
        data['Meeting'].append(meeting_name)
        data['Race'].append(int(race_number))
        data['Trk Cond'].append(trk_cond)
        get_horse_info(horse, data)

    df_r = pd.DataFrame.from_dict(data)

    horse_table_winners = s.find("table", {"class": "results"})
    get_horse_winner_info(horse_table_winners, df_r)

    df_r = calculate_open_rank(df_r)
    df_r = calculate_spr_rank(df_r)
    return df_r


def get_horse_info(horse_row, data):
    if 'Scratched' in horse_row.findAll('td')[-1].text:
        data['Meeting'].pop()
        data['Race'].pop()
        data['Trk Cond'].pop()
        return

    tab_number = horse_row.find("td", {"class": "competitorNumColumn"}).text.strip()
    horse_name = horse_row.find("span", {"class": "rnnrName"}).text
    barrier = horse_row.find("span", {"class": "rnnrBarrier"}).text[1:][:-1]

    flucs = horse_row.findAll("td", {"class": "fluc"})
    flucs = [float(x.text) for x in flucs]
    if len(flucs) > 0:
        open_c = flucs[0]
        tfluc = max(flucs)
        spr = flucs[-1]
    else:
        open_c = 999
        tfluc = 999
        spr = 999
    #     Si me pide que lo tengo que sacar al que no tiene valores, es aca

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


def get_horse_winner_info(table, df_i):
    table_body = table.find("tbody")
    table_rows = table_body.findAll("tr")

    first_element_flag = True
    second_element_flag = False
    double_first_flag = (len(table_rows) == 5)
    triple_first_flag = (len(table_rows) == 6)
    tab_number = 0
    for t_row in table_rows:
        if not first_element_flag and not second_element_flag:
            wpl = t_row.find('th').text.split(' ')[-1][0]
            tab_number = t_row.find("span").text.split('.')[0]
            df_i.loc[df_i['Tab Number'] == tab_number, 'WPL'] = float(wpl)
            values = t_row.findAll("td")[-3:]
            plcdiv = values[-2].text
            if plcdiv not in ['ND', 'NSD', 'NTD']:
                df_i.loc[df_i['Tab Number'] == tab_number, 'PLCDIV'] = float(plcdiv)

        if second_element_flag:
            values = t_row.findAll("td")[-3:]
            plcdiv = values[-2].text
            if plcdiv not in ['ND', 'NSD', 'NTD']:
                df_i.loc[df_i['Tab Number'] == tab_number, 'PLCDIV'] = float(plcdiv)
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
            wpl = t_row.find('th').text.split(' ')[-1][0]
            tab_number = t_row.find("strong").text.split('.')[0]
            df_i.loc[df_i['Tab Number'] == tab_number, 'WPL'] = float(wpl)
            values = t_row.findAll("td")[-3:]
            bo3 = 0
            for v in values:
                if bo3 < float(v.text):
                    bo3 = float(v.text)
            df_i.loc[df_i['Tab Number'] == tab_number, 'BO3'] = float(bo3)
            stab = values[-2].text
            df_i.loc[df_i['Tab Number'] == tab_number, 'STAB'] = float(stab)
            first_element_flag = False
            second_element_flag = True


def calculate_open_rank(df_o):
    counter = 1
    convert_dict = {'Tab Number': int, 'Open': float, 'Open Rank': float}
    df_o = df_o.astype(convert_dict)

    df_o = df_o.sort_values('Open')
    df_o = df_o.reset_index(drop=True)

    for index, row_o in df_o.iterrows():
        df_o.at[index, 'Open Rank'] = counter
        if counter > 1:
            if df_o.at[index - 1, 'Open'] == df_o.at[index, 'Open']:
                if math.modf(df_o.at[index - 1, 'Open Rank'])[0] == 0:
                    df_o.at[index - 1, 'Open Rank'] = df_o.at[index - 1, 'Open Rank'] + 0.5
                df_o.at[index, 'Open Rank'] = df_o.at[index - 1, 'Open Rank']

        counter = counter + 1

    df_o = df_o.sort_values('Tab Number')
    return df_o


def calculate_spr_rank(df_s):
    counter = 1
    convert_dict = {'Tab Number': int, 'SPR': float, 'SPR Rank': float}
    df_s = df_s.astype(convert_dict)

    df_s = df_s.sort_values('SPR')
    df_s = df_s.reset_index(drop=True)

    for index, row_s in df_s.iterrows():
        df_s.at[index, 'SPR Rank'] = counter
        if counter > 1:
            if df_s.at[index - 1, 'SPR'] == df_s.at[index, 'SPR']:
                if math.modf(df_s.at[index - 1, 'SPR Rank'])[0] == 0:
                    df_s.at[index - 1, 'SPR Rank'] = df_s.at[index - 1, 'SPR Rank'] + 0.5
                df_s.at[index, 'SPR Rank'] = df_s.at[index - 1, 'SPR Rank']

        counter = counter + 1

    df_s = df_s.sort_values('Tab Number')
    return df_s


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
        for race_url in races_urls:
            df = get_race_info(race_url)
            final_df = final_df.append(df)

        # pool = Pool(12)
        # p = pool.map(get_race_info, races_urls)
        # for df in p:
        #     final_df = final_df.append(df)
        # pool.terminate()
        # pool.join()
    # df = get_race_info('https://www.topsport.com.au/Racing/Thoroughbreds/Hobart/R10/22500396')   #Test
    # final_df = final_df.append(df)      #Test

    end = time.time()
    print(end - start)
    print('Finished scrapping')

    print('Generating excel')

    path = os.getcwd()
    writer = pd.ExcelWriter(path + '/' + input_url + ' TS' + '.xlsx', engine='xlsxwriter')
    final_df.to_excel(writer, sheet_name='Sheet1', index=False)
    workbook = writer.book
    worksheet = writer.sheets['Sheet1']
    worksheet.freeze_panes(1, 0)
    workbook.close()

    print('Excel exported successfully')
