import re, requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
from pymongo import MongoClient


class BBRefScraper:
    def __init__(self, bbref_id):
        self.id = bbref_id
        self.missed_szn = False
        
    def game_log_scraper(self, year):
        url = f'https://www.basketball-reference.com/players/{self.id[0]}/{self.id}/gamelog/{year}'
        r = requests.get(url)
        soup = BeautifulSoup(r.content, 'html.parser')
        div = soup.find("div", {"class": "table_container"})
        if div == None:
            return None
        table = div.find("table", {"class": "row_summable"})
        rows = table.find_all('tr')[1:]
        player_stats = [[td.getText() 
                         for td in rows[i].find_all('td')]
                         for i in range(len(rows))]
        column_headers = [th.getText() 
                          for th in table.find_all('tr', limit=2)[0].find_all('th')]
        column_headers[5], column_headers[7] = 'Home/Away', 'Result'
        empty_row = {x: np.nan for x in column_headers}
        game_log = []
        rk = 0
        for i_r, row in enumerate(player_stats):
            if row != []:
                new_row = empty_row.copy()
                for i_c, col in enumerate(column_headers):
                    # print(row, col)
                    if i_c == 0:
                        rk += 1
                        new_row[col] = rk
                    elif row[7][0].isalpha():
                        if i_c == 8:
                            new_row[col] = row[7]
                        elif (i_c < 8) and (row[i_c-1] != ''):
                            new_row[col] = row[i_c-1]
                    elif row[i_c-1] != '':
                        new_row[col] = row[i_c-1]
                    
                new_row['Series'] = 'REG'
                new_row['Season'] = year
                game_log.append(new_row)
        return game_log

    def playoff_game_log_scraper(self):
        url = f'https://www.basketball-reference.com/players/{self.id[0]}/{self.id}/gamelog-playoffs/'
        r = requests.get(url)
        soup = BeautifulSoup(r.content, 'html.parser')
        div = soup.find("div", {"class": "table_container"})
        if div == None:
            return None
        table = div.find("table", {"class": "row_summable"})
        rows = table.find_all('tr')[1:]
        player_stats = [[td.getText() 
                        for td in rows[i].find_all('td')] 
                        for i in range(len(rows))]
        column_headers = [th.getText() 
                        for th in table.find_all('tr', limit=2)[0].find_all('th')]
        column_headers[2], column_headers[5], column_headers[8] = 'Date', 'Home/Away', 'Result'
        empty_row = {x: np.nan for x in column_headers}
        game_log = []
        rk = 0
        for i_r, row in enumerate(player_stats):
            if (row != []) and (row[3] != ''):
                new_row = empty_row.copy()
                for i_c, col in enumerate(column_headers):
                    if i_c == 0:
                        rk += 1
                        new_row[col] = rk
                    elif row[8][0].isalpha():
                        if i_c == 9:
                            new_row[col] = row[8]
                        elif (i_c < 9) and (row[i_c-1] != ''):
                            new_row[col] = row[i_c-1]
                    elif row[i_c-1] != '':
                        new_row[col] = row[i_c-1]
                if new_row['Series'] == np.nan:
                    if new_row['Opp'] == game_log[-1]['Opp']:
                        new_row['Series'] = game_log[-1]['Series']
                if new_row['G#'] == np.nan:
                    if new_row['Opp'] == game_log[-1]['Opp']:
                        new_row['G#'] = int(game_log[-1]['G#']) + 1
                    else:
                        new_row['G#'] = 1
                new_row['Season'] = int(new_row['Date'][:4])
                game_log.append(new_row)
            # else:
            #     # print('!!!')
            #     pass
            # print(new_row)
        return game_log
    
    def get_missed_seasons(self):
        url = f'https://www.basketball-reference.com/players/{self.id[0]}/{self.id}.html'
        r = requests.get(url)
        soup = BeautifulSoup(r.content, 'html.parser')
        div = soup.find("div", {"class": "table_container"})
        table = div.find("table", {"class": "row_summable"})
        table.getText()
        rows = table.find_all('tr')
        player_stats = [[td.getText() 
                        for td in rows[i].find_all('td')]
                        for i in range(len(rows))]
        seasons_out = {int(re.sub(r'[-][0-9][0-9]', '', x[0]))+1: 
                        {'Season': int(re.sub(r'[-][0-9][0-9]', '', x[0]))+1,
                        'Date': pd.to_datetime(str(int(re.sub(r'[-][0-9][0-9]', '', x[0]))+1)+'-01-01'),
                        'G': x[2].replace('\xa0', ' - ')}
                        for x in player_stats if len(x) == 3}
        # print(seasons_out)
        if seasons_out == {}:
            return None
        return seasons_out

    def get_player_career_gamelog(self, from_year, to_year):
        career_reg_szn_game_log = []
        for year in range(from_year, to_year+1):
            game_log = self.game_log_scraper(year)
            if game_log == None:
                # print(self.id, year)
                if self.missed_szn == False:
                    self.missed_szn = self.get_missed_seasons()

                if self.missed_szn == None:
                    game_log = self.game_log_scraper(year)

                elif year in self.missed_szn.keys():
                    game_log = [self.missed_szn[year]]
                    print(self.id, year, game_log[0]['G'])
                
                if game_log == None:
                    # print(self.id, year, '!!!!!!')

                    game_log = [{'Season': year,
                                'Date': pd.Timestamp(f'{year}-01-01 00:00:00'),
                                'G': 'Did Not Play - (Not in NBA)'}]
                    # print(game_log)
                    # continue
            if not isinstance(game_log, list):
                print(year, type(game_log))

            career_reg_szn_game_log += game_log
        playoffs_game_log = self.playoff_game_log_scraper()
        if playoffs_game_log == None:
            # print(self.id, 'PLAYOFFS')
            playoffs_game_log = self.playoff_game_log_scraper()
            if playoffs_game_log == None:
                # print(self.id, 'PLAYOFFS!!!!!!')
                pass
        return career_reg_szn_game_log, playoffs_game_log

                


if __name__ == '__main__':
    from pprint import pprint
    bbref_id = 'mahorri01'
    KI = BBRefScraper(bbref_id)
    # game_log = KI.game_log_scraper(2015)
    regszn, playoffs = KI.get_player_career_gamelog(1981, 1999)
    # [print(game) for game in regszn if not isinstance(game, dict)]
        
    # print(regszn)
    # glog_playoffs = KI.playoff_game_log_scraper()
    df = pd.DataFrame(regszn + playoffs)
    print(df)
    # df_playoffs = pd.DataFrame(glog_playoffs)

    # pd.to_datetime(df['Date'])
    # df.sort_values('Date', inplace=True)
    # df['ORB'].astype(int)
    # print(df[df['Season'].eq(1998) & df['Series'].eq('REG')])
    # print(df_playoffs)
    # print(pd.DataFrame(regszn)['G'].count())
    # pprint(glog)