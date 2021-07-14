import pandas as pd
import numpy as np
from unidecode import unidecode
import re, requests
from bs4 import BeautifulSoup

def bbref_id_df(json_loc='/Users/mbun/Code/dsi_galvanize/capstones/capstone_2/ideas/nba_injuries/all_player_bbref_info.json'):
    df_bbref_id = pd.read_json(json_loc)
    df_bbref_id['birth_date'] = pd.to_datetime(df_bbref_id['birth_date'])
    df_bbref_id['player_format'] = df_bbref_id['player'].apply(unidecode)
    df_bbref_id['player_format'] = df_bbref_id['player_format'].str.lower()
    df_bbref_id['player_format'] = df_bbref_id['player_format'].str.replace('*', '', regex=False)
    df_bbref_id['player_format'] = df_bbref_id['player_format'].str.replace('.', '', regex=False)
    return df_bbref_id

def injuries_df(json_loc='/Users/mbun/Code/dsi_galvanize/capstones/capstone_2/ideas/nba_injuries/pst_nba_injuries_all.json'):
    df_injuries_raw = pd.read_json(json_loc)
    df_injuries_raw.at[0, 'Date'] = pd.Timestamp('2019-12-30 00:00:00')
    df_injuries_raw = df_injuries_raw.sort_values('Date').reset_index(drop=True)
    df_injuries_raw['Player'] = df_injuries_raw['Healed'] + df_injuries_raw['Injured']
    df_injuries_raw['Status'] = np.where(df_injuries_raw['Healed'] == '', 'Injured', 'Healed')
    df_injuries_raw = df_injuries_raw[['Date', 'Team', 'Player', 'Status', 'Notes']]

    df_injuries = df_injuries_raw[df_injuries_raw['Status'] == 'Injured']
    # df_injuries = df_injuries[df_injuries['Notes'] != 'placed on IL']
    df_injuries = df_injuries[~df_injuries['Notes'].str.contains('coach')]
    return df_injuries, df_injuries_raw

def date_check(date, f, t):
    if (f-2 <= date.year) and (t+2 >= date.year):
        return True
    elif date.year < f:### Check later
        return True
    elif date.year <= t+ 4:
        # print(player, date.year, bbrid, bbrid_list, from_dates, to_dates, "*"*5)
        return True
    else:
        return False

def player_check(player, df, date):
    bbrid = None
    player_df = df['player_format'].str.contains(player)
    # print(player_df)
    if player_df.sum() == 1:
        bbrid, from_date, to_date = df[player_df][['bbref_id', 'from', 'to']].values[0]
        if date_check(date, from_date, to_date):
            # print(player, date.year, from_date, to_date)
            return bbrid
        
    elif player_df.sum() > 1:
        # print(df[player_df][['bbref_id', 'from', 'to']].values)
        for bbrid, from_date, to_date in df[player_df][['bbref_id', 'from', 'to']].values:
            # print(bbrid, from_date, to_date)
            if date_check(date, from_date, to_date):
                # print(player, date.year, from_date, to_date)
                # break
                return bbrid

    elif ' / ' in player:
        for plr in player.split(' / ')[::-1]:
            bbrid = player_check(plr, df, date)
            if bbrid != None:
                return bbrid

    elif len(player.split()) > 2:
        first2 = ' '.join(player.split()[:2])
        last2 = ' '.join(player.split()[-2:])
        bbrid_f2 = player_check(first2, df, date)
        bbrid_l2 = player_check(last2, df, date)
        if bbrid_f2 != None:
            # print(first2, bbrid_f2, '*')
            return bbrid_f2
        elif bbrid_l2 != None:
            # print(last2, bbrid_l2, '**')
            return bbrid_l2
    return None

def player_name_format(player,date):
    for i in [' Jr.', ' Sr.', ' VI', ' IV', ' III', ' II']:
        if i in player:
            if 'John Lucas' in player:
                break
            player = player.replace(i, '')
    
    player = player.lower()

    if 'ö' in player:
        player = player.replace('ö', 'o')
    
    player = re.sub(r' \(.*\)', '', player)
    player = re.sub(r'\(.*\) ', '', player)
    player = player.replace(')', '')

    if '.' in player:
        player = player.replace('.', '')
    if player == '':
        # print('**************************', player)
        player = 'blank line'
    if player == 'kings':
        player = 'sacramento kings'
        # print("****GOT KINGS****")
    if player == 'nate archibald':
        player = 'tiny archibald'
    if player == 'christian welp':
        player = 'chris welp'
    if player == 'bobby hansen':
        player = 'bob hansen'
    if (player == 'john wallace') and (int(date.year) == 2010):
        player = 'john wall'

    return player


def get_df(raw=0):
    df_bbrid = bbref_id_df()
    df_inj = injuries_df()[raw]
    bbrids = []
    c = 0

    for date, player in df_inj[['Date','Player']].values:
        ### Explicit BBrid
        if player == 'Charles Davis':
            bbrid = 'davisch01'
        elif player == 'Charles Jones (A.)':
            bbrid = 'jonesch02'
        elif player == 'Charles Jones (Rahmel)':
            bbrid = 'jonesch03'
        elif player == 'Dee Brown (b. 1984-08-17)':
            bbrid = 'brownde03'
        elif player == 'Charles Smith (Cornelius)':
            bbrid = 'smithch04'
        elif player == 'Michael Smith (John) (Providence)':
            bbrid = 'smithmi02'
        elif player == 'Marcus Williams (E.)':
            bbrid = 'willima04'
        elif player == 'Chris Wright':
            bbrid = 'wrighch02'
        else:
            player_format = player_name_format(player, date)
            bbrid = player_check(player_format, df_bbrid, date)
            if bbrid == None:
                bbrid = player_check(player_format[:-1], df_bbrid, date)
                if bbrid == None:
                    bbrid = 'DROPTHISLINE'
        bbrids.append(bbrid)
        c += 1
        if c % int(0.025*len(df_inj)) == 0:
            print(f'{round(100*c/len(df_inj), 1)}%', f'{c}/{len(df_inj)}')
    
    df_inj['bbref_id'] = bbrids
    df_inj = df_inj[df_inj['bbref_id'] != 'DROPTHISLINE']
    df = df_inj.join(df_bbrid.set_index('bbref_id'), on='bbref_id')
    df['age'] = df['Date'] - df['birth_date']
    # df.drop(['birth_date', 'colleges', 'player_format'], axis=1, inplace=True)ç
    df = df[['Date','player', 'Team', 'Status', 'Notes', 'bbref_id', 'from', 'to', 'pos', 'height', 'weight', 'age']]
    df['player'] = df['player'].str.replace('*', '', regex=False)
    return df

def set_df_date(df, date='1994-08-01'):
    return df[df['bbref_id'].isin(df[df['Date'] >= date]['bbref_id'].unique())].copy()


if __name__ == '__main__':
    # df_inj, df_inj_raw = injuries_df()
    # print(df_inj)
    # print(df_inj[df_inj['Player'] == 'Kings'])
    df = get_df(0)
    print(df)
    for bbref_id in df['bbref_id'].unique().tolist():
        if len(df[df['bbref_id'] == bbref_id]['player'].unique()) > 1:
            print(bbref_id, df[df['bbref_id'] == bbref_id]['Player'].unique())
    # df_bbrid = bbref_id_df()
    # player_set = set(df_bbrid['player_format'])
    # df_inj, df_inj_raw = injuries_df()

    # c = 0
    # bbrid_dict = {'bbref_id': []}
    # # player_list = set()
    # for date, player in df_inj[['Date','Player']].values:
    #     player_format = player_name_format(player, date)
    #     # print(player)
    #     bbrid = player_check(player_format, df_bbrid, date)
    #     if bbrid == None:
    #         bbrid = player_check(player_format[:-1], df_bbrid, date)
    #         if bbrid == None:
    #             bbrid = 'DROPTHISLINE'
    #             # player_list.add(player)
    #             print(c, player, date.year)
    #             c += 1
    #     #     else:
    #     #         print(player, bbrid)
    #     # else:
    #     #     print(player, bbrid)
    #     bbrid_dict['bbref_id'].append(bbrid)
    # df_inj['bbref_id'] = bbrid_dict['bbref_id']
    # print(df_inj[df_inj['bbref_id'] != 'DROPTHISLINE'])
    # from pprint import pprint
    # pprint(bbrid_dict)
    
    # c2 = 0
    # for player in player_list:
    #     c2 += 1
    #     print(c2, player)
    # for date, player in df_inj[['Date','Player']].values:#injured_players:
    #     player_raw = player
        
    #     for i in [' Jr.', ' Sr.', ' VI', ' IV', ' III', ' II']:
    #         if i in player:
    #             if 'John Lucas' in player:
    #                 break    
    #             player = player.replace(i, '')
       
    #     player = player.lower()
    #     if 'ö' in player:
    #         player = player.replace('ö', 'o')
    #     player = re.sub(r' \(.*\)', '', player)
    #     player = re.sub(r'\(.*\) ', '', player)

    #     if '.' in player:
    #         player = player.replace('.', '')
    #     # if player == 'dj augustine':
    #     #     player = 'dj augustin'
    #     if (player == 'john wallace') and (int(date.year) == 2010):
    #         player = 'john wall'
        
    #     if player not in player_set:
    #         found = False
    #         if '/' in player:
    #             for plr in player.split(' / '):
    #                 # print(plr)
    #                 if plr in player_set:
    #                     frm_to = df_bbrid[df_bbrid['player_format'] == plr].values[0].tolist()[2:4]
    #                     if date_check(date, frm_to[0], frm_to[1]):
    #                         player = plr
    #                         found = True
    #                         break
    #                     # print(plr, frm_to)
    #                 # elif len(plr.split()) > 2:
    #                 #     first2 = ' '.join(plr.split()[:2])
    #                 #     last2 = ' '.join(plr.split()[-2:])
    #                 #     print(first2, last2)
    #                 #     if first2 in player_set:
    #                 #         frm_to = df_bbrid[df_bbrid['player_format'] == first2].values[0].tolist()[2:4]
    #                 #         if date_check(date, frm_to[0], frm_to[1]):
    #                 #             player = first2
    #                 #             found = True
    #                 #             break
    #         if not found:
    #             # print()
    #             if player == '':
    #                 print(player, '*'*10)
                
    #             elif df_bbrid['player_format'].str.contains(player).sum() > 0:
    #                 plr = df_bbrid[df_bbrid['player_format'].str.contains(player)]['player'].values
    #                 # print(player, plr)
    #                 player = plr[0]
    #                 print()
    #             elif df_bbrid['player_format'].str.contains(player[:-1]).sum() > 0:
    #                 plr = df_bbrid[df_bbrid['player_format'].str.contains(player[:-2])]['player'].values
    #                 print(player, plr, '* '*10)
    #             else:
    #                 c += 1
    #                 print(c, player, date.year, player_raw)
    #             ##### 

    #             # print()
    #     player_df =  df_bbrid[df_bbrid['player_format'] == player]
    #     bbrid_list = player_df['bbref_id'].values
    #     from_dates = player_df['from'].values
    #     to_dates = player_df['to'].values
    #     bbrid = False
    #     if len(bbrid_list) > 1:
    #         for i, bid in enumerate(bbrid_list):
    #             if (from_dates[i]-2 <= date.year) and (to_dates[i]+2 >= date.year):
    #                 bbrid = bid
    #                 # print(player, date.year, bbrid, bbrid_list, from_dates, to_dates, "*"*5)
                    
    #                 break
    #             elif from_dates[i] > date.year:### Check later
    #                 bbrid = bid
    #                 break
    #             elif date.year <= to_dates[0]+4:
    #                 bbrid = bid
    #                 # print(player, date.year, bbrid, bbrid_list, from_dates, to_dates, "*"*5)
    #                 break
    #         if not bbrid:
    #             print(player, date.year, bbrid, bbrid_list, from_dates, to_dates)
    #             # print(player)
    #     else:
    #         bbrid = str(bbrid_list)   
    #         if list(from_dates) != []:
    #             if date_check(date, from_dates[0], to_dates[0]):
    #                 pass
    #             else:
    #                 print(player, date.year, bbrid, bbrid_list, from_dates, to_dates)

                    