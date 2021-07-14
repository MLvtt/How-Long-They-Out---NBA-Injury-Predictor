from re import I
from data_cleaning import *
from bbref_gamelogs import BBRefScraper
from pymongo import MongoClient

client = MongoClient('localhost', 27017)
db = client.nba_inj
mongo_gamelogs = db.gamelogs

def pickle_inj_df(raw=0):
    df = get_df(raw)
    df = set_df_date(df)
    df.to_pickle(f'./df{raw}.pkl')
    print('done')

df = pd.read_pickle('./df.pkl')

# print(df)

def gamelogs_to_mongo(df=df, c_start=0, c_end=None):
    df_bbrid = bbref_id_df()
    c = c_start
    players = df['bbref_id'].unique()
    if c_end == None:
        players = players[c_start:]
    else:
        players = players[c_start:c_end+1]
    for player in players:
        ### Remove document if existing
        if mongo_gamelogs.find_one({'bbref_id': player}) != None:
            mongo_gamelogs.delete_one({'bbref_id': player})
            print('Removed existing document', player)
        ### Get player career from/to years
        from_year, to_year = df_bbrid[df_bbrid['bbref_id'] == player][['from', 'to']].values[0]
        # print(c, player, from_year, to_year)
        print(c, player, from_year, '-', to_year)
        ### Scrape Reg Season and Playoffs gamelogs
        try:
            regszn, playoffs = BBRefScraper(player).get_player_career_gamelog(from_year, to_year)
        except TimeoutError:
            print('* * * TO * * *')
            regszn, playoffs = BBRefScraper(player).get_player_career_gamelog(from_year, to_year)
        print('gamelog created.')
        ### Add Gamelogs to mongodb
        mongo_gamelogs.insert_one({'bbref_id': player, 'regszn': regszn, 'playoffs': playoffs})
        print(c, player, 'Done')
        c += 1

def format_gamelogs_from_mongo(bbrid):
    gamelogs = mongo_gamelogs.find_one({'bbref_id': bbrid})

    if gamelogs['playoffs'] == None:
        gamelog_df = pd.DataFrame(gamelogs['regszn'])
    else:
        gamelog_df = pd.DataFrame(gamelogs['regszn'] + gamelogs['playoffs'])

    ### Add +/- column if not in gamelogs
    if '+/-' not in gamelog_df.columns:
        gamelog_df['+/-'] = np.nan
    ### Date to Datetime
    gamelog_df['Date'] = pd.to_datetime(gamelog_df['Date'])

    ### Convert Stats Categories to Float
    float_cols = ['FG', 'FGA', 'FG%', 
                '3P', '3PA', '3P%', 
                'FT', 'FTA', 'FT%', 
                'ORB', 'DRB', 'TRB', 
                'AST', 'STL', 'BLK', 
                'TOV', 'PF', 'PTS', 
                'GmSc', '+/-']
    gamelog_df[float_cols] = gamelog_df[float_cols].astype(float)

    ### Minutes Played to Minutes Float
    gamelog_df['MP'] = gamelog_df['MP'].apply(lambda x: float(x.split(':')[0]) + float(x.split(':')[1])/60 
                                                        if isinstance(x, str) else np.nan)

    ### Set Inactive note to G column
    gamelog_df.loc[gamelog_df['G'].isna(),'G'] = gamelog_df.loc[gamelog_df['G'].isna(),'GS']

    ### Set GS to -1 if out
    gamelog_df.loc[(gamelog_df['GS'] != '0') & (gamelog_df['GS'] != '1'), 'GS'] = -1
    gamelog_df['GS'] = gamelog_df['GS'].astype(int)

    ### Home/Away to 0/1 binary
    gamelog_df['Home/Away'] = gamelog_df['Home/Away'].apply(lambda x: 1 if x == '@' else 0)

    ### Extract win/loss margin
    gamelog_df['Margin'] = gamelog_df['Result'].apply(lambda x: int(re.findall(r'\((.*?)\)', x)[0]) 
                                                                if (not pd.isnull(x)) and (str(x)[-2:] != '()') 
                                                                else np.nan)

    ### Extract W/L and convert to 1/0 binary
    gamelog_df['Result'] = gamelog_df['Result'].apply(lambda x: 1 if (not pd.isnull(x)) 
                                                                and (x.startswith('W')) else 0 
                                                                if not pd.isnull(x) else np.nan)

    ### Set Columns Order
    gamelog_df = gamelog_df[['Rk', 'G', 'Date', 'Season', 
                             'Series', 'Tm', 'Opp', 'Home/Away', 
                             'Result', 'Margin', 'GS', 'MP', 
                             'FG', 'FGA', 'FG%', '3P', '3PA', '3P%', 
                             'FT', 'FTA', 'FT%', 'ORB', 'DRB', 'TRB', 
                             'AST', 'STL', 'BLK', 'TOV', 'PF', 'PTS', 'GmSc', '+/-']]
    
    ### Sort by date and reset index
    gamelog_df.sort_values('Date', inplace=True)
    gamelog_df.reset_index(drop=True, inplace=True)

    return list(gamelog_df.T.to_dict().values())

def formatted_gamelogs_to_mongo(i_start=0):
    players = df['bbref_id'].unique()[i_start:]
    i = i_start
    for player in players:
        print(i, player)
        ### Remove if full gamelog already there
        mongo_gamelogs.update_one({'bbref_id': player}, 
                                  {'$unset': {'gamelogs': 1}})
        ### Format Gamelog
        newdf_list = format_gamelogs_from_mongo(player)
        print(i, player, 'Formatted')
        ### Add full gamelog to mongo
        mongo_gamelogs.update_one({'bbref_id': player}, 
                                  {'$set': {'gamelogs': newdf_list}})
        print(i, player, 'Added to mongo')
        i += 1


def format_injury_df(df):
    ### Add Season Column
    df['Season'] = df['Date'].apply(lambda x: get_season_column(x))
    ### Generate return dates df and join to injury df
    return_dates_df = get_return_dates(df)
    df = df.join(return_dates_df)
    ### Convert Age to year with 2 decimal places
    df['age'] = round(df['age'] / np.timedelta64(1, "Y"), 2)
    ### Height from feet-inches to inches
    df['height'] = df['height'].apply(lambda x: int(x.split('-')[0])*12 + int(x.split('-')[1]))
    ### Drop rows of player currently injured
    df.drop(df[(df['Inj_Duration'].isnull()) & (df['Out_of_NBA'] == False)].index, inplace=True)
    ### Set Players who are out of the league following injury to NaT 
    df.loc[df['Out_of_NBA'] == True, ['Return_Date','Inj_Duration']] = pd.NaT ### DO I WANT NAT or something else?????
    ### Convert weight and New Injury/Out of NBA/Career Statuses to int
    df[['weight', 'New_Inj', 'Out_of_NBA', 'Career']] = df[['weight', 'New_Inj', 'Out_of_NBA', 'Career']].astype(int)
    ### League Year where rookie year = year 0
    df['League_Years'] = df['Season'] - df['from']
    ### Count of Instances of New injuries by player for career
    df['Num_Inj_Career'] = df.groupby('bbref_id')['New_Inj'].cumsum()
    ### Count of Instances of New injuries by player for season
    df['Num_Inj_Season'] = df.groupby(['bbref_id', 'Season'])['New_Inj'].cumsum()
    return df


def get_return_dates(df):
    players = df['bbref_id'].unique() ### BBRef_ID for each player array
    return_date_df = pd.DataFrame() ### Empty DF for collecting return dates and indices
    cntr = 0 ### Counter
    return_date_dict = {}

    for player in players:
        # print(player)
        player_df = df[df['bbref_id'] == player] ### Get player injury df slice
        # print(player_df)
        
        ### Get player gamelogs and create gamelogs_df
        player_gamelogs = mongo_gamelogs.find_one({'bbref_id': player})['gamelogs']
        gamelogs_df = pd.DataFrame(player_gamelogs).dropna(subset=['MP'])
        # print(gamelogs_df)

        return_dates = [] ### Empty list for collecting return dates for player
        prev_return_date = pd.NaT ### Set nan val for initial previous return date

        for idx in player_df.index: ### Loop through each injury record
            date, injury = player_df['Date'][idx], player_df['Notes'][idx] ### injury date and injury notes
            from_szn, to_szn = player_df['from'][idx], player_df['to'][idx] ### player career
            season = get_season_column(date) ### season of injury
            # print(idx, player, season, injury)
            nba_career = 0 ### Injury occured during career
            out_of_league = 0 ### Flag for player being out of league season following injury
            if from_szn > season:
                # print('pre-career')
                nba_career = -1 ### Pre-Career, injury occured
            elif to_szn < season:
                # print('post-career')
                out_of_league = 1
                nba_career = 1 ### Post-Career, injury occured
            try: ### Find next game player played post injury
                return_date = pd.Timestamp(gamelogs_df[gamelogs_df['Date'] > date]['Date'].values[0])
                return_season = get_season_column(return_date)
                seasons_out = return_season - season
                if seasons_out == 1:
                    # print("!"*3)
                    pass
                elif seasons_out > 1:
                    if nba_career == 0:
                        # print(' ! '*5)
                        # print(idx, player, (from_szn, to_szn), injury, season, return_season, date.date(), return_date.date())
                        gamelogs_df_no_drop = pd.DataFrame(player_gamelogs)
                        for szn in range(season+1, return_season): ### Loop thru missed seasons
                            szn_glog = gamelogs_df_no_drop[gamelogs_df_no_drop['Season'] == szn]
                            reason = szn_glog['G'].values[0]
                            if any(x in reason.lower() for x in ['injury', 'illness', 'waived']):
                                # print(reason)
                                pass
                            elif reason == 'Did Not Play':
                                # print(player, szn, reason)
                                if player != 'willial02':
                                    return_date = szn_glog['Date'].values[0]
                                    out_of_league = 1
                                    break
                            elif 'retired' in reason.lower(): ### Retired out until they comeback
                                # print(player,  'Retired!!!')
                                out_of_league = 1
                            elif ('other' in reason.lower()) or ('baseball' in reason.lower()): ### Keeping baseball even though jordan wasn't out injured
                                ### COULD LOOK UP GAMELOGS FROM OTHER LEAGUES???????????? Not worth it right now 
                                return_date = szn_glog['Date'].values[0] ############## OR SHOULD IT BE pd.NaT # No because they played after
                                # print(player,'new return date', str(return_date)[:10], '* '*10)
                                out_of_league = 1
                                break
                            elif len(szn_glog) > 1: ## Pulling Players with season with inactive for whole gamelog
                                pass
                            # elif 'Did Not Play - (Not in NBA)' == reason: #### TEMPORARY to see whatelse left
                                pass
                            else: ### Gonna just handle all out_of_league as out of league instead of date
                                # print(idx, player, (from_szn, to_szn), injury, season, return_season, date.date(), return_date.date())
                                # print(szn_glog[['G', 'Date', 'Season']].values)
                                return_date = szn_glog['Date'].values[0] ############## OR SHOULD IT BE pd.NaT # No because they played after
                                # print(player, 'new return date', str(return_date)[:10])
                                # print()
                                out_of_league = 1
                                break 
                            # szn_glog['G'].values[0]
                    else:
                        # print(player, szn, nba_career) ### FIGURE THIS OUT
                        pass
                            # print()#[['Season','G']])
                elif seasons_out < 0:
                    print('WTF '*50)
                # print(season, return_season)
            except IndexError: ### For players that never return from injury (END OF NBA CAREER)
                ###END OF NBA CAREER
                if season != 2021:
                    out_of_league = 1
                else:
                    out_of_league = 0
                # print(player, date.date())
                # if player_df[player_df['bbref_id'] == player]['to'].values[0] == date.year:
                #     # print(player, date.date(), 'End of NBA Career')
                #     pass
                # else:
                #     print(player, date.date(), '!!!!!!!')
                return_date = pd.NaT
                return_season = pd.NaT
            
            if return_date == prev_return_date: ### Check to see if status update (aka not new injury)
                cntr += 1
                # print(cntr, player, injury, date.date(), return_date.date())
                new_inj = 0
                # print("!!!")
                # INJURY NOTES HANDLE HERE
            else:
                new_inj = 1
            inj_duration = return_date - date
            return_date_dict[idx] = {'Return_Date': return_date, 'Inj_Duration': inj_duration, 'New_Inj': new_inj, 'Out_of_NBA': out_of_league, 'Career': nba_career}
            return_dates.append(return_date) ### Add return date to player return date list
            prev_return_date = return_date ### Set new prev_return_date for next item in loop

    return_date_df = pd.DataFrame(return_date_dict).T.sort_index()
    return return_date_df

def get_season_column(date):
    date = pd.Timestamp(date)
    if (date.month < 8) or  ((date.month < 11) and (date.year == 2020)):
        return date.year
    else:
        return date.year + 1

def split_pos_col(df):
    for pos in ['G', 'F', 'C']:
        df['POS_'+pos] = df['pos'].str.contains(pos).astype(int)
    ##### DROP pos????
    return df

###ADD STATS COLUMNS

if __name__ == '__main__':
    # gamelogs_to_mongo(df, c_start=526)
    # bbrid = 'irvinky01'
    # print(pd.DataFrame(format_gamelogs_from_mongo(bbrid)))
    # formatted_gamelogs_to_mongo()
    # pickle_inj_df(1)
    print(format_injury_df(df))
    # print(df[(df['Date'].dt.month < 10) & (df['Date'].dt.month > 7)]['Date'].values[0])
    # print(format_injury_df(df))
    # print(df[(df['Date'].dt.month < 10) & (df['Date'].dt.month > 7)]['Date'].values[0])
    # print(df)

