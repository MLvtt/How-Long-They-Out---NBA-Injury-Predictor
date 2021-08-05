import re
from data_cleaning import get_df, set_df_date, bbref_id_df
from bbref_gamelogs import BBRefScraper
from pymongo import MongoClient
import pandas as pd
import numpy as np
import string
from nltk.tokenize import word_tokenize
from nltk.stem.porter import PorterStemmer
from nltk.stem.wordnet import WordNetLemmatizer

client = MongoClient('localhost', 27017)
db = client.nba_inj
mongo_gamelogs = db.gamelogs

def pickle_inj_df(raw=0):
    df = get_df(raw)
    df = set_df_date(df)
    df.to_pickle('../data/df.pkl')
    # df.to_pickle(f'../data/df{raw}.pkl')
    print('done')

df = pd.read_pickle('../data/df1.pkl')

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
    # return_dates_df = get_return_dates(df)
    return_dates_df = pd.read_pickle('../data/df_inj_return.pkl')
    df = df.join(return_dates_df)
    ## Combine Modern Team Names (Canonical)
    df.loc[(df['Team'] == 'Sonics') | (df['Team'] == 'Thunder'), 'Team'] = 'Sonics/Thunder'
    df.loc[(df['Team'] == 'Wizards') | (df['Team'] == 'Bullets'), 'Team'] = 'Bullets/Wizards'
    df.loc[((df['Team'] == 'Hornets') & (df['Date'] < '2013-08-01')) | (df['Team'] == 'Pelicans'), 'Team'] = 'Hornets/Pelicans'
    df.loc[((df['Team'] == 'Hornets') & (df['Date'] >= '2013-08-01')) | (df['Team'] == 'Bobcats'), 'Team'] = 'Bobcats/Hornets'
    ### Only Injuries
    df = df[df.Status.eq('Injured')]
    ### Update Notes col
    df['Notes'] = df['Inj_Check']
    ### Convert Age to year with 2 decimal places
    df['age'] = round(df['age'] / np.timedelta64(1, "Y"), 2)
    ### Height from feet-inches to inches
    df['height'] = df['height'].apply(lambda x: int(x.split('-')[0])*12 + int(x.split('-')[1]))
    ### Drop rows of player currently injured
    df.drop(df[(df['Inj_Duration'].isnull()) & (df['Out_of_NBA'] == False)].index, inplace=True)
    ### Set Players who are out of the league following injury to NaT 
    df.loc[df['Out_of_NBA'] == True, ['Return_Date','Inj_Duration']] = pd.NaT ### DO I WANT NAT or something else?????
    ### Convert weight and New Injury/Out of NBA/Career Statuses to int
    df[['weight', 'New_Inj', 'Out_of_NBA', 'Season_Ending', 'Career']] = df[['weight', 'New_Inj', 'Out_of_NBA', 'Season_Ending', 'Career']].astype(int)
    ### League Year where rookie year = year 0
    df['League_Years'] = df['Season'] - df['from']
    ### Count of Instances of New injuries by player for career
    df['Num_Inj_Career'] = df.groupby('bbref_id')['New_Inj'].cumsum()
    ### Count of Instances of New injuries by player for season
    df['Num_Inj_Season'] = df.groupby(['bbref_id', 'Season'])['New_Inj'].cumsum()
    ### Split Up injury Dates
    df['Inj_Date_Day'] = df['Date'].dt.day
    df['Inj_Date_Month'] = df['Date'].dt.month
    df['Inj_Date_Year'] = df['Date'].dt.year
    df['Inj_Date_DoW'] = df['Date'].dt.weekday
    ### Split POS to Dummies
    for pos in ['G', 'F', 'C']:
        df['POS_'+pos] = df['pos'].str.contains(pos).astype(int)
    ### Game Log info floats
    df['days_lst_gm'] = df['days_lst_gm'].astype(float)
    gamelog_stats = ['mp_lst_gm', 'pts_last_game', 'reb_lst_gm', 'ast_lst_gm', 'pm_lst_gm',
                    'gms_7d', 't_mp_7d', 't_pm_7d', 'mpg_7d', 'ppg_7d', 'rpg_7d', 'apg_7d',
                    'pmg_7d', 'gms_14d', 't_mp_14d', 't_pm_14d', 'mpg_14d', 'ppg_14d',
                    'rpg_14d', 'apg_14d', 'pmg_14d', 'gms_30d', 't_mp_30d', 't_pm_30d',
                    'mpg_30d', 'ppg_30d', 'rpg_30d', 'apg_30d', 'pmg_30d', 'gms_szn',
                    't_mp_szn', 't_pm_szn', 'mpg_szn', 'ppg_szn', 'rpg_szn', 'apg_szn',
                    'pmg_szn', 'gms_career_b4', 't_mp_career_b4', 't_pm_career_b4',
                    'mpg_career_b4', 'ppg_career_b4', 'rpg_career_b4', 'apg_career_b4',
                    'pmg_career_b4']
    df[gamelog_stats] = df[gamelog_stats].astype(float).round(2)
    ### Teams and BBRef IDs as type category
    df[['Team', 'bbref_id']] = df[['Team', 'bbref_id']].astype('category')
    ### Injury Categorization
    df = injury_categorization(df)
    ### Categorize Injury Duration
    def injury_duration_categories(x):
        if x[1] == 1:
            return 'Out Of NBA'
        elif x[2] == 1:
            return 'Season Ending'
        elif x[0].days < 4:
            return 'Few Days'
        elif x[0].days < 7:
            return 'Days'
        elif x[0].days < 14:
            return 'Week'
        elif x[0].days < 60:
            return 'Weeks'
        else:
            return 'Months'
    df['Inj_Duration_Cat'] = df[['Inj_Duration', 'Out_of_NBA', 'Season_Ending']].apply(lambda x: injury_duration_categories(x), axis=1).astype('category')
    ### Drop Columns
    drop_columns = ['Date', 'Status', 'Notes', 
                    'pos', 'from', 'to', 
                    'Return_Date', 'Inj_Check', 
                    #'Inj_Duration', 
                    'New_Inj', 'Out_of_NBA', 'Season_Ending', 'Career']
    # df.drop(drop_columns, axis=1, inplace=True)
    return df.drop(drop_columns, axis=1), df

def injury_categorization(df):
    df = df[(df['Date'] >= '1994-07-01')&df['New_Inj']]

    notes = df.Notes.apply(lambda x: re.sub('/', ' ', x))
    
    tokens = notes.apply(lambda x: word_tokenize(x.lower()))

    stopwords_ = "a,able,about,across,after,all,almost,also,am,among,an,and,any,\
    are,as,at,be,because,been,but,by,can,could,dear,did,do,does,either,\
    else,ever,every,for,from,get,got,had,has,have,he,her,hers,him,his,\
    how,however,i,if,in,into,is,it,its,just,least,let,like,likely,may,\
    me,might,most,must,my,neither,no,of,off,often,on,only,or,other,our,\
    own,rather,said,say,says,she,should,since,so,some,than,that,the,their,\
    them,then,there,these,they,this,tis,to,too,twas,us,wants,was,we,were,\
    what,when,where,which,while,who,whom,why,will,with,would,yet,you,your".split(',')
    stopwords_ += ['dnp', 'dtd', 'day-to-day', 'out', 'day', 'season', 'week', 'weeks', 'month', 'months', 'approximate', 'approximately' 'indefinitely', 'placed']
    
    punctuation_ = set(string.punctuation)
    
    def filter_tokens(sent):
        return([w for w in sent if not w in stopwords_ and not w in punctuation_ and not w[0].isnumeric() and (not w[-1].isnumeric() or w == 'covid-19')])
    
    tokens_filtered = tokens.apply(lambda x: filter_tokens(x))
    
    stemmer_porter, lemmatizer = PorterStemmer(), WordNetLemmatizer()

    tokens_lemm_stem = tokens_filtered.apply(lambda x: [stemmer_porter.stem(lemmatizer.lemmatize(w)) for w in x])

    df_inj_filtered = df.loc[tokens_lemm_stem.apply(lambda x: 
                                                    # ('muscl' in x) and not 
                                                    (
                                                    ## Sick
                                                    ('ill' in x) or 
                                                    ('flu' in x) or 
                                                    ('flu-lik' in x) or 
                                                    ('viru' in x) or 
                                                    ('viral' in x) or 
                                                    ('covid-19' in x) or 
                                                    ('protocol' in x) or 
                                                    ('strep' in x) or 
                                                    ('cold' in x) or
                                                    ('bronchiti' in x) or 
                                                    ('gastroenter' in x) or 
                                                    ('respiratori' in x) or 
                                                    ('poison' in x) or 
                                                    
                                                    ## Rest
                                                    ('rest' in x) or 


                                                    ## Head
                                                    ('headach' in x) or 
                                                    ('migrain' in x) or 
                                                    ('concuss' in x) or 
                                                    ('facial' in x) or 
                                                    ('jaw' in x) or 
                                                    ('throat' in x) or 
                                                    ('head' in x) or 
                                                    ('mouth' in x) or  
                                                    ('oral' in x) or  
                                                    ('tooth' in x) or 
                                                    ('teeth' in x) or 
                                                    ('dental' in x) or 
                                                    ('nose' in x) or 
                                                    ('sinu' in x) or 
                                                    ('eye' in x) or 
                                                    ('ear' in x) or 
                                                    ('eardrum' in x) or 
                                                    
                                                    ## Spine
                                                    ('neck' in x) or 
                                                    ('collarbon' in x) or 
                                                    ('spine' in x) or 
                                                    ('spinal' in x) or 
                                                    ('lumbar' in x) or 
                                                    ('disc' in x) or 
                                                    ('disk' in x) or 
                                                    ('tailbon' in x) or 
                                                    ('back' in x) or 
                                                    

                                                    ## Shoulder
                                                    ('shoulder' in x) or 
                                                    ('cuff' in x) or 
                                                    ('quadricep' in x) or 
                                                    ('bicep' in x) or 
                                                    ('labrum' in x) or

                                                    ## Arm
                                                    ('arm' in x) or 
                                                    ('forearm' in x) or 
                                                    ('elbow' in x) or

                                                    ## Hand
                                                    ('wrist' in x) or 
                                                    ('thumb' in x) or 
                                                    ('hand' in x) or 
                                                    ('finger' in x) or 

                                                    ## Chest
                                                    ('pector' in x) or 
                                                    ('abdomin' in x) or 
                                                    ('chest' in x) or 
                                                    ('rib' in x) or 
                                                    ('obliqu' in x) or
                                                    ('appendectomi' in x) or
                                                    
                                                    ## Heart
                                                    ('heart' in x) or 
                                                    ('heartbeat' in x) or 
                                                    
                                                    ## Midsection
                                                    ('adductor' in x) or 
                                                    ('abductor' in x) or 
                                                    ('pelvi' in x) or 
                                                    ('hip' in x) or 
                                                    ('thigh' in x) or 
                                                    ('groin' in x) or 
                                                    ('hamstr' in x) or 
                                                    
                                                    ('ligament' in x) or 
                                                    ('bone' in x) or
                                                    
                                                    ## LEG
                                                    ('leg' in x) or 
                                                    ('band' in x) or 
                                                    ('tibia' in x) or 
                                                    ('fibula' in x) or 
                                                    
                                                    ## Knee
                                                    ('kene' in x) or 
                                                    ('knee' in x) or 
                                                    ('kneecap' in x) or 
                                                    ('meniscu' in x) or 
                                                    ('acl' in x) or 
                                                    ('patella' in x) or 
                                                    ('mcl' in x) or 
                                                    
                                                    ## Lower Leg
                                                    ('achil' in x) or 
                                                    ('shin' in x) or
                                                    ('calf' in x) or 
                                                    
                                                    ## Ankle
                                                    ('ankl' in x) or 
                                                    ('anke' in x) or 

                                                    ## Foot
                                                    ('plantar' in x) or 
                                                    ('heel' in x) or 
                                                    ('toe' in x) or 
                                                    ('feet' in x) or 
                                                    ('mid-foot' in x) or 
                                                    ('foot' in x)
                                                    )), :].copy()
    
    notes_inj_filtered = df_inj_filtered.Notes.apply(lambda x: re.sub('/', ' ', x))
    
    tokens_inj_filtered = notes_inj_filtered.apply(lambda x: word_tokenize(x.lower()))
    
    tokens_filtered = tokens_inj_filtered.apply(lambda x: filter_tokens(x))
   
    tokens_lemm_stem_filtered = tokens_filtered.apply(lambda x: [stemmer_porter.stem(lemmatizer.lemmatize(w)) for w in x])

    df_inj_filtered['Inj_Type_Illness'] = tokens_lemm_stem_filtered.apply(lambda x:
                                                                    (
                                                                    ## Sick
                                                                    ('ill' in x) or 
                                                                    ('flu' in x) or 
                                                                    ('flu-lik' in x) or 
                                                                    ('viru' in x) or 
                                                                    ('viral' in x) or 
                                                                    ('covid-19' in x) or 
                                                                    ('protocol' in x) or 
                                                                    ('strep' in x) or 
                                                                    ('cold' in x) or
                                                                    ('bronchiti' in x) or 
                                                                    ('gastroenter' in x) or 
                                                                    ('respiratori' in x) or 
                                                                    ('poison' in x)
                                                                    )
                                                                    ).astype(int).tolist()
    
    df_inj_filtered['Inj_Type_Rest'] = tokens_lemm_stem_filtered.apply(lambda x: 'rest' in x).astype(int)

    df_inj_filtered['Inj_Loc_Head'] = tokens_lemm_stem_filtered.apply(lambda x:
                                                                    ## Head
                                                                    ('headach' in x) or 
                                                                    ('migrain' in x) or 
                                                                    ('concuss' in x) or 
                                                                    ('facial' in x) or 
                                                                    ('jaw' in x) or 
                                                                    ('throat' in x) or 
                                                                    ('head' in x) or 
                                                                    ('mouth' in x) or  
                                                                    ('oral' in x) or  
                                                                    ('tooth' in x) or 
                                                                    ('teeth' in x) or 
                                                                    ('dental' in x) or 
                                                                    ('nose' in x) or 
                                                                    ('sinu' in x) or 
                                                                    ('eye' in x) or 
                                                                    ('ear' in x) or 
                                                                    ('eardrum' in x)                                                                  
                                                                    ).astype(int)

    df_inj_filtered['Inj_Loc_Spine'] = tokens_lemm_stem_filtered.apply(lambda x:
                                                                    ## Spine
                                                                    ('neck' in x) or 
                                                                    ('collarbon' in x) or 
                                                                    ('spine' in x) or 
                                                                    ('spinal' in x) or 
                                                                    ('lumbar' in x) or 
                                                                    ('disc' in x) or 
                                                                    ('disk' in x) or 
                                                                    ('tailbon' in x) or 
                                                                    ('back' in x)
                                                                    ).astype(int)

    df_inj_filtered['Inj_Loc_Shoulder'] = tokens_lemm_stem_filtered.apply(lambda x:
                                                                    ## Shoulder
                                                                    ('shoulder' in x) or 
                                                                    ('cuff' in x) or 
                                                                    ('quadricep' in x) or 
                                                                    ('bicep' in x) or 
                                                                    ('labrum' in x)
                                                                    ).astype(int)

    df_inj_filtered['Inj_Loc_Arm'] = tokens_lemm_stem_filtered.apply(lambda x:
                                                                    ## Arm
                                                                    ('arm' in x) or 
                                                                    ('forearm' in x) or 
                                                                    ('elbow' in x)
                                                                    ).astype(int)

    df_inj_filtered['Inj_Loc_Hand'] = tokens_lemm_stem_filtered.apply(lambda x:
                                                                    ## Hand
                                                                    ('wrist' in x) or 
                                                                    ('thumb' in x) or 
                                                                    ('hand' in x) or 
                                                                    ('finger' in x)
                                                                    ).astype(int)

    df_inj_filtered['Inj_Loc_Chest'] = tokens_lemm_stem_filtered.apply(lambda x:
                                                                    ## Chest
                                                                    ('pector' in x) or 
                                                                    ('abdomin' in x) or 
                                                                    ('chest' in x) or 
                                                                    ('rib' in x) or 
                                                                    ('obliqu' in x) or
                                                                    ('appendectomi' in x)
                                                                    ).astype(int)

    df_inj_filtered['Inj_Loc_Heart'] = tokens_lemm_stem_filtered.apply(lambda x:
                                                                    ## Heart
                                                                    ('heart' in x) or 
                                                                    ('heartbeat' in x)
                                                                    ).astype(int)

    df_inj_filtered['Inj_Loc_Midsection'] = tokens_lemm_stem_filtered.apply(lambda x:
                                                                    ## Midsection
                                                                    ('adductor' in x) or 
                                                                    ('abductor' in x) or 
                                                                    ('pelvi' in x) or 
                                                                    ('hip' in x) or 
                                                                    ('thigh' in x) or 
                                                                    ('groin' in x) or 
                                                                    ('hamstr' in x)
                                                                    ).astype(int)
                                                                                                                                   
    df_inj_filtered['Inj_Loc_Leg'] = tokens_lemm_stem_filtered.apply(lambda x:
                                                                    ## LEG
                                                                    ('leg' in x) or 
                                                                    ('band' in x) or 
                                                                    ('tibia' in x) or 
                                                                    ('fibula' in x)
                                                                    ).astype(int)
                                                                    
    df_inj_filtered['Inj_Loc_Knee'] = tokens_lemm_stem_filtered.apply(lambda x:
                                                                    ## Knee
                                                                    ('kene' in x) or 
                                                                    ('knee' in x) or 
                                                                    ('kneecap' in x) or 
                                                                    ('meniscu' in x) or 
                                                                    ('acl' in x) or 
                                                                    ('patella' in x) or 
                                                                    ('mcl' in x)
                                                                    ).astype(int)
                                                            
    df_inj_filtered['Inj_Loc_Lower_Leg'] = tokens_lemm_stem_filtered.apply(lambda x:
                                                                    ## Lower Leg
                                                                    ('achil' in x) or 
                                                                    ('shin' in x) or
                                                                    ('calf' in x)
                                                                    ).astype(int)
                                                                    
    df_inj_filtered['Inj_Loc_Ankle'] = tokens_lemm_stem_filtered.apply(lambda x:
                                                                    ## Ankle
                                                                    ('ankl' in x) or 
                                                                    ('anke' in x)
                                                                    ).astype(int)

    df_inj_filtered['Inj_Loc_Foot'] = tokens_lemm_stem_filtered.apply(lambda x:
                                                                    ## Foot
                                                                    ('plantar' in x) or 
                                                                    ('heel' in x) or 
                                                                    ('toe' in x) or 
                                                                    ('feet' in x) or 
                                                                    ('mid-foot' in x) or 
                                                                    ('foot' in x)
                                                                    ).astype(int)

    df_inj_filtered['Inj_Type_Soft_Tissue_1'] = tokens_lemm_stem_filtered.apply(lambda x:
                                                                                        # Soft Tissue 1
                                                                                        ('sore' in x) or
                                                                                        ('tight' in x) or
                                                                                        ('stiff' in x) or
                                                                                        ('stretch' in x) or
                                                                                        ('jam' in x) or
                                                                                        ('twist' in x) or
                                                                                        ('pull' in x)
                                                                                        ).astype(int)

    df_inj_filtered['Inj_Type_Soft_Tissue_2'] = tokens_lemm_stem_filtered.apply(lambda x:
                                                                                        ## Soft Tissue 2
                                                                                        ('sublux' in x) or
                                                                                        ('sublex' in x) or
                                                                                        ('sublax' in x) or
                                                                                        ('hyperextend' in x) or
                                                                                        ('hyper-extend' in x) or
                                                                                        ('pointer' in x) or
                                                                                        ('splint' in x) or
                                                                                        ('tendin' in x) or
                                                                                        ('spasm' in x)
                                                                                        ).astype(int)

    df_inj_filtered['Inj_Type_Dislocation'] = tokens_lemm_stem_filtered.apply(lambda x:
                                                                                        ## Dislocation
                                                                                        ('disloc' in x) or
                                                                                        ('disloact' in x) or
                                                                                        ('separ' in x)
                                                                                        ).astype(int)

    df_inj_filtered['Inj_Type_Concussion'] = tokens_lemm_stem_filtered.apply(lambda x:
                                                                                        ## Concussion
                                                                                        ('concuss' in x)
                                                                                        ).astype(int)

    df_inj_filtered['Inj_Type_Swell'] = tokens_lemm_stem_filtered.apply(lambda x:
                                                                                        ## Swell
                                                                                        ('bursiti' in x) or
                                                                                        ('inflam' in x) or
                                                                                        ('inflamm' in x) or
                                                                                        ('swell' in x) or
                                                                                        ('swollen' in x) or
                                                                                        ('bruis' in x) or
                                                                                        ('contus' in x)
                                                                                        ).astype(int)

    df_inj_filtered['Inj_Type_Sprain_Strain'] = tokens_lemm_stem_filtered.apply(lambda x:
                                                                                        ## Sprain/Strain
                                                                                        ('sprain' in x) or
                                                                                        ('spain' in x) or
                                                                                        ('strain' in x) or
                                                                                        ('stain' in x)
                                                                                        ).astype(int)

    df_inj_filtered['Inj_Type_Break'] = tokens_lemm_stem_filtered.apply(lambda x:
                                                                                        ## Tear/Break
                                                                                        ('stress' in x) or
                                                                                        ('broken' in x) or
                                                                                        ('broke' in x) or
                                                                                        ('fractur' in x) or
                                                                                        ('torn' in x) or
                                                                                        ('tear' in x) or
                                                                                        ('ruptur' in x)
                                                                                        ).astype(int)
    
    df_inj_filtered['Inj_Type_Cut'] = tokens_lemm_stem_filtered.apply(lambda x:
                                                                                        ## Cut
                                                                                        ('abras' in x) or
                                                                                        ('lacer' in x) or
                                                                                        ('cut' in x)
                                                                                        ).astype(int)
    
    df_inj_filtered['Surgery'] = tokens_lemm_stem_filtered.apply(lambda x:
                                                                                        ('appendectomi' in x) or
                                                                                        ('hospit' in x) or ####
                                                                                        ('surguri' in x) or
                                                                                        ('surgeri' in x)
                                                                                        ).astype(int)

    df_inj_filtered['On_IL'] = tokens_lemm_stem_filtered.apply(lambda x: ('il' in x) or ('ir' in x)).astype(int)

    return df_inj_filtered

def get_return_dates(df):
    df_inj = df[df['Status'].eq('Injured')].copy()
    players = df_inj['bbref_id'].unique() ### BBRef_ID for each player array
    return_date_df = pd.DataFrame() ### Empty DF for collecting return dates and indices
    return_date_dict = {}
    c = 0
    t = df_inj['bbref_id'].nunique()
    for player in players:
        c += 1

        cntr = 0 ### Counter    
        player_df = df[df['bbref_id'] == player] ### Get player injury df slice
        
        ### Get player gamelogs and create gamelogs_df
        player_gamelogs = mongo_gamelogs.find_one({'bbref_id': player})['gamelogs']
        gamelogs_df = pd.DataFrame(player_gamelogs).dropna(subset=['MP'])

        # prev_inj = None
        # prev_inj_date = pd.NaT
        prev_return_date = pd.NaT ### Set nan val for initial previous return date
        init_inj_info = ''
        for idx in player_df.index: ### Loop through each injury record
            date, injury = player_df['Date'][idx], player_df['Notes'][idx] ### injury date and injury notes
            status = player_df['Status'][idx]
            from_szn, to_szn = player_df['from'][idx], player_df['to'][idx] ### player career
            season = get_season_column(date) ### season of injury
            nba_career = 0 ### Injury occured during career
            out_of_league = 0 ### Flag for player being out of league season following injury
            if from_szn > season:
                # print('pre-career')
                nba_career = -1 ### Pre-Career, injury occured
            elif to_szn < season:
                # print('post-career')
                out_of_league = 1
                nba_career = 1 ### Post-Career, injury occured
            season_ending = 0
            try: ### Find next game player played post injury
                return_date =  min(pd.Timestamp(gamelogs_df[gamelogs_df['Date'] > date]['Date'].values[0]),
                                pd.Timestamp(player_df[player_df['Status'].eq('Healed')&(player_df['Date'] >= date)]['Date'].values[0]))
                return_season = get_season_column(return_date)
                seasons_out = return_season - season
                if seasons_out == 1:## Season ending?
                    return_game = gamelogs_df[gamelogs_df['Date'] > date]['Rk'].values[0]
                    if return_game < 11:
                        season_ending = 1
                elif seasons_out > 1:
                    if nba_career == 0:
                        gamelogs_df_no_drop = pd.DataFrame(player_gamelogs) ## gamelogs without dropping missed games/season
                        for szn in range(season+1, return_season): ### Loop thru missed seasons
                            szn_glog = gamelogs_df_no_drop[gamelogs_df_no_drop['Season'] == szn] ## missed season gamelog
                            reason = szn_glog['G'].values[0] ### season missed reason
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
                                return_date = pd.NaT#szn_glog['Date'].values[0] 
                                out_of_league = 1
                                break
                            elif len(szn_glog) > 1: ## Pulling Players with season with inactive for whole gamelog
                                pass
                            else: ### Gonna just handle all out_of_league as out of league instead of date
                                return_date = pd.NaT #szn_glog['Date'].values[0] 
                                out_of_league = 1
                                break 
                    # else:
                    #     # print(player, szn, nba_career) ### FIGURE THIS OUT
                    #     pass
                    #         # print()#[['Season','G']])
                elif seasons_out < 0:
                    print('WTF '*50)
                # print(season, return_season)
            except IndexError: ### For players that never return from injury (END OF NBA CAREER)
                ###END OF NBA CAREER
                if season != 2021:
                    out_of_league = 1
                else:
                    out_of_league = 0
                return_date = pd.NaT
                return_season = pd.NaT
            if return_date == prev_return_date: ### Check to see if status update (aka not new injury)
                cntr += 1
                # print(cntr, player, injury, date.date(), return_date.date())
                new_inj = 0
                info_gap_days = (date-init_inj_date).days
                
                if re.fullmatch(r'(^pla.*?n I\w$)', init_inj_info) and \
                    (not re.fullmatch(r'(^pla.*?n I\w$)', injury)) and \
                    (status == 'Injured') and (info_gap_days < 6):
                    inj_update = re.sub(r'(^pla.*?n I\w)|( \(.*?\))', '', injury)
                    if (inj_update != '') and (not updated_init_inj):
                        if inj_update[0] != ' ':
                            inj_update = ' ' + inj_update
                        # print()
                        # print(player, init_inj_info, str(init_inj_date)[:10], '\t', info_gap_days)
                        # print('-> ',idx, player, (str(date)[:10], str(return_date)[:10]), injury)

                        return_date_dict[init_inj_idx]['Inj_Check'] += inj_update
                        updated_init_inj = True

                # print("!!!")
                # INJURY NOTES HANDLE HERE
            elif status == 'Healed':
                new_inj = 0
            else:
                new_inj = 1
                init_inj_idx = idx
                init_inj_info = injury
                init_inj_date = date
                updated_init_inj = False
                # if re.fullmatch(r'(pla.*?n I\w$)', init_inj_info):
                #     pass
            ### GAME STATS
            gamelog_df_pre_inj = gamelogs_df[gamelogs_df['Date'] <= date]
            if gamelog_df_pre_inj.shape[0] > 0:
                ## Game before injury
                date_lst_gm, mp_lst_gm, pts_last_game, reb_lst_gm, ast_lst_gm, pm_lst_gm  = gamelog_df_pre_inj[['Date', 'MP', 'PTS', 'TRB', 'AST', '+/-']].values[-1]
                days_lst_gm = (date - date_lst_gm).days
                lst_gm = {'days_lst_gm':days_lst_gm, 'mp_lst_gm':float(mp_lst_gm), 
                          'pts_last_game':float(pts_last_game), 'reb_lst_gm':float(reb_lst_gm), 
                          'ast_lst_gm':float(ast_lst_gm), 'pm_lst_gm':float(pm_lst_gm)}

                # ga
                ## 7 days before injury
                glog_7d_b4 = gamelog_df_pre_inj[gamelog_df_pre_inj['Date'] >= (date - np.timedelta64(7, 'D'))]
                keys_7d = ['gms_7d', 't_mp_7d', 't_pm_7d', 'mpg_7d', 'ppg_7d', 'rpg_7d', 'apg_7d', 'pmg_7d']
                b4_7d = {k:float(v) for k,v in zip(keys_7d, gamelog_stats_b4_inj(glog_7d_b4))}

                ## 14 days before injury
                glog_14d_b4 = gamelog_df_pre_inj[gamelog_df_pre_inj['Date'] >= (date - np.timedelta64(14, 'D'))]
                keys_14d = ['gms_14d', 't_mp_14d', 't_pm_14d', 'mpg_14d', 'ppg_14d', 'rpg_14d', 'apg_14d', 'pmg_14d']

                b4_14d = {k:float(v) for k,v in zip(keys_14d, gamelog_stats_b4_inj(glog_14d_b4))}

                ## 30 days before injury
                glog_30d_b4 = gamelog_df_pre_inj[gamelog_df_pre_inj['Date'] >= (date - np.timedelta64(30, 'D'))]
                keys_30d = ['gms_30d', 't_mp_30d', 't_pm_30d', 'mpg_30d', 'ppg_30d', 'rpg_30d', 'apg_30d', 'pmg_30d']
                b4_30d = {k:float(v) for k,v in zip(keys_30d, gamelog_stats_b4_inj(glog_30d_b4))}

                
                ## Season up to that point
                glog_szn_b4 = gamelog_df_pre_inj[gamelog_df_pre_inj['Season'] == season]
                keys_szn = ['gms_szn', 't_mp_szn', 't_pm_szn', 'mpg_szn', 'ppg_szn', 'rpg_szn', 'apg_szn', 'pmg_szn']
                b4_szn = {k:float(v) for k,v in zip(keys_szn, gamelog_stats_b4_inj(glog_szn_b4))}

                ## Career
                keys_career_b4 = ['gms_career_b4', 't_mp_career_b4', 't_pm_career_b4', 'mpg_career_b4', 'ppg_career_b4', 'rpg_career_b4', 'apg_career_b4', 'pmg_career_b4']
                b4_career = {k:float(v) for k,v in zip(keys_career_b4, gamelog_stats_b4_inj(gamelog_df_pre_inj))}

                stats_b4_inj = {**lst_gm, **b4_7d, **b4_14d, **b4_30d, **b4_szn, **b4_career}
            else:
                stats_b4_inj = {}
            inj_duration = return_date - date
            return_date_dict[idx] = {'Return_Date': return_date, 'Inj_Duration': inj_duration, 'New_Inj': new_inj, 'Out_of_NBA': out_of_league, 'Season_Ending':season_ending, 'Career': nba_career, 'Inj_Check':injury, **stats_b4_inj}
            prev_return_date = return_date ### Set new prev_return_date for next item in loop
        print(c, '/', t, '-', player)
    
    return_date_df = pd.DataFrame(return_date_dict).T.sort_index()
    return return_date_df

def gamelog_stats_b4_inj(glog_b4_df):
    if glog_b4_df.shape[0] > 0:
        gms_b4 = glog_b4_df.shape[0]
        t_mp_b4, t_pm_b4 = glog_b4_df[['MP', '+/-']].sum()
        mpg_b4, ppg_b4, rpg_b4, apg_b4, pmg_b4 = glog_b4_df[['MP', 'PTS', 'TRB', 'AST', '+/-']].mean().round(2)
    else:
        gms_b4 = t_mp_b4 = t_pm_b4 = mpg_b4 = ppg_b4 = rpg_b4 = apg_b4 = pmg_b4 = np.nan
    return [gms_b4, t_mp_b4, t_pm_b4, mpg_b4, ppg_b4, rpg_b4, apg_b4, pmg_b4]

def get_season_column(date):
    date = pd.Timestamp(date)
    if (date.month < 8) or  ((date.month < 11) and (date.year == 2020)):
        return date.year
    else:
        return date.year + 1


if __name__ == '__main__':
    # gamelogs_to_mongo(df, c_start=526)
    # bbrid = 'irvinky01'
    # print(pd.DataFrame(format_gamelogs_from_mongo(bbrid)))
    # formatted_gamelogs_to_mongo()
    # pickle_inj_df(1)
    # print('1 done df0 now')
    # pickle_inj_df(0)
    # print(get_return_dates(df))
    # df = df.join(get_return_dates(df))
    # print(df[df['New_Inj'].eq(1)&df['Status'].eq('Healed')])
    # get_return_dates(df)
    # print()
    # df_to_pkl = get_return_dates(df)
    df_to_pkl1, df_to_pkl2 = format_injury_df(df)
    print(df_to_pkl1)
    df_to_pkl1.to_pickle('../data/df_final1.pkl')
    df_to_pkl2.to_pickle('../data/df_final2.pkl')
    print(df_to_pkl1.info())
    # print(df[(df['Date'].dt.month < 10) & (df['Date'].dt.month > 7)]['Date'].values[0])
    # print(format_injury_df(df))
    # print(df[(df['Date'].dt.month < 10) & (df['Date'].dt.month > 7)]['Date'].values[0])
    # print(df)

