import requests, re, json
from pprint import pprint
from bs4 import BeautifulSoup


players_info = []
letters = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'.lower()
for ltr in letters:
    url = f'https://www.basketball-reference.com/players/{ltr}/'
    r = requests.get(url)   
    soup = BeautifulSoup(r.content, 'html.parser')
    div = soup.find("div", {"class": "table_container"})
    table = div.find("table", {"class": "sortable"})
    tbody = table.find("tbody")
    rows = tbody.find_all('tr') 
    for row in rows:
        player_name = row.find('th').getText()
        bbref_id = row.find('th')['data-append-csv']
        player_info = row.find_all('td')
        career_from = player_info[0].getText()
        career_to = player_info[1].getText()
        position = player_info[2].getText()
        height = player_info[3].getText()
        weight = player_info[4].getText()
        birthdate = player_info[5].getText()
        colleges = player_info[6].getText()
        players_info.append({'player': player_name, 'bbref_id': bbref_id, 
                             'from': career_from, 'to': career_to, 
                             'pos': position, 'height': height, 'weight': weight, 
                             'birth_date': birthdate, 'colleges': colleges})


with open('/Users/mbun/Code/dsi_galvanize/capstones/capstone_2/ideas/nba_injuries/all_player_bbref_info.json', 'w') as outfile:
    json.dump(players_info, outfile)