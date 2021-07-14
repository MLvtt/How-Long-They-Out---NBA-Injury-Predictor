import requests, copy, json
from bs4 import BeautifulSoup
from pymongo import MongoClient

client = MongoClient('localhost', 27017)
db = client.nba_inj
pages = db.pages
all_rows = []
total_pages = pages.count_documents({})
for i in range(total_pages):
    page = i+1
    r = pages.find_one({'page': page})['html']
    soup = BeautifulSoup(r, "html.parser")
    div = soup.find("div", {"class": "container"})
    table = div.find("table")
    rows = table.find_all("tr")
    empty_row = {"Date": None, 
                 "Team": None, 
                 "Healed": None, 
                 "Injured": None, 
                 "Notes": None}
    for row in rows[1:]:
        new_row = copy.copy(empty_row)
        columns = row.find_all("td")
        new_row['Date'] = columns[0].text.strip()
        new_row['Team'] = columns[1].text.strip()
        new_row['Healed'] = columns[2].text.strip()[2:]
        new_row['Injured'] = columns[3].text.strip()[2:]
        new_row['Notes'] = columns[4].text.strip()
        all_rows.append(new_row)  
    print(f"Page {page} scraped")
with open('../data/pst_nba_injuries_all.json', 'w') as outfile:
    json.dump(all_rows, outfile)