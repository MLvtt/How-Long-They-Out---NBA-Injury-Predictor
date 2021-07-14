import requests, copy, json
from bs4 import BeautifulSoup
from pymongo import MongoClient

client = MongoClient('localhost', 27017)
db = client.nba_inj
pages = db.pages

url = "http://www.prosportstransactions.com/basketball/Search/SearchResults.php?Player=&Team=&BeginDate=&EndDate=&ILChkBx=yes&InjuriesChkBx=yes&Submit=Search&start="
for x in range(0, 62301, 25):
    page = int((x/25) + 1)
    if page in [177,285,394,583,627,696,1150,1761,1820,2080]:
        r = requests.get(url+str(x))
        pages.insert_one({'page': page, 'html': r.content})
        print(f"Page {page} into 'pages'")
    else:
        print(f"Page {page} skipped")

