# How Long They Out? - NBA Injury Predictor
One of the major throughlines during the NBA 2020-21 Season was injuries to major players 

# The Data and Collection Pipeline
## Injury Dataset
The injury dataset was collected from prosportstransactions.com (PST). [[dataset](http://prosportstransactions.com/basketball/Search/SearchResults.php?Player=&Team=&BeginDate=&EndDate=&ILChkBx=yes&InjuriesChkBx=yes&Submit=Search)]

![pst_page_example](img/pst_example.png) Typical Page of Injury Table from PST

The dataset contains 62,243 rows of injury records including:
- New Injuries/Illnesses
- Injury Updates
- Players Recovering
- Player movement to and from Injured Reserve List (IL/IR)

And 5 Columns:
- Date: Date of Injury
- Team: Team of Injured Player
- Acquired: Name of Players recovering/returning to the team
- Relinquished: Name of Player injured/leaving team
- Notes: Text information on the injury and/or status

The dataset contains injury information from 1947-48 season of the BAA (predecessor to the NBA) to present. The first date in the dataset is actually 12-30-1899 is an error and actually supposed to be 12-30-2019 (see Willie Cauley-Stein's Illness in [NBA Official NBA Injury Report 12-30-19](https://ak-static.cms.nba.com/referee/injury/Injury-Report_2019-12-30_08PM.pdf)).

Although the dataset begins in 1947, more than 97% of the data occurs after July 1 1994, the beginning of the league calender for the 1994-95 NBA season (see first plot in Feature Engineering). Therefore the final dataset only contains injuries after 07-01-1994.

## Basketball-Reference IDs and Game Logs
Player specific information including game logs were collected from basketball-reference.com (BBRef)

Before collecting the game logs I first needed to connect the players in the injury dataset to their unique BBRef ID. I first scraped together all players BBRef IDs and the following information:
- Player Name (Basketball Hall of Famers indicated by *)
- BBRef ID
- NBA Career Start
- NBA Career End
- Position
- Height
- Weight
- Birth Date
- Colleges Attended

From 

## Technologies Used
Information from both sites were scraped using the Requests and BS4 (BeautifulSoup) python libraries. The raw HTML was stored with MongoDB, then the tables and info were scraped from the HTML, converted to JSON files and again stored in MongoDB.


# Feature Engineering
As can be seen from example above the dataset scraped from PST was very raw and required an extensive amount of cleaning and feature engineering to create a usable dataset for machine learning algorithms.

![dataset](img/daily_injury_data_counts.png)

## Target: Injury Duration
In the initial dataset, most injuries do not include return dates. The injury return dates were calculated based on using the player's game log to determine when he returned to the lineup.

The injury durations were then categorized as follows:
- Few Days: 0-3 Days [30% of Dataset]
- Days: 4-6 Days [16% of Dataset]
- Week: 7-13 Days [15% of Dataset]
- Weeks: 13-59 Days [17% of Dataset]
- Months: 60-365 Days [4% of Dataset]
- More Than A Year: > 365 Days [0.2% of Dataset]
- Season Ending: Player injured and does not play again that season until the first 10 games the following season [6% of Dataset]
- Out of the NBA: Player injured and does not play again that season and does not play in the NBA the following season for reason other than injury (i.e. retirement, playing in other leagues, and season-long suspensions) [12% of Dataset]

## Features from Notes Column

Injury locations (body parts):
- 

## Features from Game Logs

# EDA
Once the injury duration and injury type/status/location were categorized, I began to explore the data with respect of the injury duration categories.

![team_injuries](img/injury_cat_teams.png)


![position_injuries](img/injury_cat_positions.png)


![loc_injuries](img/injury_cat_loc.png)

![type_injuries](img/injury_cat_type.png)

![height_injuries](img/injury_cat_height.png)

![weight_injuries](img/injury_cat_weight.png)


#

# Model Selection

To be completed for capstone 3 project