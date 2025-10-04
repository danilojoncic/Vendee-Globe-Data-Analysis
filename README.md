The aim of this project is for me to learn as much as possible in Data Engineering and Data Analysis, and to hopefully impress some people at The Ocean Race enough that i may get a job. Enjoy the ride...

## Discoveries:

#### 30.09.2025@15:32 
The French and English versions of the Vendee Globe website use different time zones
Classement mis à jour à 03h00, 07h00, 11h00, 15h00, 19h00 et 23h00. FRA
Ranking updated at 02h00, 06h00, 10h00, 14h00, 18h00 et 22h00. (UTC time) ENG


#### 30.09.2025@15:53
The tracking data from the first and first half of the second day is not available
10.11.2024 and 11.11.2024

#### 30.09.2025@15:59
On my macbook air m2 with 16gb ram it took 3:30 seconds to download all the available leaderboard data
using the threadpool in the code 
56.673.095 bytes (57 MB on disk) for 697 items

#### 30.09.2025@16:09
The VendeeGlobe website for the tracker is using weather forecasting for the "current" time


#### 30.09.2025@16:33
The .xlsx file has some very specific contents which need to be carefully parsed

### 30.09.2025@20:14
To open the .xlsx file needed to use python-calamine engine 

### 02.10.2025@17:15
Studying Apache Spark, starting to like it. Plan is to use it for the reading and organizing the 697 .xlsx files

### 04.10.2025@01:22
When parsing the excel with pandas dataframes came to the realisation that Jean Le Cam is the exception with his name
also found out this symbol exists: "°", used to represent degreees, but not needed as im turing strings for coordinates into floats


### 04.10.2025@01:35
Thinking of writting a small dictionary or guide book with all the domain knowledge 
so other non sailors can hopefully understand the ideas and the concepts


### 04.10.2025@02:01
By carefully parsing from .xlsx to csv, and getting rid of the excel junk decreased the size of a file 
from 82KB to 7KB that more than 10X decrease in size just for the intermediary dataframe, will write data comparison once 
saved to csv and later on to Postgres
Thinking of writting a small dictionary or guide book with all the domain knowledge 
so other non sailors can hopefully understand the ideas and the concepts



### Acknowledgments:
- https://blog.geotribu.net/2024/12/02/track-the-vendée-globe-race-with-python-and-qgis/?utm_source=chatgpt.com#steps-to-follow
- Functor Data Engineering Course
