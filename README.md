# Vendée Globe Data Analysis (but more like Engineering) Project

The goal of this project is to deepen my knowledge in Data Engineering and Data Analysis, and hopefully impress the team at The Ocean Race enough to land a position. This repository documents the entire journey of building an end-to-end data pipeline for race tracking data.

## Pipeline Overview
<img width="1037" height="520" alt="Screenshot 2025-10-06 at 00 28 59" src="https://github.com/user-attachments/assets/92a38f34-b1ac-4271-8761-5e8c884dc637" />



## Pipeline Steps

1. **Data Scraping** - Automated scraping of the Vendée Globe website to collect all .xlsx files from race updates
2. **Data Parsing** - Processing 697 .xlsx files into a unified .csv using pandas with extensive transformations:
   - Standardizing column names
   - Converting decimal degree minute format to pure decimal format
   - Removing units (kt, nm, degree symbols)
   - Type casting (string to float conversions)
3. **Data Extraction** - Creating a separate dataframe for Sailor, Longitude, Latitude, and Time in France data
4. **Weather Data Integration** - Using Open Meteo API to fetch weather conditions for each data point:
   - Fetching in small chunks to avoid rate limiting
   - Retrieving conditions based on location (Latitude, Longitude), time (Time in France), and competitor (Sailor)
5. **Data Consolidation** - Combining chunks into a single dataset and converting wind speed from km/h to knots
6. **Dataset Merging** - Merging weather data with the original dataset to create a comprehensive 18,000-row dataset
7. **Database Design** - Creating a star schema for Postgres with one fact table and multiple dimension tables
8. **Database Population** - Establishing connection and inserting data into Postgres
9. **Visualization Setup** - Configuring Metabase for data visualization:
   - Creating admin account
   - Connecting to Postgres database
   - Building interactive dashboards
10. **Performance Optimization** - Implementing PySpark for transformations in a separate directory with docker-compose for resource management

## About the Vendée Globe

The Vendée Globe is the most challenging sailing race in the world: solo, non-stop, and without assistance around the globe. The race follows in the footsteps of the 1968 Golden Globe, which pioneered solo circumnavigation via the three great capes (Good Hope, Leeuwin, and Horn).

In 1968, nine pioneers set sail from Falmouth. Only one succeeded. On April 6, 1969, after 313 days at sea, British sailor Robin Knox-Johnston returned victorious.

Twenty years later, Philippe Jeantot, after winning the BOC Challenge twice, introduced a revolutionary concept: a race around the world, but completely non-stop. The Vendée Globe was born.

On November 26, 1989, thirteen sailors started the first edition. Only seven returned to Les Sables d'Olonne after more than three months at sea.

## Requirements

### Recommended: Docker Setup
The entire pipeline is containerized. Simply run:
```bash
docker-compose up --build
```

### Alternative: Manual Setup
If running scripts individually (not recommended):
```txt
pandas~=2.3.3
requests~=2.32.5
tqdm~=4.67.1
python-calamine~=0.5.3
sqlalchemy~=2.0.43
typing-extensions~=4.15.0
asn1crypto~=1.5.1
pg8000~=1.31.5
scramp~=1.4.6
```
- Python 3.13

## Database Schema

![Database Schema](https://github.com/user-attachments/assets/3c9d7e67-b891-4f8e-ba75-80a18fbb2705)

The database uses a star schema optimized for analytical queries with a central fact table connected to multiple dimension tables.

## Data Samples

Sample data and the complete dataset are available in the `/data-samples` directory, including:
- Original .xlsx files
- Processed .csv files
- Weather data chunks

## Metabase Visualizations

![Dashboard Overview](https://github.com/user-attachments/assets/c2a16fdc-df82-41b8-8dbf-7de39f5701e6)
![Detailed Metrics](https://github.com/user-attachments/assets/31b311f9-f4e8-44d2-8749-b3ce75182c77)
![Race Analytics](https://github.com/user-attachments/assets/e27c3b85-2c1c-4a58-84ea-9b46d64aa9b0)

## Docker Configuration

### docker-compose.yaml
```yaml
version: "3.8"

services:
  data_creation:
    build:
      context: ../data-creation
      dockerfile: ../docker/Dockerfile-data-creation
    container_name: data_creation_v1
    volumes:
      - ../data-creation/files:/app/files
      - ../data-creation/output:/app/output
      - ../data-creation/wind:/app/wind
    environment:
      - PYTHONUNBUFFERED=1
      - POSTGRES_HOST=postgres
      - POSTGRES_DB=race_data
      - POSTGRES_USER=admin
      - POSTGRES_PASSWORD=admin123
    depends_on:
      - postgres
    restart: "no"

  postgres:
    image: postgres:15-alpine3.21
    container_name: postgres_db
    restart: "no"
    environment:
      POSTGRES_USER: admin
      POSTGRES_PASSWORD: admin123
      POSTGRES_DB: race_data
    ports:
      - "5432:5432"
    volumes:
      - postgres_data:/var/lib/postgresql/data

  metabase:
    image: metabase/metabase:v0.56.x
    container_name: metabase
    ports:
      - "3000:3000"
    environment:
      MB_DB_TYPE: postgres
      MB_DB_DBNAME: race_data
      MB_DB_PORT: 5432
      MB_DB_USER: admin
      MB_DB_PASS: admin123
      MB_DB_HOST: postgres
    depends_on:
      - postgres
    restart: "no"

volumes:
  postgres_data:
```

### Dockerfile-data-creation
```Dockerfile
FROM python:3.13-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY *.py .

CMD ["python", "main.py"]
```

## Project Structure

```bash
.
├── README.md
├── data-creation
│   ├── chunks
│   │   └── first_part_with_wind_chunk_0_500.csv
│   ├── combiner.py
│   ├── files
│   │   ├── leaderboard_20241110_220000.xlsx
│   │   └── total.csv  # 697 files when fully downloaded
│   ├── main.py
│   ├── output
│   │   ├── dataset.csv
│   │   ├── fetch_parameters_first9999.csv
│   │   ├── fetch_parameters_rest.csv
│   │   └── total.csv
│   ├── parser.py
│   ├── requirements.txt
│   ├── saver.py
│   ├── scraper.py
│   └── wind
│       └── wind_data.csv
├── data_presentation
├── data_samples
│   ├── dataset.csv
│   ├── finishers_format_leaderboard_20250214_060000.xlsx
│   ├── first_part_with_wind_chunk_0_500.csv
│   ├── start_format_leaderboard_20241110_220000.xlsx
│   ├── vendee_data.csv
│   └── wind_data.csv
├── docker
│   ├── Dockerfile-data-creation
│   ├── Dockerfile-data-presentation-backend
│   ├── Dockerfile-data-presentation-frontend
│   └── docker-compose.yaml
├── requirements.txt
└── spark
    ├── docker-compose.yaml  # Separated for resource management
    └── sparky.py  # PySpark transformations
```

## Discoveries

Small amounts of knowledge learned and written in the form of a small diary or blog. At some moments I am frustrated and at others I'm happy, such is life I guess.

### 30.09.2025@15:32 
The French and English versions of the Vendee Globe website use different time zones.
Classement mis à jour à 03h00, 07h00, 11h00, 15h00, 19h00 et 23h00. FRA
Ranking updated at 02h00, 06h00, 10h00, 14h00, 18h00 et 22h00. (UTC time) ENG

### 30.09.2025@15:53
The tracking data from the first and first half of the second day is not available.
10.11.2024 and 11.11.2024

### 30.09.2025@15:59
On my MacBook Air M2 with 16GB RAM it took 3:30 minutes to download all the available leaderboard data using the threadpool in the code.
56.673.095 bytes (57 MB on disk) for 697 items

### 30.09.2025@16:09
The VendeeGlobe website for the tracker is using weather forecasting for the "current" time.

### 30.09.2025@16:33
The .xlsx file has some very specific contents which need to be carefully parsed.

### 30.09.2025@20:14
To open the .xlsx file, I needed to use the python-calamine engine.

### 02.10.2025@17:15
Studying Apache Spark, starting to like it. Plan is to use it for reading and organizing the 697 .xlsx files.

### 04.10.2025@01:22
When parsing the excel with pandas dataframes, I came to the realization that Jean Le Cam is the exception with his name. Also found out this symbol exists: "°", used to represent degrees, but not needed as I'm turning strings for coordinates into floats.

### 04.10.2025@01:35
Thinking of writing a small dictionary or guide book with all the domain knowledge so other non-sailors can hopefully understand the ideas and the concepts.

### 04.10.2025@02:01
By carefully parsing from .xlsx to csv, and getting rid of the excel junk, I decreased the size of a file from 82KB to 7KB. That's more than 10X decrease in size just for the intermediary dataframe. Will write data comparison once saved to csv and later on to Postgres.

### 04.10.2025@03:12
Just found out that the format of the xlsx files completely changes once a boat finishes the race, as it completely destroys the pattern by introducing another header inside the file for the boats still racing and those that finished.

### 04.10.2025@03:26
Race retirements are also messing up the parser.

### 04.10.2025@03:31
Why use FIN or finished when you can use ARV for arrived, :(

### 04.10.2025@17:16
Open Meteo API has a limit of:
600 calls / minute
5,000 calls / hour

### 04.10.2025@18:15
The weather data fetching is going to last close to 4 hours.

### 04.10.2025@22:10
Turns out that they don't even know their API limits, so it's pretty much just running the combiner fetcher script, waiting for the response to go bad, and then doing the same thing again until all 18k rows are downloaded as chunks.

### 04.10.2025@22:10
2025-10-04 22:11:58,682 - INFO - Processing 9999 rows in chunks of 500...
2025-10-04 22:11:58,682 - INFO - Last completed chunk ends at row 2500. Resuming from next chunk...
This is saving me.

### 04.10.2025@22:45
Open Meteo API is returning wind speed in km/h so they need to be turned into knots (1 knot in speed is 1 nautical mile per hour).

### 05.10.2025@00:03
If WiFi does not work, use Mobile Data.

### 05.10.2025@17:44
1 Nautical Mile is 1852 Meters or 1.852 Kilometers.

### 05.10.2025@21:02
Decided to create a star schema for the data inserted into Postgres.

### 05.10.2025@21:25
Do not vibe code database insertions.

## Key Technical Notes

- **Time Zones**: French site uses local time, English site uses UTC
- **Data Gaps**: November 10-11, 2024 tracking data unavailable
- **File Processing**: Python-calamine required for .xlsx parsing
- **Data Size**: 10x reduction achieved through careful parsing (82KB → 7KB)
- **Weather API**: Chunked fetching required due to rate limits
- **Wind Speed**: Converted from km/h to knots (nautical standard)
- **Naming Conventions**: Jean Le Cam requires special handling; boats use "ARV" for finished status
- **Database**: Star schema optimized for analytical queries

## Acknowledgments

- [Geotribu Blog - Track the Vendée Globe with Python and QGIS](https://blog.geotribu.net/2024/12/02/track-the-vendée-globe-race-with-python-and-qgis/?utm_source=chatgpt.com#steps-to-follow)
- Functor Data Engineering Course
- [Open Meteo API](https://open-meteo.com) - Free (limited) weather forecast and historical data by coordinates
- FossFLOW for diagram
