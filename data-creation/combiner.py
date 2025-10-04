#The purpose of this file is to combine the data from each boat at one moment in time with
#the weather forecast at that position

import logging
import os
import requests
import pandas as pd
from datetime import datetime, timedelta
from tqdm import tqdm
import time
import glob
import re

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "output")
CHUNKS_DIR = os.path.join(os.path.dirname(__file__), "chunks")
os.makedirs(OUTPUT_DIR, exist_ok=True)


#this function is only written for presentation sake, the true combining takes place
#when we load another csv that we will merge with the total.csv
#This file is only used when preparing the dataset for merging
#this is not used when running normally or in docker
def create_fetch_parameters_csv():
    df = pd.read_csv(OUTPUT_DIR + "/total.csv")
    df_fetch_parameters = df[["Time in France", "Latitude", "Longitude","Sailor"]]
    df_first9999 = df_fetch_parameters.iloc[:9999]
    df_rest = df_fetch_parameters.iloc[9999:]

    df_first9999.to_csv(os.path.join(OUTPUT_DIR, "fetch_parameters_first9999.csv"), index=False)
    df_rest.to_csv(os.path.join(OUTPUT_DIR, "fetch_parameters_rest.csv"), index=False)


def call_for_data(which_part: int, chunk_size: int = 500):
    if which_part == 0:
        input_file = os.path.join(OUTPUT_DIR, "fetch_parameters_first9999.csv")
        output_file_base = "first_part_with_wind"
    else:
        input_file = os.path.join(OUTPUT_DIR, "fetch_parameters_rest.csv")
        output_file_base = "second_part_with_wind"

    df = pd.read_csv(input_file)
    total_rows = len(df)
    logger.info(f"Processing {total_rows} rows in chunks of {chunk_size}...")

    # Find last completed chunk
    os.makedirs(CHUNKS_DIR, exist_ok=True)
    chunk_files = glob.glob(os.path.join(CHUNKS_DIR, f"{output_file_base}_chunk_*.csv"))

    last_completed_end = -1
    pattern = re.compile(r"_chunk_(\d+)_(\d+)\.csv$")
    for f in chunk_files:
        m = pattern.search(f)
        if m:
            start_idx, end_idx = int(m.group(1)), int(m.group(2))
            last_completed_end = max(last_completed_end, end_idx)

    logger.info(f"Last completed chunk ends at row {last_completed_end}. Resuming from next chunk...")

    # Process in chunks
    for start in range(last_completed_end, total_rows, chunk_size):
        end = min(start + chunk_size, total_rows)
        df_chunk = df.iloc[start:end].copy()

        wind_speeds = []
        wind_dirs = []
        wind_gusts = []

        for _, row in tqdm(df_chunk.iterrows(), total=len(df_chunk), desc=f"Rows {start}-{end}"):
            ws, wd, wg = fetch_weather_for_row(row)
            wind_speeds.append(ws)
            wind_dirs.append(wd)
            wind_gusts.append(wg)

        df_chunk["Wind Speed"] = wind_speeds
        df_chunk["Wind Direction"] = wind_dirs
        df_chunk["Wind Gust"] = wind_gusts

        chunk_file = os.path.join(CHUNKS_DIR, f"{output_file_base}_chunk_{start}_{end}.csv")
        df_chunk.to_csv(chunk_file, index=False)
        logger.info(f"Saved chunk {start}-{end} to {chunk_file}")



def fetch_weather_for_row(row, max_retries=3):
    lat = row['Latitude']
    lon = row['Longitude']
    row_time = datetime.fromisoformat(row['Time in France'])
    date_str = row_time.strftime("%Y-%m-%d")

    url = (
        "https://archive-api.open-meteo.com/v1/archive?"
        f"latitude={lat}&longitude={lon}&start_date={date_str}&end_date={date_str}"
        "&hourly=wind_speed_10m,wind_direction_10m,wind_gusts_10m&timezone=Europe/Paris"
    )

    for attempt in range(max_retries):
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()  # Raise error if status != 200
            data = response.json()
            hourly = data.get("hourly", {})
            times = [datetime.fromisoformat(t) for t in hourly.get("time", [])]
            if not times:
                return None, None, None
            nearest_idx = min(range(len(times)), key=lambda i: abs(times[i] - row_time))
            wind_speed = hourly["wind_speed_10m"][nearest_idx]
            wind_dir = hourly["wind_direction_10m"][nearest_idx]
            wind_gust = hourly["wind_gusts_10m"][nearest_idx]
            return wind_speed, wind_dir, wind_gust

        except (requests.RequestException, ValueError) as e:
            print(f"Attempt {attempt+1} failed for {lat},{lon} at {row_time}: {e}")
            time.sleep(2)  # wait a bit before retrying

    # If all retries fail
    return None, None, None





def add_weather():
    if not os.path.exists(OUTPUT_DIR + "/total.csv" ):
        logger.info(f"Origin file does not exist {OUTPUT_DIR}. Skipping processing.")
        logger.info("Generate or drop in the file in order to use this step")
        return
    pass


def main():
    call_for_data(which_part=0)

if __name__ == "__main__":
    main()





