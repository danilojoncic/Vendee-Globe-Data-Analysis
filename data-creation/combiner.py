#The purpose of this file is to combine the data from each boat at one moment in time with
#the weather forecast at that position

import logging
import os
from time import sleep

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
WIND_DIR = os.path.join(os.path.dirname(__file__), "wind")
os.makedirs(WIND_DIR, exist_ok=True)


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


def call_for_data(which_part: int, chunk_size: int = 100):
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
        sleep(60)



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
            response = requests.get(url, timeout=60)
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

    return None, None, None

def kmh_to_knots(kmh: float) -> float:
    knots =  (kmh / 1.852)
    return round(knots,1)



def add_weather():
    if not os.path.exists(OUTPUT_DIR + "/total.csv" ):
        logger.info(f"Origin file does not exist {OUTPUT_DIR}. Skipping processing.")
        logger.info("Generate or drop in the file in order to use this step")
        return
    if not os.path.exists(WIND_DIR + "/wind_data.csv" ):
        logger.info(f"Wind file does not exist {WIND_DIR}. Skipping processing.")
        logger.info("Generate or drop in the file in order to use this step")
        return

    df_vendee = pd.read_csv(OUTPUT_DIR + "/total.csv")
    df_wind_data = pd.read_csv(WIND_DIR + "/wind_data.csv")

    print(df_vendee.info())
    print(df_wind_data.info())

    df_wind_data["Wind Speed"] = df_wind_data["Wind Speed"].apply(kmh_to_knots)
    df_wind_data["Wind Gust"] = df_wind_data["Wind Gust"].apply(kmh_to_knots)

    df_wind_data["Latitude"] = df_wind_data["Latitude"].round(4)
    df_wind_data["Longitude"] = df_wind_data["Longitude"].round(4)
    df_vendee["Latitude"] = df_vendee["Latitude"].round(4)
    df_vendee["Longitude"] = df_vendee["Longitude"].round(4)

    df_wind_data = df_wind_data.dropna()

    merged = pd.merge(
        df_wind_data,
        df_vendee,
        on=["Time in France", "Sailor", "Latitude", "Longitude"],
        how="inner")

    merged.to_csv(OUTPUT_DIR + "/dataset.csv", index=False)

def combine_chunks():
    os.makedirs(WIND_DIR, exist_ok=True)
    output_path = os.path.join(WIND_DIR, "wind_data.csv")

    files = sorted([os.path.join(CHUNKS_DIR, f) for f in os.listdir(CHUNKS_DIR) if f.endswith(".csv")])
    if not files:
        logger.warning("No chunk files found.")
        return

    for i, f in enumerate(tqdm(files, desc="Merging incrementally")):
        df = pd.read_csv(f)
        mode = "w" if i == 0 else "a"
        header = i == 0
        df.to_csv(output_path, mode=mode, header=header, index=False)
        del df

    logger.info(f"✅ Combined CSV saved to {output_path}")






