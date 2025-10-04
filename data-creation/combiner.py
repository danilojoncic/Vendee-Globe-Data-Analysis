#The purpose of this file is to combine the data from each boat at one moment in time with
#the weather forecast at that position

import logging
import os
import requests
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "output")
os.makedirs(OUTPUT_DIR, exist_ok=True)



def call_weather_data_for_one_file(filename:str) -> pd.DataFrame:
    df = pd.DataFrame

    pass


def add_weather():
    if not os.path.exists(OUTPUT_DIR + "/total.csv" ):
        logger.info(f"Origin file does not exist {OUTPUT_DIR}. Skipping processing.")
        logger.info("Generate or drop in the file in order to use this step")
        return
    pass


def main():
    call_weather_data_for_one_file("weather.csv")


if __name__ == "__main__":
    main()





