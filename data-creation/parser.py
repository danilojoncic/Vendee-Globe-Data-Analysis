import os
import pandas as pd
import logging
from tqdm import tqdm

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

FILES_DIR = os.path.join(os.path.dirname(__file__), "files")
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "output")
os.makedirs(OUTPUT_DIR, exist_ok=True)


# going from degrees and decimal minutes
# to just decimal or float format for later easier geo stuff
def parse_coordinates(cords: str) -> float:
    try:
        degs, rest = cords.split("°")
        minutes = rest[:-2]  # last two
        direction = rest[-1]  # N S E W
        decimal_degrees = float(degs) + float(minutes) / 60
        if direction in ["S", "W"]:
            decimal_degrees = -decimal_degrees
        return decimal_degrees
    except Exception as e:
        logger.error(f"Error parsing coordinates '{cords}': {e}")
        raise


# Same thing like for coordinates, but now we want to get rid of the degrees
def parse_heading(heading: str) -> int:
    try:
        return int(heading[:-1].strip())
    except Exception as e:
        logger.error(f"Error parsing heading '{heading}': {e}")
        raise


# Here we need to get rid of our beloved knots (kts) speed measure
def parse_speed(speed: str) -> float:
    try:
        return float(speed[:-3].strip())
    except Exception as e:
        logger.error(f"Error parsing speed '{speed}': {e}")
        raise


# Here we need to get rid of our beloved nautical miles (nm)
def parse_distance(distance: str) -> float:
    try:
        return float(distance[:-2].strip())
    except Exception as e:
        logger.error(f"Error parsing distance '{distance}': {e}")
        raise




def parse_file(filename: str) -> pd.DataFrame:
    logger.info(f"Parsing file: {filename}")

    try:
        # Date parts
        basename = os.path.basename(filename)
        date_str = basename.split("_")[1]
        date_part = pd.to_datetime(date_str, format="%Y%m%d").date()
        logger.debug(f"Extracted date: {date_part}")

        df_check = pd.read_excel(filename, engine="calamine", skiprows=3, nrows=1)

        # Check if ARV format exists
        if any('arrivée' in str(col).lower() or 'arrival date' in str(col).lower() for col in df_check.columns):
            logger.info("Detected ARV format, searching for racing table...")

            # Read full file to find racing header
            df_full = pd.read_excel(filename, engine="calamine", header=None)

            beginning_of_racing_rows = 3  # default
            for idx in range(len(df_full)):
                row_values = df_full.iloc[idx].astype(str).str.cat(sep=' ')
                if 'Depuis 30 minutes' in row_values or 'Since 30 minutes' in row_values:
                    beginning_of_racing_rows = idx
                    logger.info(f"Found racing data starting at row {idx}")
                    break
        else:
            beginning_of_racing_rows = 3




        df = pd.read_excel(filename, engine="calamine", skiprows=beginning_of_racing_rows)

        df = df.iloc[:, 1:]
        df = df.iloc[:-4, :]
        df = df.drop(0, errors="ignore")

        # Cleaning column strings of useless tabs and spaces
        df.columns = df.columns.str.replace(r'[\r\n]+', ' ', regex=True).str.strip()
        # Cleaning row strings of useless tabs and spaces
        df = df.map(lambda x: str(x).replace("\r\n", " ").strip() if isinstance(x, str) else x)

        #Get rid of RET,DNF or ARV Sailors
        first_col = df.columns[0]
        df = df[~df[first_col].astype(str).str.strip().isin(['RET', 'DNF', 'ARV'])]
        df = df.reset_index(drop=True)





        # Renaming
        df.rename(columns={
            df.columns.values[0]: "Ranking",
            df.columns.values[1]: "Sailor Nationality and Sail Number",
            df.columns.values[2]: "Sailor Name and Team Name",
            df.columns.values[3]: "Time in France",
            df.columns.values[4]: "Latitude",
            df.columns.values[5]: "Longitude",

            "Depuis 30 minutes Since 30 minutes": "Heading 30min",
            "Unnamed: 8": "Speed 30min",
            "Unnamed: 9": "VMG 30min",
            "Unnamed: 10": "Dist 30min",

            "Depuis le dernier classement Since the the last report": "Heading Last Report",
            "Unnamed: 12": "Speed Last Report",
            "Unnamed: 13": "VMG Last Report",
            "Unnamed: 14": "Dist Last Report",

            "Depuis 24 heures Since 24 hours": "Heading 24h",
            "Unnamed: 16": "Speed 24h",
            "Unnamed: 17": "VMG 24h",
            "Unnamed: 18": "Dist 24h",
        }, inplace=True)

        # Applying
        df["Latitude"] = df["Latitude"].apply(parse_coordinates)
        df["Longitude"] = df["Longitude"].apply(parse_coordinates)

        df["Sailor"] = df["Sailor Name and Team Name"].apply(lambda x: " ".join(x.split()[:2]))
        df["Team"] = df.apply(lambda row: row["Sailor Name and Team Name"].replace(row["Sailor"], "").strip(), axis=1)
        df["Nation"] = df["Sailor Nationality and Sail Number"].apply(lambda x: " ".join(x.split()[:1]))
        df["Sail"] = df["Sailor Nationality and Sail Number"].apply(lambda x: " ".join(x.split()[-2:]))

        df["Time in France"] = df["Time in France"].apply(lambda t: t.split()[0])
        df["Time in France"] = df["Time in France"].apply(
            lambda t: pd.to_datetime(f"{date_part} {t}", format="%Y-%m-%d %H:%M"))



        df["Heading 30min"] = df["Heading 30min"].apply(parse_heading)
        df["Heading Last Report"] = df["Heading Last Report"].apply(parse_heading)
        df["Heading 24h"] = df["Heading 24h"].apply(parse_heading)

        df["Speed 30min"] = df["Speed 30min"].apply(parse_speed)
        df["VMG 30min"] = df["VMG 30min"].apply(parse_speed)

        df["Speed Last Report"] = df["Speed Last Report"].apply(parse_speed)
        df["VMG Last Report"] = df["VMG Last Report"].apply(parse_speed)

        df["Speed 24h"] = df["Speed 24h"].apply(parse_speed)
        df["VMG 24h"] = df["VMG 24h"].apply(parse_speed)

        df["Dist 30min"] = df["Dist 30min"].apply(parse_distance)
        df["Dist Last Report"] = df["Dist Last Report"].apply(parse_distance)
        df["Dist 24h"] = df["Dist 24h"].apply(parse_distance)
        df["DTL"] = df["DTL"].apply(parse_distance)
        df["DTF"] = df["DTF"].apply(parse_distance)

        # Types
        df["Ranking"] = df["Ranking"].astype(int)
        df[["Sailor", "Team", "Nation", "Sail"]] = df[["Sailor", "Team", "Nation", "Sail"]].astype("string")

        # Replacing, respect for the legend
        df["Sailor"] = df["Sailor"].replace("Jean Le", "Jean Le Cam")

        # Dropping
        df.drop(columns=["Sailor Nationality and Sail Number", "Sailor Name and Team Name"], inplace=True)

        # Reordering
        cols_in_prefered_order = [
            "Ranking", "Sailor", "Nation", "Team", "Sail", "Latitude", "Longitude", "Time in France",
            "Heading 30min", "Heading Last Report", "Heading 24h",
            "Speed 30min", "Speed Last Report", "Speed 24h",
            "VMG 30min", "VMG Last Report", "VMG 24h",
            "Dist 30min", "Dist Last Report", "Dist 24h",
            "DTF", "DTL"
        ]

        df = df.reindex(columns=cols_in_prefered_order)
        logger.info(f"Successfully parsed {len(df)} rows from {filename}")
        return df

    except Exception as e:
        logger.error(f"Failed to parse file {filename}: {e}")
        raise


def parse_directory():
    logger.info(f"Starting directory parsing from: {FILES_DIR}")

    files = [f for f in os.listdir(FILES_DIR) if os.path.isfile(os.path.join(FILES_DIR, f))]

    if len(files) == 0:
        logger.warning("No files found in directory")
        return

    if os.path.exists(OUTPUT_DIR + "/total.csv" ):
        logger.info(f"Output file already exists at {OUTPUT_DIR}. Skipping processing.")
        logger.info("Delete the file if you want to regenerate it.")
        return


    logger.info(f"Found {len(files)} files to process")

    df_total = pd.DataFrame()

    for file in tqdm(files, desc="Processing files", unit="file"):
        try:
            file_path = os.path.join(FILES_DIR, file)
            df_temp = parse_file(file_path)
            df_total = pd.concat([df_total, df_temp], ignore_index=True)
        except Exception as e:
            logger.error(f"Skipping file {file} due to error: {e}")
            continue

    output_path = os.path.join(OUTPUT_DIR, "total.csv")
    df_total.to_csv(output_path, index=False)
    logger.info(f"Successfully saved combined data to {output_path}")
    logger.info(f"Total rows in combined dataset: {len(df_total)}")



