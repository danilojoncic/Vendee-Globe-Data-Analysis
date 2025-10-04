import os
import pandas as pd
from numpy import integer

FILES_DIR = "files"


#going from degrees and decimal minutes
#to just decimal or float format for later easier geo stuff
def parse_coordinates(cords:str)->float:
    degs,rest = cords.split("°")
    minutes = rest[:-2] #last two
    direction = rest[-1] #N S E W
    decimal_degrees = float(degs) + float(minutes)/60
    if direction in ["S","W"]:
        decimal_degrees = -decimal_degrees
    return decimal_degrees


#Same thing like for cordinates, but now we want to get rid of the degrees
def parse_heading(heading:str)->int:
     return int(heading[:-1].strip())


#Here we need to get rid of out beloved knots (kts) speed measure
def parse_speed(speed:str)->float:
    return float(speed[:-3].strip())


#Here we need to get rid of our beloved nautical miles (nm)
def parse_distance(distance:str)->float:
    return float(distance[:-2].strip())




def parse_file(filename: str):

    #Date parts
    basename = os.path.basename(filename)
    date_str = basename.split("_")[1]
    date_part = pd.to_datetime(date_str, format="%Y%m%d").date()

    #Special reading using calamine as engine
    df = pd.read_excel(filename, engine="calamine", skiprows=3)


    df = df.iloc[:, 1:]
    df = df.iloc[:-4, :]
    df = df.drop(0, errors="ignore")

    #Clealing column strings of uselass tabs and spaces
    df.columns = df.columns.str.replace(r'[\r\n]+', ' ', regex=True).str.strip()
    #Cleaning row strings of uselass tabs and spaces
    df = df.map(lambda x: str(x).replace("\r\n", " ").strip() if isinstance(x, str) else x)

    #Renaming
    df.rename(columns={
                    df.columns.values[0] : "Ranking",
                    df.columns.values[1] : "Sailor Nationality and Sail Number",
                    df.columns.values[2] : "Sailor Name and Team Name",
                    df.columns.values[3] : "Time in France",
                    df.columns.values[4] : "Latitude",
                    df.columns.values[5] : "Longitude",

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


    #Applying
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

    #Types
    df["Ranking"] = df["Ranking"].astype(int)
    df[["Sailor", "Team", "Nation", "Sail"]] = df[["Sailor", "Team", "Nation", "Sail"]].astype("string")

    #Replacing, respect for the legend
    df["Sailor"] = df["Sailor"].replace("Jean Le", "Jean Le Cam")

    #Dropping
    df.drop(columns=["Sailor Nationality and Sail Number","Sailor Name and Team Name"],inplace=True)


    #Reodrering
    cols_in_prefered_order = [
        "Ranking","Sailor","Nation","Team","Sail","Latitude","Longitude","Time in France",
        "Heading 30min","Heading Last Report","Heading 24h",
        "Speed 30min","Speed Last Report","Speed 24h",
        "VMG 30min","VMG Last Report","VMG 24h",
        "Dist 30min","Dist Last Report","Dist 24h",
        "DTF","DTL"
    ]

    df = df.reindex(columns=cols_in_prefered_order)

    with pd.option_context('display.max_rows', None, 'display.max_columns', None, 'display.width', 2000):
        print(df)

    print(df.info())

    return df


def parse_directory():
    files = [f for f in os.listdir(FILES_DIR) if os.path.isfile(os.path.join(FILES_DIR, f))]
    if len(files) == 697:
        print(f"{Fore.CYAN}✔ Folder already has 697 files, skipping download.{Style.RESET_ALL}")
        return



def main():
    parse_file("./files/leaderboard_20241110_220000.xlsx")


if __name__ == "__main__":
    main()
