#The purpose of this file is to parse the .xlsx files and convert them into single simple .csv
import os

import pandas as pd


def parse_file(filename:str):
    df = pd.read_excel(filename, engine="calamine",skiprows=3)
    df = df.iloc[:, 1:]

    df = df.iloc[:-4,:]

    df.columns = df.columns.str.replace(r'[\r\n]+', ' ', regex=True).str.strip()
    df = df.map(lambda x: str(x).replace("\r\n", " ").strip() if isinstance(x, str) else x)
    print(df)
    pass




def parse_directory():
    pass



def main():
    parse_file("./files/leaderboard_20241110_220000.xlsx")


if __name__ == '__main__':
    main()