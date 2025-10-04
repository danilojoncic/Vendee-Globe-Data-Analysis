import os
import requests
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from colorama import Fore, Style

start_date = "20241110_130000"
end_date = "20250308_070000"
leaderboard_link = "https://www.vendeeglobe.org/sites/default/files/ranking/vendeeglobe_leaderboard_YYYYMMDD_HHMMSS.xlsx"

FILES_DIR = "files"
os.makedirs(FILES_DIR, exist_ok=True)


def generate_timestamps(start_date: str, end_date: str, step_hours: int = 1):
    start = datetime.strptime(start_date, "%Y%m%d_%H%M%S")
    end = datetime.strptime(end_date, "%Y%m%d_%H%M%S")
    current = start
    while current <= end:
        yield current.strftime("%Y%m%d_%H%M%S")
        current += timedelta(hours=step_hours)


def download_file(ts: str):
    url = leaderboard_link.replace("YYYYMMDD_HHMMSS", ts)
    save_path = os.path.join(FILES_DIR, f"leaderboard_{ts}.xlsx")

    if os.path.exists(save_path):
        return f"{Fore.YELLOW}⏩ {ts} skipped{Style.RESET_ALL}"

    try:
        r = requests.get(url, timeout=10)
        if r.status_code == 200 and len(r.content) > 1000 and r.content[:2] == b"PK":
            with open(save_path, "wb") as f:
                f.write(r.content)
            return f"{Fore.GREEN}✅ {ts} saved ({len(r.content)} bytes){Style.RESET_ALL}"
        else:
            return f"{Fore.RED}❌ {ts} miss{Style.RESET_ALL}"
    except Exception as e:
        return f"{Fore.RED}⚠️ {ts} error: {e}{Style.RESET_ALL}"


def download():
    files = [f for f in os.listdir(FILES_DIR) if os.path.isfile(os.path.join(FILES_DIR, f))]
    if len(files) == 697:
        print(f"{Fore.CYAN}✔ Folder already has 697 files, skipping download.{Style.RESET_ALL}")
        return

    timestamps = list(generate_timestamps(start_date, end_date, step_hours=1))

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(download_file, ts): ts for ts in timestamps}

        for future in tqdm(as_completed(futures), total=len(futures), desc="Downloading"):
            print(future.result())

    print(f"{Fore.YELLOW}FINISHED Downloading{Style.RESET_ALL}")