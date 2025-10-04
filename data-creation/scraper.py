import os
import requests
import logging
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

start_date = "20241110_100000"
end_date = "20250308_070000"
leaderboard_link = "https://www.vendeeglobe.org/sites/default/files/ranking/vendeeglobe_leaderboard_YYYYMMDD_HHMMSS.xlsx"
FILES_DIR = os.path.join(os.path.dirname(__file__), "files")
os.makedirs(FILES_DIR, exist_ok=True)


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
    ]
)
logger = logging.getLogger(__name__)

def generate_timestamps(start_date: str, end_date: str, step_hours: int = 4):
    start = datetime.strptime(start_date, "%Y%m%d_%H%M%S")
    end = datetime.strptime(end_date, "%Y%m%d_%H%M%S")
    current = start
    while current <= end:
        yield current.strftime("%Y%m%d_%H%M%S")
        current += timedelta(hours=step_hours)

def download_file(ts: str):
    save_path = os.path.join(FILES_DIR, f"leaderboard_{ts}.xlsx")
    if os.path.exists(save_path):
        #Yup its a double check
        return f"⏩ {ts} skipped (already exists)"

    url = leaderboard_link.replace("YYYYMMDD_HHMMSS", ts)
    try:
        r = requests.get(url, timeout=10)
        if r.status_code == 200 and len(r.content) > 1000 and r.content[:2] == b"PK":
            with open(save_path, "wb") as f:
                f.write(r.content)
            return f"✅ {ts} saved ({len(r.content)} bytes)"
        else:
            return f"❌ {ts} missing or invalid content"
    except Exception as e:
        return f"⚠️ {ts} error: {e}"

def download(max_workers=10):
    existing_files = set(os.listdir(FILES_DIR))
    timestamps_to_download = [
        ts for ts in generate_timestamps(start_date, end_date)
        if f"leaderboard_{ts}.xlsx" not in existing_files
    ]

    if not timestamps_to_download:
        logger.info("✔ All files already exist, nothing to download.")
        return

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(download_file, ts): ts for ts in timestamps_to_download}

        for future in tqdm(as_completed(futures), total=len(futures), desc="Downloading"):
            result = future.result()
            logger.info(result)
    logger.info("✔ Finished downloading all files.")