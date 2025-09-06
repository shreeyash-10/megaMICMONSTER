#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Headless, parallel TTS downloader with logging.

- Runs 5 headless Chrome instances in parallel (one per Excel file).
- Reads input text from Excel, downloads audio, renames file, writes filename to Excel.
- Logs all steps and errors to a log file (tts_headless_multi.log).
"""

import os
import time
import glob
import shutil
import logging
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

try:
    from webdriver_manager.chrome import ChromeDriverManager
    USE_WDM = True
except Exception:
    USE_WDM = False

# -----------------------------
# CONFIGURATION
# -----------------------------

TTS_URL = "https://micmonster.com/text-to-speech/telugu-india/"

EXCEL_FILES = [
    r"C:\path\to\file1.xlsx",
    r"C:\path\to\file2.xlsx",
    r"C:\path\to\file3.xlsx",
    r"C:\path\to\file4.xlsx",
    r"C:\path\to\file5.xlsx",
]
BASE_DIR = Path(__file__).parent
EXCEL_FILES = [str(BASE_DIR / f"file{i}.xlsx") for i in range(1,6)]

INPUT_COL_INDEX = 0
BASE_DOWNLOAD_DIR = str(Path.home() / "Downloads" / "tts_downloads")
FILENAME_PATTERN = "{sheet}_row{row:04d}.mp3"
OUTPUT_SUFFIX = "_with_filenames.xlsx"

PAGE_LOAD_TIMEOUT = 30
CONVERT_TIMEOUT = 90
DOWNLOAD_APPEAR_TIMEOUT = 120
POST_CLICK_WAIT = 1.5

SELECTORS = {
    "textarea": "textarea",
    "convert_button_css": "button[type='submit'], button.convert, #convertButton",
    "convert_button_xpath_fallback": "//button[contains(., 'Convert') or contains(., 'Generate')]",
    "download_button_css": "a[download], a[href*='.mp3'], button.download, .download a",
    "download_button_xpath_fallback": "//a[contains(@href,'.mp3')]",
}

# -----------------------------
# Logging setup
# -----------------------------

LOG_FILE = str(Path(__file__).with_suffix(".log"))
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(threadName)s] %(message)s",
)
logger = logging.getLogger(__name__)

# -----------------------------
# Utilities
# -----------------------------

def setup_driver(download_dir: str) -> webdriver.Chrome:
    os.makedirs(download_dir, exist_ok=True)
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--window-size=1280,2000")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--lang=en-US")
    opts.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36")
    prefs = {
        "download.default_directory": download_dir,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
        "safebrowsing.disable_download_protection": True,
    }
    opts.add_experimental_option("prefs", prefs)
    if USE_WDM:
        service = Service(ChromeDriverManager().install())
    else:
        service = Service()
    driver = webdriver.Chrome(service=service, options=opts)
    driver.execute_cdp_cmd(
        "Page.setDownloadBehavior",
        {"behavior": "allow", "downloadPath": download_dir},
    )
    return driver


def wait_for_file(download_dir: str, timeout: int = DOWNLOAD_APPEAR_TIMEOUT) -> str:
    start = time.time()
    observed = set(glob.glob(os.path.join(download_dir, "*")))
    while time.time() - start < timeout:
        time.sleep(1)
        current = set(glob.glob(os.path.join(download_dir, "*")))
        new_files = [f for f in current - observed if not f.endswith(".crdownload")]
        if new_files:
            return max(new_files, key=os.path.getmtime)
    raise TimeoutError("Download file did not appear in time")


def click_with_fallback(driver, css_selector: str, xpath_fallback: str, timeout: int = 30):
    wait = WebDriverWait(driver, timeout)
    try:
        el = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, css_selector)))
        try:
            el.click()
        except Exception:
            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", el)
            time.sleep(0.2)
            try:
                el.click()
            except Exception:
                driver.execute_script("arguments[0].click();", el)
        return
    except Exception:
        pass
    el = wait.until(EC.element_to_be_clickable((By.XPATH, xpath_fallback)))
    try:
        el.click()
    except Exception:
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", el)
        time.sleep(0.2)
        try:
            el.click()
        except Exception:
            driver.execute_script("arguments[0].click();", el)


def click_by_text(driver, text: str, timeout: int = 30):
    # Case-insensitive contains on common clickable elements
    lowered = text.lower()
    xpath = (
        "//*[self::button or self::a or self::label or self::div or self::span or self::li]"
        "[contains(translate(normalize-space(.), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), '" + lowered + "')]"
    )
    wait = WebDriverWait(driver, timeout)
    el = wait.until(EC.element_to_be_clickable((By.XPATH, xpath)))
    try:
        el.click()
    except Exception:
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", el)
        time.sleep(0.2)
        try:
            el.click()
        except Exception:
            driver.execute_script("arguments[0].click();", el)


def process_excel(excel_path: str, worker_id: int):
    worker_dir = os.path.join(BASE_DOWNLOAD_DIR, f"worker_{worker_id}")
    os.makedirs(worker_dir, exist_ok=True)
    logger.info(f"Worker {worker_id}: Starting on {excel_path}")

    df = pd.read_excel(excel_path, engine="openpyxl")
    out_col_index = INPUT_COL_INDEX + 1
    if out_col_index >= len(df.columns):
        df.insert(out_col_index, "output_filename", "")

    driver = setup_driver(worker_dir)
    wait = WebDriverWait(driver, PAGE_LOAD_TIMEOUT)

    try:
        driver.get(TTS_URL)
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, SELECTORS["textarea"])))

        for row_idx in range(len(df)):
            text = df.iloc[row_idx, INPUT_COL_INDEX]
            if pd.isna(text) or str(text).strip() == "":
                continue

            try:
                logger.info(f"Worker {worker_id}: Processing row {row_idx+1}")
                driver.get(TTS_URL)
                wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, SELECTORS["textarea"])))

                textarea = driver.find_element(By.CSS_SELECTOR, SELECTORS["textarea"])
                textarea.clear()
                textarea.send_keys(str(text))
                time.sleep(POST_CLICK_WAIT)

                # Select desired options: click "mohan" then "shruti"
                try:
                    click_by_text(driver, "mohan", timeout=PAGE_LOAD_TIMEOUT)
                    time.sleep(0.2)
                    click_by_text(driver, "shruti", timeout=PAGE_LOAD_TIMEOUT)
                except Exception as e:
                    logger.warning(f"Worker {worker_id}: Could not click requested options (mohan/shruti): {e}")

                click_with_fallback(
                    driver,
                    SELECTORS["convert_button_css"],
                    SELECTORS["convert_button_xpath_fallback"],
                    timeout=PAGE_LOAD_TIMEOUT
                )

                try:
                    el = WebDriverWait(driver, CONVERT_TIMEOUT).until(
                        EC.element_to_be_clickable((By.CSS_SELECTOR, SELECTORS["download_button_css"]))
                    )
                    el.click()
                except Exception:
                    el = WebDriverWait(driver, CONVERT_TIMEOUT).until(
                        EC.element_to_be_clickable((By.XPATH, SELECTORS["download_button_xpath_fallback"]))
                    )
                    el.click()

                downloaded = wait_for_file(worker_dir, timeout=DOWNLOAD_APPEAR_TIMEOUT)
                target_name = FILENAME_PATTERN.format(sheet=Path(excel_path).stem, row=row_idx + 1)
                target_path = os.path.join(worker_dir, target_name)
                ext = os.path.splitext(downloaded)[1].lower() or ".mp3"
                if ext != os.path.splitext(target_path)[1].lower():
                    target_path = os.path.splitext(target_path)[0] + ext

                i = 1
                base_no_ext, ext_only = os.path.splitext(target_path)
                while os.path.exists(target_path):
                    target_path = f"{base_no_ext} ({i}){ext_only}"
                    i += 1

                shutil.move(downloaded, target_path)
                df.iloc[row_idx, out_col_index] = target_path
                logger.info(f"Worker {worker_id}: Saved {target_path}")

            except Exception as e:
                logger.error(f"Worker {worker_id}: Error on row {row_idx+1} - {e}")

    finally:
        driver.quit()

    out_excel = str(Path(excel_path).with_name(Path(excel_path).stem + OUTPUT_SUFFIX))
    df.to_excel(out_excel, index=False, engine="openpyxl")
    logger.info(f"Worker {worker_id}: Finished {excel_path}, saved {out_excel}")
    return out_excel, worker_dir


def main():
    if len(EXCEL_FILES) != 5:
        raise ValueError("Please provide EXACTLY 5 Excel file paths in EXCEL_FILES.")
    Path(BASE_DOWNLOAD_DIR).mkdir(parents=True, exist_ok=True)
    # Ensure input Excel files exist locally; create empty templates with a 'text' column if missing
    for p in EXCEL_FILES:
        pth = Path(p)
        if not pth.exists():
            try:
                pd.DataFrame(columns=["text"]).to_excel(pth, index=False, engine="openpyxl")
                logger.info(f"Created template Excel: {pth}")
            except Exception as e:
                logger.error(f"Failed to create template Excel {pth}: {e}")
    results = []
    with ThreadPoolExecutor(max_workers=5) as exe:
        fut_to_excel = {exe.submit(process_excel, path, i+1): path for i, path in enumerate(EXCEL_FILES)}
        for fut in as_completed(fut_to_excel):
            src = fut_to_excel[fut]
            try:
                out_excel, out_dir = fut.result()
                results.append((src, out_excel, out_dir, "OK"))
            except Exception as e:
                logger.error(f"Error in worker for {src}: {e}")
                results.append((src, "", "", f"ERROR: {e!r}"))
    print("\n=== SUMMARY ===")
    for src, out_excel, out_dir, status in results:
        print(f"Source: {src}\n -> Status: {status}\n -> Updated Excel: {out_excel}\n -> Downloads: {out_dir}\n")
    print(f"Logs written to {LOG_FILE}")


if __name__ == "__main__":
    main()
