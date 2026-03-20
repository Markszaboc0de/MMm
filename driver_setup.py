"""
Shared Chrome WebDriver factory.
Dynamically detects the correct chrome binary and chromedriver for the
current operating system/architecture (works on both macOS and ARM Linux).
"""
import os
import shutil
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service


def _find_binary(candidates):
    """Return the first existing file from a list of candidate paths."""
    for path in candidates:
        if path and os.path.isfile(path):
            return path
    return None


def get_chrome_driver() -> webdriver.Chrome:
    """
    Returns a headless, sandboxed Chrome WebDriver instance.
    Automatically detects the correct binary locations.
    """
    options = Options()
    options.add_argument("--blink-settings=imagesEnabled=false")
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")

    chrome_candidates = [
        shutil.which("chromium-browser"),
        shutil.which("chromium"),
        shutil.which("google-chrome"),
        shutil.which("google-chrome-stable"),
        "/usr/bin/chromium-browser",
        "/usr/bin/chromium",
        "/usr/bin/google-chrome",
        "/snap/bin/chromium",
    ]

    driver_candidates = [
        shutil.which("chromedriver"),
        "/usr/bin/chromedriver",
        "/usr/lib/chromium-browser/chromedriver",
        "/usr/lib/chromium/chromedriver",
        "/snap/bin/chromium.chromedriver",
    ]

    chrome_binary = _find_binary(chrome_candidates)
    chromedriver_path = _find_binary(driver_candidates)

    if chrome_binary:
        options.binary_location = chrome_binary

    if not chromedriver_path:
        raise RuntimeError(
            "chromedriver not found. On Ubuntu/ARM run: sudo apt install chromium-browser chromium-chromedriver"
        )

    service = Service(executable_path=chromedriver_path)
    return webdriver.Chrome(service=service, options=options)
