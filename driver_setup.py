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
from webdriver_manager.chrome import ChromeDriverManager
from webdriver_manager.core.os_manager import ChromeType


def _find_binary(candidates):
    """Return the first existing file from a list of candidate paths."""
    for path in candidates:
        if path and os.path.isfile(path):
            return path
    return None


def get_chrome_driver() -> webdriver.Chrome:
    """
    Returns a headless, sandboxed Chrome WebDriver instance.
    Automatically detects the correct binary locations using webdriver-manager
    and fallback paths for common Linux/VM environments.
    """
    options = Options()
    options.page_load_strategy = 'eager'
    options.add_argument("--blink-settings=imagesEnabled=false")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")

    # Common binary locations for Chromium-based browsers on Linux/Snaps
    chrome_candidates = [
        shutil.which("google-chrome"),
        shutil.which("google-chrome-stable"),
        shutil.which("chromium"),
        shutil.which("chromium-browser"),
        "/usr/bin/google-chrome",
        "/usr/bin/google-chrome-stable",
        "/usr/bin/chromium-browser",
        "/usr/bin/chromium",
        "/snap/bin/chromium",
        "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome", # Mac fallback
    ]
    
    chrome_binary = _find_binary(chrome_candidates)
    if chrome_binary:
        options.binary_location = chrome_binary
        print(f"   🔍 Using Chrome Binary: {chrome_binary}")

    try:
        # Determine if we should use Chromium vs Google Chrome for manager
        chrome_type = ChromeType.CHROMIUM if (chrome_binary and "chrom" in chrome_binary.lower()) else ChromeType.GOOGLE
        driver_path = ChromeDriverManager(chrome_type=chrome_type).install()
        service = Service(executable_path=driver_path)
    except Exception as e:
        print(f"   ⚠️ WebDriverManager failed ({e}), searching for local drivers...")
        driver_candidates = [
            shutil.which("chromedriver"),
            "/usr/bin/chromedriver",
            "/usr/lib/chromium-browser/chromedriver",
            "/snap/bin/chromium.chromedriver",
        ]
        chromedriver_path = _find_binary(driver_candidates)
        if not chromedriver_path:
             raise RuntimeError(f"chromedriver not found and manager failed. Error: {e}")
        service = Service(executable_path=chromedriver_path)

    return webdriver.Chrome(service=service, options=options)
