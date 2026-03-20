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

    # IMPORTANT: On ARM64 VMs (e.g. Oracle Cloud), webdriver_manager downloads
    # an x86_64 binary that fails with "Exec format error". Always prefer a
    # native system/snap chromedriver that matches the machine architecture.
    driver_candidates = [
        "/snap/bin/chromium.chromedriver",           # Snap Chromium (ARM64 + x86)
        shutil.which("chromedriver"),                # System PATH
        "/usr/bin/chromedriver",
        "/usr/lib/chromium-browser/chromedriver",
        "/usr/lib/chromium/chromedriver",
    ]
    chromedriver_path = _find_binary(driver_candidates)

    if chromedriver_path:
        print(f"   🔧 Using native ChromeDriver: {chromedriver_path}")
        service = Service(executable_path=chromedriver_path)
    else:
        # Only fall back to webdriver_manager if no native driver found.
        # This may fail on ARM64 if the downloaded binary is wrong architecture.
        print("   ⚠️ No native chromedriver found, trying webdriver_manager...")
        try:
            chrome_type = ChromeType.CHROMIUM if (chrome_binary and "chrom" in chrome_binary.lower()) else ChromeType.GOOGLE
            driver_path = ChromeDriverManager(chrome_type=chrome_type).install()
            service = Service(executable_path=driver_path)
        except Exception as e:
            raise RuntimeError(
                f"chromedriver not found. On Ubuntu/ARM run:\n"
                f"  sudo snap install chromium  (includes chromedriver)\n"
                f"  -- or --\n"
                f"  sudo apt install chromium-browser chromium-chromedriver\n"
                f"Manager error: {e}"
            )

    return webdriver.Chrome(service=service, options=options)
