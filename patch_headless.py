"""
Patch script v3: Explicitly sets chromium binary + chromedriver Service path for ARM Ubuntu.
Re-run this after any previous patch runs.
"""

import os
import re
import ast

TARGET_DIRS = [
    "/Users/mac/Desktop/Programozás/Magyar-Manual-main/Magyar/modules",
    "/Users/mac/Desktop/Programozás/Magyar-Manual-main/Manual/modules",
]

OLD_IMPORT = "import undetected_chromedriver as uc"

NEW_IMPORTS = """from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service"""

# ARM-native paths installed via: sudo apt install chromium-browser chromium-chromedriver
CHROME_BINARY = "/usr/bin/chromium-browser"
CHROMEDRIVER_PATH = "/usr/bin/chromedriver"


def patch_file(filepath):
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()

    # Only process selenium-based scrapers
    if 'webdriver' not in content and 'undetected_chromedriver' not in content:
        return False

    original = content

    # 1. Swap uc import for standard selenium
    content = content.replace(OLD_IMPORT, NEW_IMPORTS)
    content = content.replace("uc.ChromeOptions()", "Options()")
    content = re.sub(r'uc\.Chrome\(', 'webdriver.Chrome(', content)
    content = re.sub(r',?\s*version_main\s*=\s*\w+', '', content)

    # 2. Strip ALL existing headless/sandbox/binary/service lines to start clean
    strip_patterns = [
        r'[ \t]*options\.add_argument\(["\']--headless(?:=new)?["\']\)[ \t]*\n',
        r'[ \t]*#\s*options\.add_argument\(["\']--headless.*?\n',
        r'[ \t]*options\.add_argument\(["\']--no-sandbox["\']\)[ \t]*\n',
        r'[ \t]*options\.add_argument\(["\']--disable-dev-shm-usage["\']\)[ \t]*\n',
        r'[ \t]*options\.add_argument\(["\']--disable-gpu["\']\)[ \t]*\n',
        r'[ \t]*options\.binary_location\s*=.*\n',
        r'[ \t]*_service\s*=\s*Service\(.*?\)\n',
    ]
    for pattern in strip_patterns:
        content = re.sub(pattern, '', content)

    # 3. Normalize all Chrome() calls to webdriver.Chrome(options=options)
    content = re.sub(r'webdriver\.Chrome\([^)]*\)', 'webdriver.Chrome(options=options)', content)

    # 4. Remove orphaned try/except blocks (left by previous patches)
    content = re.sub(
        r'[ \t]*except Exception as e:\n[ \t]+print\(f?"[^"]*\{e\}[^"]*"\)\n[ \t]+return\n',
        '',
        content,
        flags=re.IGNORECASE
    )

    # 5. Inject the full ARM-compatible driver block before webdriver.Chrome(options=options)
    def inject(m):
        indent = m.group(1)
        return (
            f'{indent}options.add_argument("--headless=new")\n'
            f'{indent}options.add_argument("--no-sandbox")\n'
            f'{indent}options.add_argument("--disable-dev-shm-usage")\n'
            f'{indent}options.add_argument("--disable-gpu")\n'
            f'{indent}options.binary_location = "{CHROME_BINARY}"\n'
            f'{indent}_service = Service(executable_path="{CHROMEDRIVER_PATH}")\n'
            f'{indent}driver = webdriver.Chrome(service=_service, options=options)'
        )
    content = re.sub(
        r'^([ \t]+)driver\s*=\s*webdriver\.Chrome\(options=options\)',
        inject,
        content,
        flags=re.MULTILINE
    )

    if content != original:
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(content)
        return True
    return False


if __name__ == "__main__":
    patched = 0
    skipped = 0
    errored = []

    for directory in TARGET_DIRS:
        if not os.path.exists(directory):
            print(f"⚠️  Not found: {directory}")
            continue
        for filename in sorted(os.listdir(directory)):
            if not filename.endswith(".py"):
                continue
            filepath = os.path.join(directory, filename)
            try:
                if patch_file(filepath):
                    patched += 1
                    print(f"  ✅ {filename}")
                else:
                    skipped += 1
            except Exception as e:
                errored.append(filename)
                print(f"  ❌ {filename}: {e}")

    print(f"\nPatched: {patched} | Skipped: {skipped} | Errors: {len(errored)}")
    if errored:
        print("Errors:", errored)
