"""
Patch script: replaces undetected_chromedriver (uc) with standard Selenium Chrome
in all scraper modules so they work correctly on ARM-based servers.

Changes applied per file:
  1. 'import undetected_chromedriver as uc'  ->  standard selenium imports
  2. 'uc.ChromeOptions()'  ->  'Options()'
  3. 'uc.Chrome(options=options)'  ->  'webdriver.Chrome(options=options)'
  4. Ensures --headless=new, --no-sandbox, --disable-dev-shm-usage are present
  5. Removes version_main=... args (not applicable to standard selenium)
  6. Removes any broken try/except blocks left by the previous patch attempt
"""

import os
import re

TARGET_DIRS = [
    "/Users/mac/Desktop/Programozás/Magyar-Manual-main/Magyar/modules",
    "/Users/mac/Desktop/Programozás/Magyar-Manual-main/Manual/modules",
]

OLD_IMPORT = "import undetected_chromedriver as uc"

NEW_IMPORTS = """from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service"""

def patch_file(filepath):
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()

    if 'undetected_chromedriver' not in content:
        return False  # Not a uc-based scraper, skip

    original = content

    # Step 1: Replace the uc import block
    content = content.replace(OLD_IMPORT, NEW_IMPORTS)

    # Step 2: Replace uc.ChromeOptions() -> Options()
    content = content.replace("uc.ChromeOptions()", "Options()")

    # Step 3: Remove version_main arguments
    content = re.sub(r',?\s*version_main\s*=\s*\w+', '', content)

    # Step 4: Replace uc.Chrome(options=options) -> webdriver.Chrome(options=options)
    content = re.sub(r'uc\.Chrome\(', 'webdriver.Chrome(', content)

    # Step 5: Fix broken try/except blocks left by previous patch
    # Pattern: options.add_argument("--headless=new")\n    driver = webdriver.Chrome(
    # was accidentally wrapped in try with wrong context - clean up
    content = re.sub(
        r'try:\s*\n(\s+)options\.add_argument\("--headless=new"\)\s*\n\s*driver\s*=',
        r'\1options.add_argument("--headless=new")\n\1driver =',
        content
    )

    # Step 6: Find the indented block where options are built (look for ChromeOptions context)
    # and ensure we have the three headless-safe flags
    # First strip any existing duplicated headless flags
    content = re.sub(r'\s*options\.add_argument\("--headless=new"\)\s*\n', '\n', content)
    content = re.sub(r'\s*#\s*options\.add_argument\(["\']--headless=new["\']\).*\n', '\n', content)  

    # Step 7: Inject the required headless/sandbox args right before webdriver.Chrome init
    # We look for the driver = webdriver.Chrome( line and insert before it
    def inject_headless(m):
        indent = m.group(1)
        return (
            f'{indent}options.add_argument("--headless=new")\n'
            f'{indent}options.add_argument("--no-sandbox")\n'
            f'{indent}options.add_argument("--disable-dev-shm-usage")\n'
            f'{indent}options.add_argument("--disable-gpu")\n'
            f'{indent}driver = webdriver.Chrome('
        )
    content = re.sub(r'^(\s+)driver\s*=\s*webdriver\.Chrome\(', inject_headless, content, flags=re.MULTILINE)

    # Step 8: Fix any double commas or trailing commas in Chrome() call
    content = re.sub(r'options=options,\s*\)', 'options=options)', content)
    content = re.sub(r'\(\s*,', '(', content)

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
            print(f"⚠️  Directory not found: {directory}")
            continue
        for filename in sorted(os.listdir(directory)):
            if not filename.endswith(".py"):
                continue
            filepath = os.path.join(directory, filename)
            try:
                if patch_file(filepath):
                    patched += 1
                    print(f"  ✅ Patched: {filename}")
                else:
                    skipped += 1
            except Exception as e:
                errored.append(filename)
                print(f"  ❌ Error patching {filename}: {e}")

    print(f"\nDone. Patched: {patched}, Skipped (no uc): {skipped}, Errors: {len(errored)}")
    if errored:
        print("Errored files:", errored)
