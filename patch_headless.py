"""
Patch script v4: Replace all webdriver.Chrome initialization with a import of 
get_chrome_driver() from the shared driver_setup.py module.
This avoids all hardcoded paths and works on any platform/architecture.
"""

import os
import re
import ast

TARGET_DIRS = [
    "/Users/mac/Desktop/Programozás/Magyar-Manual-main/Magyar/modules",
    "/Users/mac/Desktop/Programozás/Magyar-Manual-main/Manual/modules",
]

OLD_IMPORT = "import undetected_chromedriver as uc"

NEW_IMPORTS = """import sys as _sys
import os as _os
_sys.path.append(_os.path.dirname(_os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))))
from driver_setup import get_chrome_driver"""


def patch_file(filepath):
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()

    # Only process uc-based or selenium-based scrapers
    if 'webdriver' not in content and 'undetected_chromedriver' not in content:
        return False

    original = content

    # 1. Swap uc import for driver_setup import
    if OLD_IMPORT in content:
        content = content.replace(OLD_IMPORT, NEW_IMPORTS)
    elif 'from driver_setup import get_chrome_driver' not in content:
        # Already patched to selenium but not yet using driver_setup - add import
        # Insert after the last selenium import line
        content = re.sub(
            r'(from selenium\.webdriver\.chrome\.service import Service\n)',
            r'\1' + '\nimport sys as _sys\nimport os as _os\n_sys.path.append(_os.path.dirname(_os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))))\nfrom driver_setup import get_chrome_driver\n',
            content
        )

    # 2. Clean up any uc.* references
    content = content.replace("uc.ChromeOptions()", "Options()")
    content = re.sub(r'uc\.Chrome\(', 'webdriver.Chrome(', content)
    content = re.sub(r',?\s*version_main\s*=\s*\w+', '', content)

    # 3. Strip ALL old headless/binary/service lines
    strip_patterns = [
        r'[ \t]*options\.add_argument\(["\']--headless(?:=new)?["\']\)[ \t]*\n',
        r'[ \t]*#\s*options\.add_argument\(["\']--headless.*?\n',
        r'[ \t]*options\.add_argument\(["\']--no-sandbox["\']\)[ \t]*\n',
        r'[ \t]*options\.add_argument\(["\']--disable-dev-shm-usage["\']\)[ \t]*\n',
        r'[ \t]*options\.add_argument\(["\']--disable-gpu["\']\)[ \t]*\n',
        r'[ \t]*options\.add_argument\(["\']--window-size=.*?["\']\)[ \t]*\n',
        r'[ \t]*options\.binary_location\s*=.*\n',
        r'[ \t]*_service\s*=\s*Service\(.*?\)\n',
    ]
    for pattern in strip_patterns:
        content = re.sub(pattern, '', content)

    # 4. Remove orphaned try/except blocks left by previous patches
    content = re.sub(
        r'[ \t]*except Exception as e:\n[ \t]+print\(f?"[^"]*\{e\}[^"]*"\)\n[ \t]+return\n',
        '',
        content,
        flags=re.IGNORECASE
    )

    # 5. Replace ALL driver = webdriver.Chrome(...) patterns with get_chrome_driver()
    content = re.sub(
        r'^([ \t]*)driver\s*=\s*webdriver\.Chrome\([^)]*\)',
        r'\1driver = get_chrome_driver()',
        content,
        flags=re.MULTILINE
    )
    # Also handle existing Options() setup blocks - replace driver creation
    # in case the pattern spans options that are now only Options()
    content = re.sub(
        r'^([ \t]*)driver\s*=\s*webdriver\.Chrome\(service=\w+,\s*options=\w+\)',
        r'\1driver = get_chrome_driver()',
        content,
        flags=re.MULTILINE
    )

    # 6. Remove now-unused Options() init and option building lines
    # (keep other args like window-size if somehow they remained)
    content = re.sub(r'^[ \t]*options\s*=\s*Options\(\)\s*\n', '', content, flags=re.MULTILINE)

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
