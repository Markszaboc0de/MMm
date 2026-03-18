import os
import re

directories = [
    r"/Users/mac/Desktop/Programozás/Magyar-Manual-main/Magyar/modules",
    r"/Users/mac/Desktop/Programozás/Magyar-Manual-main/Manual/modules"
]

def patch_file(filepath):
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()

    original = content

    # 1. Ensure headless is added right before uc.Chrome initialization
    # If the file uses uc.ChromeOptions(), we ensure --headless=new is there.
    if 'uc.ChromeOptions()' in content:
        # First, remove any commented out headless arguments
        content = re.sub(r'#\s*options\.add_argument\([\'"]--headless=new[\'"]\).*?\n', '', content)
        content = re.sub(r'options\.add_argument\([\'"]--headless=new[\'"]\).*?\n', '', content)
        
        # Add headless properly before the driver init
        content = re.sub(r'(driver\s*=\s*uc\.Chrome\()', 
                         r'options.add_argument("--headless=new")\n    \1', 
                         content)

    # 2. Remove version_main=XXX
    content = re.sub(r',?\s*version_main\s*=\s*\d+', '', content)
    content = re.sub(r',?\s*version_main\s*=\s*CHROME_VERSION', '', content)
    
    # 3. Clean up the signature if it became `uc.Chrome(options=options, )`
    content = re.sub(r'options=options,\s*\)', 'options=options)', content)

    if content != original:
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(content)
        return True
    return False

if __name__ == "__main__":
    patched = 0
    for directory in directories:
        if not os.path.exists(directory):
            continue
        for filename in os.listdir(directory):
            if filename.endswith(".py"):
                filepath = os.path.join(directory, filename)
                if patch_file(filepath):
                    patched += 1
                    
    print(f"Patched {patched} scraper modules to be permanently headless and version-agnostic.")
