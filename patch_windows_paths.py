import os
import re

TARGET_DIRS = [
    "Magyar/modules",
    "Manual/modules",
]

FIXED = 'DATA_FOLDER = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data")'

patched = 0
for d in TARGET_DIRS:
    for fname in sorted(os.listdir(d)):
        if not fname.endswith(".py"):
            continue
        fp = os.path.join(d, fname)
        content = open(fp, encoding="utf-8").read()
        # Replace any line: DATA_FOLDER = r"C:\..." or DATA_FOLDER = "C:\..."
        new = re.sub(
            r'^DATA_FOLDER\s*=\s*r?"[A-Za-z]:[^"]*"',
            FIXED,
            content,
            flags=re.MULTILINE
        )
        if new != content:
            open(fp, "w", encoding="utf-8").write(new)
            patched += 1
            print(f"  PATCHED: {fname}")

print(f"\nDone. Patched {patched} files.")
