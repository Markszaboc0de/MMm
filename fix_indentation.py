"""
Fixes broken indentation left by the first patch attempt.
Specifically cleans up the orphaned try: / except: blocks and over-indented options lines.
"""

import os
import re
import ast

TARGET_DIRS = [
    "/Users/mac/Desktop/Programozás/Magyar-Manual-main/Magyar/modules",
    "/Users/mac/Desktop/Programozás/Magyar-Manual-main/Manual/modules",
]

def fix_file(filepath):
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()

    # Check if file has a syntax error
    try:
        ast.parse(content)
        return False  # Already good, skip
    except SyntaxError:
        pass

    original = content

    # Pattern 1: remove orphaned `except Exception as e: ...return` blocks that
    # don't have a matching `try:` - these were left by the first patch
    # They look like:
    #   options = Options()
    #       options.add_argument(...)  <- over-indented
    #       options.add_argument("--headless=new")
    #       ...
    #       driver = webdriver.Chrome(...)
    #   except Exception as e:
    #       ...
    #       return

    # Fix over-indented option lines: 8-space indent -> 4-space
    # Find blocks where `options = Options()` is at 4-space indent
    # but subsequent options.add_argument lines are at 8-space indent
    
    lines = content.split('\n')
    fixed_lines = []
    i = 0
    while i < len(lines):
        line = lines[i]
        
        # Detect the orphaned `except` block that closes a non-existent `try:`
        # It appears right after the driver = webdriver.Chrome(...) injected block
        if re.match(r'^    except\s+Exception\s+as\s+\w+:', line):
            # Look back to see if there's a matching try: without indented body
            # If no try: found in context, this is an orphaned except - remove it and the body
            j = i + 1
            while j < len(lines) and (lines[j].startswith('        ') or lines[j].strip() == ''):
                j += 1
            # Skip the except block entirely
            i = j
            continue
        
        # Fix over-indented options.add_argument lines (8-space instead of 4-space)
        # when they follow `    options = Options()`
        if re.match(r'^        options\.add_argument\(', line):
            line = line[4:]  # Remove 4 extra spaces
        
        # Fix over-indented driver = webdriver.Chrome line
        if re.match(r'^        driver\s*=\s*webdriver\.Chrome\(', line):
            line = line[4:]  # Remove 4 extra spaces
            
        # Fix over-indented options = Options() call 
        if re.match(r'^        options\s*=\s*Options\(\)', line):
            line = line[4:]

        fixed_lines.append(line)
        i += 1

    content = '\n'.join(fixed_lines)

    # Verify it parses now
    try:
        ast.parse(content)
        if content != original:
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(content)
            return True
    except SyntaxError as e:
        print(f"    Still broken after fix: {e}")

    return False


if __name__ == "__main__":
    fixed = 0
    still_broken = []

    for directory in TARGET_DIRS:
        if not os.path.exists(directory):
            continue
        for filename in sorted(os.listdir(directory)):
            if not filename.endswith(".py"):
                continue
            filepath = os.path.join(directory, filename)
            try:
                result = fix_file(filepath)
                if result:
                    fixed += 1
                    print(f"  🔧 Fixed: {filename}")
            except Exception as e:
                still_broken.append((filename, str(e)))
                print(f"  ❌ Could not fix {filename}: {e}")

    print(f"\nFixed: {fixed}. Still broken: {len(still_broken)}")
    if still_broken:
        for f, e in still_broken:
            print(f"  {f}: {e}")
