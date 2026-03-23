import os
import sys

# Define proper paths so imports work correctly
root_dir = os.path.dirname(os.path.abspath(__file__))
if root_dir not in sys.path:
    sys.path.insert(0, root_dir)

import main

if __name__ == "__main__":
    print("Forcing manual push of all stored ATS SQLite databases to PostgreSQL...")
    main.export_unified_data()
    print("Done!")
