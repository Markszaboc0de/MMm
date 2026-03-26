import os
import subprocess
import sys

root_dir = os.path.dirname(os.path.abspath(__file__))

print("=== EXPORTING MANUAL JOBS ===")
subprocess.run([
    sys.executable, 
    "-c", 
    "import master_runner; from postgres_export import push_to_postgres; jobs = master_runner.get_all_jobs_from_sqlite(); push_to_postgres(jobs)"
], cwd=os.path.join(root_dir, "Manual"))

print("\n=== EXPORTING MAGYAR JOBS ===")
subprocess.run([
    sys.executable, 
    "-c", 
    "import master_runner; from postgres_export import push_to_postgres; jobs = master_runner.get_all_jobs_from_sqlite(); push_to_postgres(jobs)"
], cwd=os.path.join(root_dir, "Magyar"))

print("\n=== EXPORTING ATS JOBS ===")
subprocess.run([
    sys.executable, 
    "main.py", 
    "export"
], cwd=os.path.join(root_dir, "ATS scrapers", "Run"))

print("\n✅ Force export sequence complete!")
