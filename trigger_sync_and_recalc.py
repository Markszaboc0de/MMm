"""
This script triggers the sync_jobs.py update, and once it finishes successfully,
it uses SSH to trigger `recalculate.py` on VM1.

Usage requirements:
- VM2 must have SSH access to VM1 (preferably via SSH keys so it doesn't prompt for a password).
"""

import subprocess
import sys

# ==========================================
# ⚙️ CONFIGURATION FOR VM1 SSH
# ==========================================
VM1_IP = "192.168.1.151"
VM1_USER = "ubuntu"  # Replace with the actual SSH username on VM1 (e.g., ubuntu, opc, etc.)

# The command to run on VM1. You may need to activate a virtual environment first,
# and specify the full path to recalculate.py.
# Example: "cd /home/ubuntu/Resumatch && source venv/bin/activate && python recalculate.py"
RECALCULATE_COMMAND = "cd /home/ubuntu/Resumatch_OCI/recalculate.PY"


def main():
    print("🚀 Step 1: Running database sync (raw_db -> job_match_db)...")
    
    # Run the sync_jobs.py script locally on VM2
    sync_result = subprocess.run([sys.executable, "sync_jobs.py"])
    
    if sync_result.returncode != 0:
        print("❌ Database sync failed or was interrupted. Aborting recalculation.")
        sys.exit(1)
        
    print("\n✅ Step 1 Complete: Database synced successfully.")
    print(f"🚀 Step 2: Triggering recalculate.py on VM1 ({VM1_IP})...")
    
    # Construct the SSH command
    ssh_command = [
        "ssh",
        "-o", "StrictHostKeyChecking=accept-new", # Prevents prompt on first connection
        f"{VM1_USER}@{VM1_IP}",
        RECALCULATE_COMMAND
    ]
    
    print(f"   Executing: {' '.join(ssh_command)}\n")
    
    # Run the SSH command
    recalc_result = subprocess.run(ssh_command)
    
    if recalc_result.returncode == 0:
        print("\n✅ Step 2 Complete: Recalculation triggered successfully on VM1!")
    else:
        print(f"\n❌ Step 2 Failed: SSH command returned exit code {recalc_result.returncode}.")
        print("   Make sure VM2 has SSH access to VM1 and the RECALCULATE_COMMAND is correct.")

if __name__ == "__main__":
    main()
