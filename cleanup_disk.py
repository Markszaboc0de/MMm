#!/usr/bin/env python3
"""
cleanup_disk.py
Automatically cleans up disk space hogs on the Ubuntu VM.
Safe to run repeatedly. Must be run as root (sudo) for full effect.

Cleans:
  - Chromium snap temp files (/tmp/snap-private-tmp/snap.chromium/tmp/)
  - Old systemd journal logs (keeps last 7 days, caps at 500MB going forward)
  - Old/disabled snap revisions
"""

import subprocess
import os
import shutil
import sys

def run(cmd, check=False):
    """Run a shell command and print it."""
    print(f"  $ {cmd}", flush=True)
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if result.stdout.strip():
        print(f"    {result.stdout.strip()}", flush=True)
    if result.stderr.strip():
        print(f"    ⚠️  {result.stderr.strip()[:200]}", flush=True)
    return result.returncode == 0

def cleanup_chromium_tmp():
    """Delete Chromium snap temp cache — often grows to 9GB+."""
    path = "/tmp/snap-private-tmp/snap.chromium/tmp"
    if os.path.exists(path):
        try:
            freed = sum(
                os.path.getsize(os.path.join(dp, f))
                for dp, dn, filenames in os.walk(path)
                for f in filenames
                if not os.path.islink(os.path.join(dp, f))
            ) / (1024**3)
            shutil.rmtree(path, ignore_errors=True)
            os.makedirs(path, exist_ok=True)
            print(f"  ✅ Cleared ~{freed:.1f}GB of Chromium temp files.", flush=True)
        except Exception as e:
            print(f"  ⚠️  Chromium tmp delete failed: {e}", flush=True)
    else:
        print(f"  ✅ Chromium tmp dir not found — nothing to clean.", flush=True)

def cleanup_journal_logs():
    """Vacuum systemd journal to last 7 days and set a 500MB hard cap."""
    print("  Vacuuming systemd journal logs to last 7 days...", flush=True)
    run("journalctl --vacuum-time=7d")
    
    # Set permanent cap in /etc/systemd/journald.conf
    conf_path = "/etc/systemd/journald.conf"
    if os.path.exists(conf_path):
        try:
            with open(conf_path, 'r') as f:
                content = f.read()
            
            # Replace or inject the cap setting
            if "SystemMaxUse=" in content:
                # Update any existing line (even commented-out ones)
                import re
                content = re.sub(r'#?SystemMaxUse=.*', 'SystemMaxUse=500M', content)
            else:
                content += "\nSystemMaxUse=500M\n"
                
            with open(conf_path, 'w') as f:
                f.write(content)
                
            run("systemctl restart systemd-journald")
            print("  ✅ Journal log permanent cap set to 500MB.", flush=True)
        except PermissionError:
            print("  ⚠️  Not root — skipping journald.conf update. Run with sudo to apply permanent cap.", flush=True)
    else:
        print(f"  ⚠️  journald.conf not found. Skipping permanent cap.", flush=True)

def cleanup_old_snaps():
    """Remove old disabled snap revisions — often saves 2-3GB."""
    print("  Removing old/disabled snap revisions...", flush=True)
    result = subprocess.run("snap list --all", shell=True, capture_output=True, text=True)
    
    removed = 0
    for line in result.stdout.splitlines():
        if "disabled" in line:
            parts = line.split()
            if len(parts) >= 3:
                snapname = parts[0]
                revision = parts[2]
                ok = run(f"snap remove {snapname} --revision={revision}")
                if ok:
                    removed += 1
                    
    if removed:
        print(f"  ✅ Removed {removed} old snap revision(s).", flush=True)
    else:
        print("  ✅ No old snap revisions found.", flush=True)
        
    # Set snap to only retain 2 revisions going forward
    run("snap set system refresh.retain=2")

def main():
    print("\n" + "="*50)
    print("🧹 DISK CLEANUP RUNNING...")
    print("="*50)
    
    is_root = os.geteuid() == 0
    if not is_root:
        print("⚠️  WARNING: Not running as root. Some cleanups (journald config, snaps) may be skipped.")
        print("   For full cleanup: sudo python cleanup_disk.py\n")
    
    print("\n[1/3] Clearing Chromium snap temp files...")
    cleanup_chromium_tmp()
    
    print("\n[2/3] Vacuuming systemd journal logs...")
    cleanup_journal_logs()
    
    print("\n[3/3] Removing old snap revisions...")
    cleanup_old_snaps()
    
    print("\n" + "="*50)
    print("✅ Disk cleanup complete!")
    print("="*50 + "\n")

if __name__ == "__main__":
    main()
