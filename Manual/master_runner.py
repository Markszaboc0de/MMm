import os
import subprocess
import sys
import time

MODULES_FOLDER = "modules"

def run_all_modules():
    print("🚀 Starting the Master Scraper Pipeline...\n")
    
    if not os.path.exists(MODULES_FOLDER):
        print(f"❌ Error: '{MODULES_FOLDER}' folder not found. Please create it.")
        return

    # Find all Python files in the modules folder
    modules = [f for f in os.listdir(MODULES_FOLDER) if f.endswith('.py')]
    
    if not modules:
        print(f"⚠️ No scraper modules found in '{MODULES_FOLDER}/'.")
        return

    print(f"📊 Found {len(modules)} modules to execute. Beginning run...\n")
    print("=" * 50)

    success_count = 0
    fail_count = 0

    for module in modules:
        module_path = os.path.join(MODULES_FOLDER, module)
        print(f"▶️ Running: {module}...")
        
        try:
            # subprocess isolates each script so a crash doesn't kill the whole pipeline
            result = subprocess.run([sys.executable, module_path], capture_output=True, text=True)
            
            if result.returncode == 0:
                print(f"   ✅ Success!")
                success_count += 1
            else:
                print(f"   ❌ Failed. Error log:")
                print(f"      {result.stderr.strip()}")
                fail_count += 1
                
        except Exception as e:
            print(f"   ❌ Critical failure running {module}: {e}")
            fail_count += 1
            
        print("-" * 50)
        time.sleep(2) # Brief pause between modules so your computer doesn't overheat

    print("\n🏁 PIPELINE COMPLETE")
    print(f"📈 Successful Modules: {success_count}")
    print(f"📉 Failed Modules: {fail_count}")

if __name__ == "__main__":
    run_all_modules()