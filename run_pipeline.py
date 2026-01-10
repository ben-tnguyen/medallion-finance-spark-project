import subprocess
import sys

def run_script(script_name):
    print(f" Starting: {script_name}...")
    result = subprocess.run([sys.executable, script_name], capture_output=False)
    if result.returncode == 0:
        print(f" {script_name} finished successfully.\n")
    else:
        print(f" Error in {script_name}. Pipeline stopped.")
        sys.exit(1)

if __name__ == "__main__":
    print("---  STARTING AIMCO DATA PIPELINE  ---\n")
    run_script("ingest.py")
    run_script("transform.py")
    run_script("gold_layer.py")
    print("---  ALL STAGES COMPLETE  ---")