import os
import subprocess
import sys
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

def run_script(script_path, cwd):
    print(f"--- Running {script_path} in {cwd} ---")
    try:
        # Run the script, passing the current Python executable
        subprocess.run([sys.executable, script_path], cwd=cwd, check=True)
        print(f"--- Successfully finished {script_path} ---\n")
    except subprocess.CalledProcessError as e:
        print(f"--- Error running {script_path} (Exit code: {e.returncode}) ---\n")

def send_email_notification():
    sender_email = os.getenv("SENDER_EMAIL")
    sender_password = os.getenv("SENDER_PASSWORD") # e.g., Gmail App Password
    recipient_email = os.getenv("RECIPIENT_EMAIL")

    if not sender_email or not sender_password or not recipient_email:
        print("\n⚠️ Email credentials not found in environment variables. Skipping email notification.")
        print("To enable, set SENDER_EMAIL, SENDER_PASSWORD (App Password), and RECIPIENT_EMAIL.")
        return

    print(f"\n📧 Sending completion email to {recipient_email}...")
    subject = "✅ Scrapers Finished Running!"
    body = "The run_all_scrapers.py script has completed executing all attached scraper modules safely."

    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = recipient_email
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'plain'))

    try:
        # Connect to Gmail's SMTP server (Change host/port if using Outlook/Yahoo)
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(sender_email, sender_password)
        text = msg.as_string()
        server.sendmail(sender_email, recipient_email, text)
        server.quit()
        print(f"✅ Notification email successfully sent!")
    except Exception as e:
        print(f"❌ Failed to send email notification: {e}")

def main():
    # Base directory of this script (Magyar-Manual-main)
    base_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Define the scripts to run and their desired working directories
    scripts_to_run = [
        {"script": "main.py", "cwd": os.path.join(base_dir, "ATS scrapers", "Run")},
        {"script": "master_runner.py", "cwd": os.path.join(base_dir, "Magyar")},
        {"script": "master_runner.py", "cwd": os.path.join(base_dir, "Manual")},
    ]

    for item in scripts_to_run:
        script_path = item["script"]
        cwd = item["cwd"]
        
        # Check if the directory and script exist before running
        full_script_path = os.path.join(cwd, script_path)
        if not os.path.exists(full_script_path):
            print(f"Error: Script not found at {full_script_path}")
            continue
            
        run_script(script_path, cwd)

    print("All structured scrapers have completed executing.")
    
    # Trigger the email notification
    send_email_notification()

if __name__ == "__main__":
    main()
