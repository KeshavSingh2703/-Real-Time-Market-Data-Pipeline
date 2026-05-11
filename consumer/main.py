import time
import os

print(f"Starting consumer service. DB_URL: {os.environ.get('DB_URL')}...")
print("Entering infinite sleep loop to keep container alive.")

while True:
    time.sleep(3600)
