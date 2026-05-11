import time
import os

print(f"Starting producer service. Target KAFKA_BROKER: {os.environ.get('KAFKA_BROKER')}...")
print("Entering infinite sleep loop to keep container alive.")

while True:
    time.sleep(3600)
