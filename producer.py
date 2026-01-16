import time
import json
import random
import re
import pandas as pd
from kafka import KafkaProducer
from datetime import datetime

# CONFIG
KAFKA_TOPIC = 'patient_vitals'
BOOTSTRAP_SERVERS = 'localhost:29092' # Connects to Kafka container
DATA_PATH = 'synthea_data/csv'         

print("Connecting to Kafka...")
try:
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')  
    )
    print("Connected successfully!")
except Exception as e:
    print(f"FAILED to connect: {e}")
    exit(1)

# STATIC DATA (Patient)
print("\n Loading Patient Data...")
try:
    patients_df = pd.read_csv(f'{DATA_PATH}/patients.csv')

    patients_df = patients_df[['Id', 'FIRST', 'LAST', 'BIRTHDATE', 'GENDER']]
    patients_df['BIRTHDATE'] = pd.to_datetime(patients_df['BIRTHDATE'])

    # Function to calculate age
    def calculate_age(birthdate):
        today = datetime.today()
        return today.year - birthdate.year

    patient_lookup = {}
    for _, row in patients_df.iterrows():
        patient_lookup[row['Id']] = {
            'name': f"{row['FIRST']} {row['LAST']}",
            'age': calculate_age(row['BIRTHDATE']),
            'gender': row['GENDER'],
            'bed_id': f"ICU-{random.randint(101, 200)}"  # Simulated Bed Number
        }
    print(f"Loaded {len(patient_lookup)} patients.")
except Exception as e:
    print(f"Error loading patients.csv: {e}")
    exit(1)

# LOAD & PREPARE OBSERVATIONS
print("Loading Vitals Data...")
try:
    obs_df = pd.read_csv(f'{DATA_PATH}/observations.csv')
    
    # Codes for: Heart Rate, Resp Rate, Body Temp, BP Systolic, BP Diastolic
    TARGET_CODES = ['8867-4', '9279-1', '8310-5', '8480-6', '8462-4']
    vitals = obs_df[obs_df['CODE'].isin(TARGET_CODES)].copy()
    vitals['DATE'] = pd.to_datetime(vitals['DATE'], format='mixed')
    vitals = vitals.sort_values('DATE')
    
    print(f"Found {len(vitals)} vital sign events to stream.")

except Exception as e:
    print(f"Error loading observations.csv: {e}")
    exit(1)

#STREAMING
print("\n STARTING LIVE STREAM")

print("Press Ctrl+C to stop.\n")

# to endless loop
while True:
    try:
        for index, row in vitals.iterrows():
            patient_id = row['PATIENT']
            
            # Skip if patient missing (rare data error)
            if patient_id not in patient_lookup:
                continue

            patient_info = patient_lookup[patient_id]
            # Cleaning the name
            clean_name = re.sub(r'\d+', '', patient_info['name'])
            
            # Construct the "Contextual" Message
            message = {
                "event_type": "vital_sign",
                "timestamp": datetime.utcnow().isoformat(),
                "patient_id": patient_id,
                "patient_name": clean_name,
                "age": patient_info['age'],               # Merged
                "gender": patient_info['gender'],
                "bed_id": patient_info['bed_id'],         # Simulated
                "vital_id": row['CODE'],
                "vital_name": row['DESCRIPTION'],
                "value": row['VALUE'],# "value": 180 if row['DESCRIPTION'] == 'Heart rate' else row['VALUE'],
                "units": row['UNITS']
            }

            # Send to Kafka
            producer.send(KAFKA_TOPIC, message)
            print(f"Sent: {patient_info['name']} - {row['DESCRIPTION']}: {row['VALUE']} {row['UNITS']}")

            # SIMULATE NURSE CHECK-IN 
            # 5% chance to trigger a nurse check-in event
            if random.random() < 0.05:
                nurse_msg = {
                    "event_type": "nurse_check_in",
                    "timestamp": datetime.now().isoformat(),
                    "patient_id": patient_id,
                    "nurse_name": random.choice(["Nurse Jackie", "Nurse gemma", "Nurse Joy"]),
                    "notes": "Patient stable. Vitals checked.",
                    "response": random.choice(["Responsive", "Sleeping"])
                }
                producer.send(KAFKA_TOPIC, nurse_msg)
                print(f"   -> EVENT: Nurse Check-in for {patient_info['name']}")

            # Speed Control: Sleep 0.5s to mimic real-time (Remove this to dump data fast)
            time.sleep(0.2)

    except KeyboardInterrupt:
        print("\nStopping Stream...")
    finally:
        producer.close()