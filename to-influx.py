import json
from kafka import KafkaConsumer
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

# --- CONFIG ---
KAFKA_TOPIC = 'patient_vitals'
BOOTSTRAP_SERVERS = 'localhost:29092'

INFLUX_URL = "http://localhost:8086"
INFLUX_TOKEN = "my-super-secret-auth-token"
INFLUX_ORG = "streamhealth"
INFLUX_BUCKET = "vitals"

# --- SETUP ---
print("Connecting to InfluxDB...")
client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
write_api = client.write_api(write_options=SYNCHRONOUS)

print("Connecting to Kafka...")
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Bridge Started: Kafka -> InfluxDB")

#  MAIN LOOP 
for message in consumer:
    data = message.value
    
    # We only care about numbers for the graph
    if data['event_type'] == 'vital_sign':
        try:
            # Create a Data Point
            # Measurement: "vital_signs"
            # Tags: Patient Name, Vital Name (e.g., Heart Rate)
            # Field: The actual value (e.g., 80)
            point = Point("vital_signs") \
                .tag("patient", data['patient_name']) \
                .tag("bed", data['bed_id']) \
                .tag("metric", data['vital_name']) \
                .field("value", float(data['value'])) \
                .time(data['timestamp'])

            write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=point)
            print(f"Stored: {data['patient_name']} - {data['vital_name']}: {data['value']}")
            
        except Exception as e:
            print(f"Error writing to Influx: {e}")