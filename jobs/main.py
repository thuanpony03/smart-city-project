import datetime
import os
import random
import uuid
from confluent_kafka import SerializingProducer
import simplejson as json
import time

from confluent_kafka import Producer



HCM_COORDINATES= {"latitude": 10.7765, "longtitude": 106.7009}
DALAT_COORDINATES= {"latitude": 11.9404, "longtitude": 108.4583}

LATITUDE_INCREMENT = (DALAT_COORDINATES["latitude"] - HCM_COORDINATES["latitude"])/100
LONGTITUDE_INCREMENT = (DALAT_COORDINATES["longtitude"] - HCM_COORDINATES["longtitude"])/100

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

VEHICLE_TOPIC = os.getenv("VEHICLE_TOPIC", "vehicle_data")
GPS_TOPIC = os.getenv("GPS_TOPIC", "gps_data")
TRAFFIC_TOPIC = os.getenv("TRAFFIC_TOPIC", "traffic_data")
WEATHER_TOPIC = os.getenv("WEATHER_TOPIC", "weather_data")
EMERGENCY_TOPIC = os.getenv("EMERGENCY_TOPIC", "emergency_data")

start_time = datetime.datetime.now()
start_location = HCM_COORDINATES.copy()

def get_next_time():
    global start_time
    start_time += datetime.timedelta(seconds=random.randint(30, 60))
    return start_time

def simulate_vehicle_movement():
    global start_location
    start_location['latitude'] += LATITUDE_INCREMENT
    start_location['longtitude'] += LONGTITUDE_INCREMENT

    start_location["latitude"] += random.uniform(-0.0005, 0.0005)
    start_location["longtitude"] += random.uniform(-0.0005, 0.0005)
    return start_location

def generate_vehicle_data(device_id):
    location = simulate_vehicle_movement()
    return {
        'id': uuid.uuid4(),
        'deviceId': device_id,
        'timestamp': get_next_time().isoformat(),
        'location': location,
        'speed': random.randint(20, 40),
        'direction': 'north',
        'make': 'BMW',
        'model': 'C500',
        'year': 2024,
        'fuelType': 'Hybrid'
    }

def generate_gps_data(device_id, timestamp, vehicle_type='private'):
    return {
        'id': uuid.uuid4(),
        'deviceId': device_id,
        'timestamp': timestamp,
        'speed': random.randint(20, 40),
        'direction': 'north',
        'vehicle_type': vehicle_type

    }

def generate_traffic_camera_data(device_id, location, timestamp, camera_id):
    return {
        'id': uuid.uuid4(),
        'deviceId': device_id,
        'location': location,
        'timestamp': timestamp,
        'cameraId': camera_id,
        'snapshot': 'Base64EncodedString'
    }

def generate_weather_data(device_id, location, timestamp):
    return {
        'id': uuid.uuid4(),
        'deviceId': device_id,
        'location': location,
        'timestamp': timestamp,
        'weatherCondition': random.choice(['sunny', 'rainy', 'cloudy', 'foggy']),
        'temperature': random.uniform(15, 32),
        'windSpeed': random.uniform(0, 100),
        'humidity': random.randint(0, 100),
        'airQualityIndex': random.uniform(0, 500)
    }

def generate_emergency_incident_data(device_id, location, timestamp):
    return {
        'id': uuid.uuid4(),
        'deviceId': device_id,
        'incidentId': uuid.uuid4(),
        'location': location,
        'timestamp': timestamp,
        'incidentType': random.choice(['Accident', 'Police', 'Medical', 'None', 'Fire']),
        'status': random.choice(['active', 'resolved']),
        'description': 'Description of the incident'
    }

def json_serializer(obj):
    if isinstance(obj, uuid.UUID):
        return str(obj)
    raise TypeError(f'Object of type {obj.__class__.__name__} is not JSON serializable')

def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

def produce_data_to_kafka(producer, topic, data):
    producer.produce(
        topic,
        key = str(data['id']),
        value = json.dumps(data, default=json_serializer).encode('utf-8'),
        on_delivery = delivery_report
    )

    producer.flush()


    


def simulate_journey(producer, device_id):
    while True:
        vehicle_data = generate_vehicle_data(device_id)
        gps_data = generate_gps_data(device_id, vehicle_data['timestamp'])
        traffic_camera_data = generate_traffic_camera_data(device_id, vehicle_data['location'], vehicle_data['timestamp'], camera_id='Nikon')
        weather_data = generate_weather_data(device_id, vehicle_data['location'], vehicle_data['timestamp'])
        emergency_incident_data = generate_emergency_incident_data(device_id, vehicle_data['location'], vehicle_data['timestamp'])

        if (vehicle_data['location']['latitude'] >= DALAT_COORDINATES['latitude'] and vehicle_data['location']['longtitude'] <= DALAT_COORDINATES['longtitude']):
            
            print('The vehicle has reached DA LAT. Simulate ending...')
            break 

        produce_data_to_kafka(producer, VEHICLE_TOPIC, vehicle_data)
        produce_data_to_kafka(producer, GPS_TOPIC, gps_data)
        produce_data_to_kafka(producer, TRAFFIC_TOPIC, traffic_camera_data)
        produce_data_to_kafka(producer, WEATHER_TOPIC, weather_data)
        produce_data_to_kafka(producer, EMERGENCY_TOPIC, emergency_incident_data)
        time.sleep(2)



        # print(vehicle_data)
        # print(gps_data)
        # print(traffic_camera_data)
        # print(weather_data)
        # print(emergency_incident_data)
        # break

if __name__ == "__main__":
    producer_config = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'error_cb': lambda err: print(f'Kafka error: {err}')
    }

    producer = SerializingProducer(producer_config)

    try: 
        simulate_journey(producer, 'Pony-car')
    except KeyboardInterrupt:
        print("Simulation ended by user")
    except Exception as e:
        print(f'Unexpected error occurred: {e}')