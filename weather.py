import time
import boto3
import weatherhat
from datetime import datetime
from io import StringIO
import csv

# AWS credentials
AWS_ACCESS_KEY = secret
AWS_SECRET_KEY = secret

# S3 bucket details
S3_BUCKET_NAME = 'goofygooberweatherstation'
S3_FOLDER_NAME = 'weather_data/'

def upload_to_s3(data):
    """Uploads data to S3 bucket."""
    try:
        s3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY)
        now = datetime.now()
        filename = S3_FOLDER_NAME + now.strftime("%d-%m-%Y_%H:%M:%S") + ".csv"
        s3.put_object(Bucket=S3_BUCKET_NAME, Key=filename, Body=data)
        print(f"Data uploaded to S3: {filename}")
    except Exception as e:
        print(f"Error uploading to S3: {str(e)}")

sensor = weatherhat.WeatherHAT()

print("""
basic.py - Basic example showing how to read Weather HAT's sensors.
Press Ctrl+C to exit!
""")

while True:
    sensor.update(interval=60.0)

    wind_direction_cardinal = sensor.degrees_to_cardinal(sensor.wind_direction)

    data_dict = {'Date': datetime.now().strftime("%d-%m-%Y"),
                 'Temperature': sensor.temperature,
                 "Humidity": sensor.humidity,
                 "Pressure": sensor.pressure,
                 "Light": sensor.lux,
                 "Wind": sensor.wind_speed,
                 "Rainfall": sensor.rain,
                 "Wind Direction": wind_direction_cardinal}
    
    fields = ['Date', 'Temperature', 'Humidity', 'Pressure', 'Light', 'Wind', 'Rainfall', 'Wind Direction']

    csv_buffer = StringIO()
    csv_writer = csv.DictWriter(csv_buffer, fieldnames=fields)
    csv_writer.writeheader()
    
    data_list = [data_dict]  # Convert dictionary to list
    csv_writer.writerows(data_list)  # Write the list of dictionaries to CSV

    csv_data = csv_buffer.getvalue()  # Get CSV data as a string

    # Print sensor data
    print(data_dict)

    # Upload data to S3 bucket
    upload_to_s3(csv_data)

    time.sleep(60 )  # Sleep for 1 minute
