import urllib.request
import json
import os
import base64
import ssl
import cv2
from azure.eventhub import EventHubProducerClient, EventData
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Retrieve credentials securely
event_hub_connection_str = os.getenv("EVENT_HUB_CONNECTION_STR")
event_hub_name = os.getenv("EVENT_HUB_NAME")
azure_ml_api_key = os.getenv("AZURE_ML_API_KEY")
azure_ml_url = os.getenv("AZURE_ML_URL")

#Function to allow self-signed HTTPS certificates
def allowSelfSignedHttps (allowed):
    if allowed and not os.environ.get('PYTHONHTTPSVERIFY','') and getattr(ssl, '_create_unverified_context', None):
        ssl._create_default_https_context = ssl._create_unverified_context

allowSelfSignedHttps(True) # Enable self-signed HTTPS

#Function to capture image from camera
def capture_camera_image():
    cap = cv2.VideoCapture(0) #Use 0 for the default camera
    ret, frame = cap.read()
    cap.release()
    return frame

#Function to encode image to base64
def encode_image_to_base64(image):
    _, buffer = cv2.imencode('.jpg', image)
    image_base64 = base64.b64encode(buffer).decode('utf-8')
    return image_base64

#Azure Event Hub connection details
event_hub_name = event_hub_name
event_hub_connection_str = event_hub_connection_str

# Initialize Event Hub Producer Client
producer = EventHubProducerClient.from_connection_string(event_hub_connection_str, eventhub_name=event_hub_name)

#Azure AI scoring endpoint details
url = azure_ml_url
api_key = azure_ml_api_key

if not api_key:
    raise Exception("A key should be provided to invoke the endpoint")

headers = {'Content-Type': 'application/json', 'Authorization': ('Bearer ' + api_key)}

#Flag to check if the first result is saved
first_result_saved = False

try:
    while True:
        #Capture image from camera
        image = capture_camera_image()

        # Encode image to base64
        image_base64 = encode_image_to_base64(image)

        # Prepare data for the request
        data = {
            "input_data": {
                "columns": ["image"],
                "index": [0],
                "data": [image_base64]
            },
            "params": {
                "text_prompt": "sample string",
                "custom_entities": True
            }
        }

        body = str.encode(json.dumps(data))

        # Make request to Azure ML scoring endpoint
        req = urllib.request.Request(url, body, headers)

        response = urllib.request.urlopen(req)
        result = response.read().decode('utf-8')

        # Log the raw result for debugging
        print(result)

        # Convert result to JSON object
        result_json = json.loads(result)

        # Log the JSON object for debugging
        print("Parsed JSON result:", result_json)

        #Save the first JSON result to a file
        if not first_result_saved:
            with open("sample_result.json", "w") as outfile:
                json.dump(result_json, outfile, indent=4) # Using indent for pretty-printing to file
            print("First result saved to sample_result.json")
            first_result_saved = True

        # Prepare the JSON string for Event Hub
        # No indent for compact form
        event_data = json.dumps(result_json)

        # Log the event data for debugging
        #print("Event data to be sent:", event_data)

        # Send result to Azure Event Hub
        event_data_batch = producer.create_batch()
        event_data_batch.add(EventData(event_data))
        producer.send_batch(event_data_batch)

        print("Result sent to Azure Event Hub.")

        # Capture and send image to Event Hub
        if image is not None:
            image_base64 = encode_image_to_base64(image)
            event_data = EventData(body=image_base64)
            with producer:
                producer.send_event(event_data)
            #print("Image sent to Event Hub")
        else:
            print("Failed to capture image from camera")

except KeyboardInterrupt:
    print("Camera input stopped by user")

except urllib.error.HTTPError as error:
    print("The request failed with status code: " + str(error.code))
    print(error.info())
    print(error.read().decode("utf8", 'ignore'))

except Exception as ex:
    print("An error occurred:", str(ex))

finally:
    producer.close()
    print("Ingestion stopped to Azure Event Hub.")