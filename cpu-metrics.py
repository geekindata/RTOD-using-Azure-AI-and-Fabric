import json
import psutil
from azure.eventhub import EventHubProducerClient, EventData
import time

# Azure Event Hub connection string and event hub name
connection_string = ""
eventhub_name = ""

# Function to gather system metrics and send to Event Hub
def send_system_metrics():
    # Create a producer client
    producer = EventHubProducerClient.from_connection_string(connection_string, eventhub_name=eventhub_name)

    while True:
        # CPU Metrics
        cpu_percent = psutil.cpu_percent(interval=1)
        cpu_count = psutil.cpu_count(logical=False)
        logical_cpu_count = psutil.cpu_count(logical=True)
        cpu_perc_cores = psutil.cpu_percent(percpu=True)

        # Memory Metrics
        virtual_memory = psutil.virtual_memory()
        swap_memory = psutil.swap_memory()

        # Disk Metrics
        disk_usage = psutil.disk_usage('/')
        disk_io = psutil.disk_io_counters()

        # Network Metrics
        network_io = psutil.net_io_counters()

        # System Sensors (Battery)
        battery_info = psutil.sensors_battery() if hasattr(psutil, 'sensors_battery') else None
        
        # Check if temperature sensors are available
        temperatures = {}
        if hasattr(psutil, 'sensors_temperatures'):
            temperatures = psutil.sensors_temperatures()

        # Prepare the data to send
        system_metrics = {
            "cpu_percent": cpu_percent,
            "cpu_count": cpu_count,
            "logical_cpu_count": logical_cpu_count,
            "cpu_perc_cores": cpu_perc_cores,
            "virtual_memory": {
                "total": virtual_memory.total,
                "used": virtual_memory.used,
                "free": virtual_memory.free,
                "available": virtual_memory.available,
                "percent": virtual_memory.percent
            },
            "swap_memory": {
                "total": swap_memory.total,
                "used": swap_memory.used,
                "free": swap_memory.free,
                "percent": swap_memory.percent
            },
            "disk_usage": {
                "total": disk_usage.total,
                "used": disk_usage.used,
                "free": disk_usage.free,
                "percent": disk_usage.percent
            },
            "disk_io": {
                "read_count": disk_io.read_count,
                "write_count": disk_io.write_count,
                "read_bytes": disk_io.read_bytes,
                "write_bytes": disk_io.write_bytes
            },
            "network_io": {
                "bytes_sent": network_io.bytes_sent,
                "bytes_recv": network_io.bytes_recv,
                "packets_sent": network_io.packets_sent,
                "packets_recv": network_io.packets_recv
            },
            "battery_info": battery_info._asdict() if battery_info else {},
            "temperatures": temperatures
        }

        # Create a batch and send events
        event_data_batch = producer.create_batch()

        # Serialize the event to JSON
        event_data_json = json.dumps(system_metrics)
        print(f"Sending Event: {event_data_json}")

        # Create an EventData instance from the JSON string
        event = EventData(event_data_json)

        try:
            # Try to add the event to the batch
            event_data_batch.add(event)
        except ValueError as e:
            # If the batch is full, send the batch and create a new one
            print("Batch is full, sending batch")
            producer.send_batch(event_data_batch)
            event_data_batch = producer.create_batch()
                
            # Add the event to the new batch
            event_data_batch.add(event)

        # Send the batch
        producer.send_batch(event_data_batch)

        # Sleep for a while before sending the next metrics
        #time.sleep(5)  # Send data every 5 seconds

    # Close the producer client
    producer.close()

# Call the function to start sending system metrics
send_system_metrics()
