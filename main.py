from umqtt.simple import MQTTClient
import machine
import time
import ubinascii
import utime
import ujson  # Import ujson for JSON handling
from ota import OTAUpdater
from WIFI_CONFIG import SSID, PASSWORD
import ntptime


#if needed, overwrite default time server
ntptime.host = "0.uk.pool.ntp.org"

firmware_url = "https://raw.githubusercontent.com/giz1007/dosing_pump/main/"

# MQTT Configuration
MQTT_BROKER = '192.168.10.52'
MQTT_CLIENT_ID = ubinascii.hexlify(machine.unique_id())
MQTT_TOPIC_PREFIX = "dosing_pump"
MQTT_TOPIC_CALIBRATION = "calibration"
MQTT_TOPIC_PRIME = "prime"
MQTT_TOPIC_LOGS = "logs"  # New MQTT topic for logs
MQTT_TOPIC_WATCHDOG = "watchdog"
MQTT_TOPIC_RESET = "restart"
MQTT_TOPIC_UPDATE = "update" 

# Pump Configuration
DOSING_PUMPS = {
    "pump1": {"pin": 14, "topic": "pump1/volume_mls", "prime": "pump1/prime"},
    "pump2": {"pin": 4, "topic": "pump2/volume_mls","prime": "pump2/prime"},
    "pump3": {"pin": 12, "topic": "pump3/volume_mls", "prime": "pump3/prime"},
    "pump4": {"pin": 13, "topic": "pump4/volume_mls", "prime": "pump4/prime"},
}


# Default Calibration Constant
#DEFAULT_CALIBRATION = 0.4
# Default Calibration Constants
DEFAULT_CALIBRATIONS = {
    "pump1": {"calibration": 0.3540, "slope": 1.0, "intercept": 0.0},
    "pump2": {"calibration": 0.3540, "slope": 1.0, "intercept": 0.0},
    "pump3": {"calibration": 0.3740, "slope": 1.0, "intercept": 0.0},
    "pump4": {"calibration": 0.4000, "slope": 1.13, "intercept": -0.77},
}

#format timestamp for message to broker
def format_timestamp(timestamp):
    # Convert timestamp to a tuple representing local time
    time_tuple = utime.localtime(timestamp)

    # Format the time tuple as a string
    time_str = "{:04d}-{:02d}-{:02d} {:02d}:{:02d}:{:02d}".format(
        time_tuple[0], time_tuple[1], time_tuple[2],
        time_tuple[3], time_tuple[4], time_tuple[5]
    )

    return time_str


#read and write a request to update the main file from the system.
def read_update():
    try:
        with open(f"update.txt", "r") as file:
            return int(file.read())
    except OSError:
        return None  # Return None if file doesn't exist

def write_update(update):
    try:
        with open(f"update.txt", "w") as file:
            file.write(str(update))
    except OSError:
        print(f"Failed to write speed for {stirrer_name} to file.")


# Function to write calibration constants to the file
def write_calibration(pump, calibration, slope, intercept):
    try:
        calibration_file = "/calibration_{}.txt".format(pump)
        with open(calibration_file, 'w') as f:
            f.write("{:.4f} {:.4f} {:.4f}".format(calibration, slope, intercept))
        print(f"Calibration for {pump} written to file: {calibration}, Slope: {slope}, Intercept: {intercept}")
    except Exception as e:
        log_message = f"Failed to write calibration for {pump} to file: {e}"
        print(log_message)
        publish_log(log_message)

# Function to read the calibration constant for a specific pump from a file
def read_calibration(pump):
    try:
        calibration_file = "/calibration_{}.txt".format(pump)
        with open(calibration_file, 'r') as f:
            calibration, slope, intercept = map(float, f.read().split())
            return calibration, slope, intercept
    except (ValueError, OSError):
        log_message = f"Failed to read calibration for {pump} from file. Reverting to default."
        print(log_message)
        publish_log(log_message)
        return DEFAULT_CALIBRATIONS[pump]["calibration"], DEFAULT_CALIBRATIONS[pump]["slope"], DEFAULT_CALIBRATIONS[pump]["intercept"]

# Function to control a dosing pump
def pump_control(pump_name, duration,dose_type):
    try:
        log_message = f"Turning {pump_name} on for {duration} seconds..."
        print(log_message)
        #publish_log(log_message)
        
        pump_pin = machine.Pin(DOSING_PUMPS[pump_name]["pin"], machine.Pin.OUT)
        pwm = machine.PWM(pump_pin)  # Initialize PWM for the pump pin
        pwm.freq(1000)  # Set PWM frequency (adjust as needed)

        # code to make the acceleration and deceleration % of the over all run duration
        
        # Acceleration phase
        for step in range(5):
            acceleration = (step + 1) * 20  # Increase speed in steps
            pwm.duty(int(acceleration * 1023 / 100))
            utime.sleep_us(int(100000))   # Adjust the acceleration duration

        # Constant speed phase
        pwm.duty(int(100 * 1023 / 100))  # Set maximum duty cycle for constant speed
        print(f"Turning {pump_name} on for {duration} seconds...")

        # Maintain constant speed for the specified duration
        duration = duration -1
        utime.sleep_us(int(duration * 1000000))

        # Deceleration phase
        for step in range(5):
            deceleration = (5- step) * 20  # Decrease speed in steps
            pwm.duty(int(deceleration * 1023 / 100))
            utime.sleep_us(int(100000))  # Adjust the deceleration duration

        # Turn off the pump
        pwm.duty(0)
        pwm.deinit()  # Deinitialize PWM
        calibration_value, slope, intercept = read_calibration(pump_name)
        volume_timed = duration +1  # duration the dose requested * calibration - accel & deceleration so the acc + Dec need to be readded in.
        #dose_volume_delivered = volume_timed / calibration_value
        volume_requested = (volume_timed / calibration_value)*slope+intercept
        
        publish_pump_run_info(pump_name, calibration_value, volume_requested, dose_type, slope, intercept)
        message = f"dosing {pump_name} task completed"
        publish_log(message)

    except Exception as e:
        log_message = f"Failed to control {pump_name}: {e}"
        print(log_message)
        publish_log(log_message)

# Function to publish log messages to the MQTT broker
def publish_log(message):
    try:
        mqtt_client.publish(f"{MQTT_TOPIC_PREFIX}/{MQTT_TOPIC_LOGS}", message)
    except Exception as e:
        print(f"Failed to publish log message: {e}")

# check to publish that the dosing mechanism is working as intended, as simple watchdog
def publish_working_status():
    try:
        #current_timestamp = utime.time()  # Get the current timestamp
        #formatted_time = format_timestamp(current_timestamp)
        #status_message = f"{formatted_time},ok"
        status_message = ("Local Time: %s" %str(time.localtime()))
        mqtt_client.publish(f"{MQTT_TOPIC_PREFIX}/{MQTT_TOPIC_WATCHDOG}", status_message)
        print(f"Published status message: {status_message}")
    except Exception as e:
        print(f"Failed to publish status message: {e}")

# Function to publish pump run information to MQTT  - JSON mqtt message
def publish_pump_run_info(pump_name, calibration_value, volume_requested, dose_type, slope, intercept):
    try:
        #timestamp = utime.time()
        #formatted_time = format_timestamp(timestamp)
        formatted_time = ("Time: %s" %str(time.localtime()))
        info_dict = {
            "timestamp": formatted_time,
            "pump_name": pump_name,
            "volume_requested": volume_requested,
            "dosing_type": dose_type,
            "calibration_constant": calibration_value,
            "slope": slope,
            "intercept": intercept            
        }

        json_message = ujson.dumps(info_dict)
        topic = f"{MQTT_TOPIC_PREFIX}/{pump_name}"
        mqtt_client.publish(topic, json_message)
        print(f"Published pump run info for {pump_name}: {json_message}")

    except Exception as e:
        print(f"Failed to publish pump run info: {e}")


def esp8266_reset_request():
    try:
        machine.reset()        
    except Exception as e:
        print(f"Failed to reset: {e}")

# MQTT Callback function
def mqtt_callback(topic, msg):
    print(f"Received message: {msg} on topic: {topic}")
    
    if MQTT_TOPIC_RESET in topic:
        try:
            esp8266_reset_request()
        except Exception as e:
                log_message = f"Failed Reset system: {e}"
                print(log_message)
                publish_log(log_message)
    
    if MQTT_TOPIC_UPDATE in topic:
        try:
            log_message = f"update main request received."
            # the update here is 1 
            interval = int(msg.decode('utf-8'))
            print(log_message)  # Debugging
            write_update(interval)
        except Exception as e:
            log_message = f"Failed to process control message: {e}"
            print(log_message)
            publish_log(log_message)

# Calibration handling
    # Calibration handling
    if MQTT_TOPIC_CALIBRATION in topic:
        try:
            pump_name = topic.split(b"/")[1].decode('utf-8')  # Extract pump name from the middle of the topic
            #old working lines - note functions above would need to be changed.
            #calibration_value = float(msg)
            #write_calibration(pump_name, calibration_value)
            # new lines
            calibration_value, slope, intercept = map(float, msg.split()) 
            write_calibration(pump_name, calibration_value, slope, intercept)
            print(f"Calibration for {pump_name} updated: {calibration_value}, Slope: {slope}, Intercept: {intercept}")

            # Check if pump_name is not an empty string before publishing
            if pump_name:
                # Convert the calibration value to a string
                calibration_str = str(calibration_value)

                # Publish the updated calibration value
                # mqtt_client.publish(f"{MQTT_TOPIC_PREFIX}/{pump_name}/{MQTT_TOPIC_CALIBRATION}", calibration_str)
                # print(f"Published calibration constant for {pump_name}: {calibration_value}")
            else:
                print("Cannot publish calibration constant. Empty pump_name.")

        except ValueError as ve:
            log_message = f"ValueError handling calibration constant for {pump_name}: {ve}. Reverting to default."
            print(log_message)
            publish_log(log_message)
            calibration_value, slope, intercept = DEFAULT_CALIBRATIONS[pump_name]["calibration"], DEFAULT_CALIBRATIONS[pump_name]["slope"], DEFAULT_CALIBRATIONS[pump_name]["intercept"]
        except TypeError as te:
            log_message = f"TypeError handling calibration constant for {pump_name}: {te}. Reverting to default."
            print(log_message)
            publish_log(log_message)
            calibration_value, slope, intercept = DEFAULT_CALIBRATIONS[pump_name]["calibration"], DEFAULT_CALIBRATIONS[pump_name]["slope"], DEFAULT_CALIBRATIONS[pump_name]["intercept"]
        except Exception as e:
            log_message = f"An unexpected error occurred during calibration handling: {e}. Reverting to default."
            print(log_message)
            publish_log(log_message)
            calibration_value, slope, intercept = DEFAULT_CALIBRATIONS[pump_name]["calibration"], DEFAULT_CALIBRATIONS[pump_name]["slope"], DEFAULT_CALIBRATIONS[pump_name]["intercept"]

    # Dosing pump control handling
    for pump_name, pump in DOSING_PUMPS.items():
        if topic.endswith(pump["topic"]):
            try:
                #calibration_value = read_calibration(pump_name)
                calibration_value, slope, intercept = read_calibration(pump_name)
                duration = float(msg)
                #duration *= calibration_value  # Apply calibration constant to the duration
                volume_requested = duration
                adjusted_volume = ((volume_requested - intercept)/ slope) * calibration_value
                duration = adjusted_volume
                dose_type = "standard"
                pump_control(pump_name, duration,dose_type)
            except Exception as e:
                log_message = f"Failed control for {pump_name}: {e} at the message decrypting section"
                print(log_message)
                publish_log(log_message)

    
    # Pump priming handling - added pump priming section
    for pump_name, pump in DOSING_PUMPS.items():
        if topic.endswith(pump["prime"]):
            try:
                duration = float(msg)
                calibration_value, slope, intercept = read_calibration(pump_name)
                #duration *= calibration_value  # Apply calibration constant to the duration
                volume_requested = duration
                adjusted_volume = ((volume_requested - intercept)/ slope) * calibration_value
                duration = adjusted_volume
                dose_type = "prime"
                pump_control(pump_name, duration,dose_type)
            except Exception as e:
                log_message = f"Failed to prime {pump_name}: {e}"
                print(log_message)
                publish_log(log_message)





# Initialize MQTT client
mqtt_client = MQTTClient(MQTT_CLIENT_ID, MQTT_BROKER)
mqtt_client.set_callback(mqtt_callback)
mqtt_client.connect()
mqtt_client.subscribe(b"{}/#".format(MQTT_TOPIC_PREFIX))
mqtt_client.subscribe(b"{}/#".format(MQTT_TOPIC_PRIME))
mqtt_client.subscribe(b"{}/#".format(MQTT_TOPIC_UPDATE)) 

try:
    hour_counter = 0
    while True:
        mqtt_client.check_msg()      
        # Increment the counter every second over 1 hour to check if its working.
        hour_counter += 1

        # Check if an hour has passed
        if hour_counter >= 30:  # 3600 seconds in an hour, so check every 10 minutes
            publish_working_status()  # Publish the working status
            hour_counter = 0  # Reset the hour_counter

        time.sleep(1)
        print(hour_counter)
        update_triggered = read_update()
        print({update_triggered})
        if update_triggered == 1:
            write_update("0")
            ota_updater = OTAUpdater(SSID, PASSWORD, firmware_url, "main.py")
            ota_updater.download_and_install_update_if_available()
        
        
        
except KeyboardInterrupt:
    print("Keyboard interrupt. Disconnecting from MQTT broker.")
    mqtt_client.disconnect()


