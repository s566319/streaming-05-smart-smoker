"""
    This program sends a message to a queue on the RabbitMQ server.
    Make tasks harder/longer-running by adding dots at the end of the message.
    Messages are sent from a defined CSV file (smoker-temps.csv in this example).
"""

import pika
import sys
import webbrowser
import csv
import time
import logging 

"""
Define some file-level variables that will be used throughout the program
"""
# Defining what we are going to call our smoker and our two foods
bbq_smoker = "BBQ Smoker"
pork = "Pork Chops"
ribs = "Ribs"
# Define a file-level variable to store the unicode degree sign
# This variable can be called to print the degree symbol after temperature readings
degree_sign = u'\N{DEGREE SIGN}'

# [TODO] Implement logging to a file in Module 6
# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def offer_rabbitmq_admin_site():
    """Offer to open the RabbitMQ Admin website"""
    ans = input("Would you like to monitor RabbitMQ queues? y or n ")
    print()
    if ans.lower() == "y":
        webbrowser.open_new("http://localhost:15672/#/queues")
        print()

def send_message(host: str, queue_name: str, message: str):
    """
    Creates and sends a message to the queue each execution.
    This process runs and finishes.

    Parameters:
        host (str): the host name or IP address of the RabbitMQ server
        queue_name (str): the name of the queue
        message (str): the message to be sent to the queue
    """
    try:
        conn = pika.BlockingConnection(pika.ConnectionParameters(host))
        ch = conn.channel()
        ch.queue_declare(queue=queue_name, durable=True)
        ch.basic_publish(exchange="", routing_key=queue_name, body=message)
        print(f"{message}")
    except pika.exceptions.AMQPConnectionError as e:
        print(f"Error: Connection to RabbitMQ server failed: {e}")
        sys.exit(1)
    finally:
        conn.close()

def main(host: str, filename: str):
    """
    Open a CSV and iterate through each row of the CSV.
    Separate three processes by column and send each message to the queue
    by calling the send_message() function.

    Parameters:
    host (str): the host name or IP address of the RabbitMQ server
    filename (str): the location of the input file

    Code comments are above the code, referring to the code of the next line
    """
    with open(filename, 'r', newline='') as file:
        reader = csv.DictReader(file)
        # Skips header of "Time (UTC),Channel1,Channel2,Channel3"
        next(reader)
        # Iterate through each row
        for row in reader:
            # Store the first column in a variable
            time_utc = row['Time (UTC)']
            # Store the second column in a varible
            smoker_temp = row['Channel1']
            # Store the third coumn in a variable
            pork_temp = row['Channel2'] 
            # Store the fourth column in a variable
            ribs_temp = row['Channel3']
            # Split out the 'Time_UTC' column for date and time
            date_split, time_split = time_utc.split(' ')
            # Always true
            if smoker_temp:
                # Skip null values
                if all(value =='' for value in row.values()):
                    # Continue iterating after skipping a null row
                    continue
                # Example message reading: "BBQ Smoker Reading = Date: 05/22/21, Time: 12:20:20; temp is 84.2°"
                message = f"{bbq_smoker} Reading = Date: {date_split}, Time: {time_split}; temp is {float(smoker_temp)}{degree_sign}"
                # Send the message to the 'smoker-queue'
                send_message(host, "smoker-queue", message)
            # Always true
            if pork_temp:
                # Skip null values (there isn't a Channel2 reading until line 178 of the CSV)
                if all(value == '' for value in row.values()):
                    # Continue iterating after skipping a null row
                    continue
                # Example message reading: BBQ Smoker Reading = Date: 05/22/21, Time: 13:46:35; Pork temp is 38.7°
                message = f"{bbq_smoker} Reading = Date: {date_split}, Time: {time_split}; {pork} temp is {float(pork_temp)}{degree_sign}"
                # Send the message to the 'pork-queue'
                send_message(host, 'pork-queue', message)
            if ribs_temp: # Always true
                # Skip null values (there isn't a Channel3 reading until line 179 of the CSV)
                if all(value == '' for value in row.values()):
                    # Continue iterating after skipping a null row
                    continue
                # Example message reading: BBQ Smoker Reading = Date: 05/22/21, Time: 13:46:40; Ribs temp is 37.0°
                message = f"{bbq_smoker} Reading = Date: {date_split}, Time: {time_split}; {ribs} temp is {float(ribs_temp)}{degree_sign}"
                # Send the message to the 'ribs-queue'
                send_message(host, 'ribs-queue', message)

            # After iterating through a row, sleep for 30 seconds
            time.sleep(0.1)

if __name__ == "__main__":  
    offer_rabbitmq_admin_site()    

    temp_file = 'smoker-temps.csv'
    host = 'localhost'
    main(host, temp_file)