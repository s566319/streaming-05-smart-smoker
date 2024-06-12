# Student: Inga Miller
# Module 5: streaming-05-smart-smoker
# Date: 5/31/2024
# Date: 6/9/2024


# Objective:
Use RabbitMQ to distribute tasks to multiple workers

One process will create task messages. Multiple worker processes will share the work.

# RabbitMQ Admin
RabbitMQ comes with an admin panel. When you run the task emitter, reply y to open it.

(Python makes it easy to open a web page - see the code to learn how.)

# Prerequisites
RabbitMQ and pika must be installed

Start RabbitMQ in your terminal
With Homebrew on Mac run this command in the terminal in the project:

## Execute the Producer

1. Run emitter_of_tasks.py (say y to monitor RabbitMQ queues)

## Execute a Consumer / Worker

1. Run listening_worker.py

## Ready for Work

1. Use your emitter_of_tasks to produce more task messages.

## Start Another Listening Worker 

1. Use your listening_worker.py script to launch a second worker. 

First, run command in powershel screen: python emitter_of_tasks.py
Second, screenshot of the messages in the terminal

These messages will be sent to three separate queues:

smoker-queue, pork-queue, ribs-queue

Project complete! Great work!  

Screenshots added:
![alt text](image-1.png)
![alt text](image-5.png)
![alt text](image-6.png)
