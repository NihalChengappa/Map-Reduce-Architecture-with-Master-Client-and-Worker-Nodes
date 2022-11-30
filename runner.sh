#! /usr/bin/bash

echo "running the workers"

gnome-terminal  -e "python3 worker_1.py" &
gnome-terminal  -e "python3 worker_2.py" &
gnome-terminal  -e "python3 worker_3.py" 
