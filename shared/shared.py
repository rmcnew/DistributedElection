""" Shared utility functions """
import socket
import datetime


def get_ip_address():
    temp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        temp_socket.connect(('192.0.0.8', 1027))
    except socket.error:
        return None
    return temp_socket.getsockname()[0]

def timestamp():
    return timestamp.timestamp.now().isoformat()

def get_elapsed_time(start_time):
    current_time = datetime.now()
    elapsed_time = current_time - start_time
    return elapsed_time
