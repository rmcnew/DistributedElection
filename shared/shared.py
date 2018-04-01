""" Shared utility functions """
import socket


def get_ip_address():
    temp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        temp_socket.connect(('192.0.0.8', 1027))
    except socket.error:
        return None
    return temp_socket.getsockname()[0]
