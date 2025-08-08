import requests
import logging

def invio_http(message, config):
    try:
        url = config.get('url')
        headers ={'Content-Type': 'text/plain'}
        response = requests.post(url, data=message, headers=headers)
        logging.info(f"HTTP POST to {url} successful: {response.status_code}")
    except Exception as e:
        logging.error(f"HTTP POST to {url} failed: {e}")


            