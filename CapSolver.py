# pip install requests
import requests
import time
import logging
import logging.config
import json
import pathlib

# TODO: set your config
# api_key = "YOUR_API_KEY"  # your api key of capsolver
# site_key = "6Le-wvkSAAAAAPBMRTvw0Q4Muexq9bi0DJwx_mJ-"  # site key of your target site
# site_url = "https://www.google.com/recaptcha/api2/demo"  # page url of your target site


def capsolver(api_key, site_key, site_url):
    # Controlador.setup_logging()
    # logger = logging.getLogger(__name__)

    payload = {
        "clientKey": api_key,
        "task": {
            "type": 'ReCaptchaV2TaskProxyLess',
            "websiteKey": site_key,
            "websiteURL": site_url
        }
    }
    res = requests.post("https://api.capsolver.com/createTask", json=payload)
    resp = res.json()
    task_id = resp.get("taskId")
    if not task_id:
        print("Failed to create task:", res.text)
        # logger.error("Failed to create task:", res.text)
        return
    print(f"Got taskId: {task_id} / Getting result...")
    # logger.info(f"Got taskId: {task_id} / Getting result...")

    while True:
        time.sleep(5)  # delay
        payload = {"clientKey": api_key, "taskId": task_id}
        res = requests.post("https://api.capsolver.com/getTaskResult", json=payload)
        resp = res.json()
        status = resp.get("status")
        if status == "ready":
            # logger.info(f"Recaptcha solved")
            return resp.get("solution", {}).get('gRecaptchaResponse')
        if status == "failed" or resp.get("errorId"):
            print("Solve failed! response:", res.text)
            # logger.error("Solve failed! response:", res.text)
            return
