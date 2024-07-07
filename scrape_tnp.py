import logging
import httpx
import os
import schedule
import time

from dotenv import load_dotenv
from lxml import html
from typing import Generator, Iterator

load_dotenv()

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter(
    "%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
))
logger.addHandler(handler)

HEADERS = {
    "User-Agent": ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"),
}
BASE_URL = os.environ["URL"]
LOGIN_GET_URL, LOGIN_POST_URL, LOGOUT_URL = (
    f"{BASE_URL}/login.html", f"{BASE_URL}/auth/login.html",
    f"{BASE_URL}/logout.html"
)
INDEX_URL, JOBS_URL = f"{BASE_URL}/index.html", f"{BASE_URL}/applyjobs.html"
PAYLOAD = {"identity": os.environ["USERNAME"],
           "password": os.environ["PASSWORD"],
           "submit": "Login", "txtcentrenm": ""}
CHAT_ID, API_KEY = os.environ["CHAT_ID"], os.environ["TOKEN"]
TG_HEADERS = {'Content-Type': 'application/json', 'Proxy-Authorization': 'Basic base64'}


def send_notification(jobs: Iterator):
    payload = {"chat_id": CHAT_ID,
               "text": "🚨🚨🚨 ALERT 🚨🚨🚨\n\n" + "\n".join(jobs),
               "parse_mode": "HTML",
               "disable_notification": True}
    resp = httpx.post(f'https://api.telegram.org/bot{API_KEY}/sendMessage',
                      json=payload, headers=TG_HEADERS)
    if resp.status_code != 200:
        logger.error("Something wrong in Telegram API")


def extract_details() -> Generator[str, None, None]:
    with httpx.Client(headers=HEADERS) as client:
        logger.info("GET %s", LOGIN_GET_URL)
        client.get(LOGIN_GET_URL)
        logger.info("POST %s", LOGIN_POST_URL)
        client.post(LOGIN_POST_URL, data=PAYLOAD, follow_redirects=True)
        time.sleep(1)

        logger.info("GET %s", JOBS_URL)
        jobs_page = client.get(JOBS_URL)
        logger.info("BEGIN extraction")
        html_root = html.fromstring(jobs_page.text)

        jobs_tbody_ele = html_root.find(".//table[@id='job-listings']/tbody")
        for i, job in enumerate(jobs_tbody_ele.findall(".//tr"), start=1):
            title, end_date, posted_date, _ = job.findall(".//td")
            yield (f"{i}: {title.text}\n      End Date:{end_date.text}\n      "
                   f"Posted Date:{posted_date.text}")
        logger.info("GET %s", LOGOUT_URL)
        client.get(LOGOUT_URL)
        time.sleep(1)
        logger.info("BYE BYE!!")


def main():
    try:
        send_notification(extract_details())
    except Exception:
        logger.exception("Something went wrong.")


if __name__ == "__main__":
    schedule.every(2).hours.do(main).run()
    while True:
        try:
            schedule.run_pending()
            time.sleep(5)
        except KeyboardInterrupt:
            break
