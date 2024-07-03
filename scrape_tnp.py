import logging
import httpx
import os

from dotenv import load_dotenv
from lxml import html
from typing import Generator

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


def extract_details() -> Generator[str, None, None]:
    with httpx.Client(headers=HEADERS) as client:
        logger.info("GET %s", LOGIN_GET_URL)
        client.get(LOGIN_GET_URL)
        logger.info("POST %s", LOGIN_POST_URL)
        client.post(LOGIN_POST_URL, data=PAYLOAD, follow_redirects=True)

        logger.info("GET %s", JOBS_URL)
        jobs_page = client.get(JOBS_URL)
        logger.info("BEGIN extraction")
        html_root = html.fromstring(jobs_page.text)

        jobs_tbody_ele = html_root.find(".//table[@id='job-listings']/tbody")
        for i, job in enumerate(jobs_tbody_ele.findall(".//tr"), start=1):
            title, end_date, posted_date, _ = job.findall(".//td")
            yield (f"{i}: {title.text}\n\tEnd Date:{end_date.text}\n\t"
                   f"Posted Date:{posted_date.text}")
        logger.info("GET %s", LOGOUT_URL)
        client.get(LOGOUT_URL)
        logger.info("BYE BYE!!")


if __name__ == "__main__":
    for i in extract_details():
        print(i)
