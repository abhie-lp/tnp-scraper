import asyncio
import httpx
import os

from collections import namedtuple
from dotenv import load_dotenv
from lxml import html
from typing import Generator

from database import job_exists, insert_job
from logger import logger

load_dotenv()


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

Job = namedtuple("Job", ("title", "end_date", "posted_date"))


async def save_new_jobs() -> None:
    logger.info("Get and save new jobs")
    async for job in extract_job_details():
        if not await job_exists(job.title, job.end_date, job.posted_date):
            await insert_job(job.title, job.end_date, job.posted_date)


async def extract_job_details() -> Generator[Job, None, None]:
    async with httpx.AsyncClient(headers=HEADERS) as client:
        logger.info("GET %s", LOGIN_GET_URL)
        await client.get(LOGIN_GET_URL)
        logger.info("POST %s", LOGIN_POST_URL)
        await client.post(LOGIN_POST_URL, data=PAYLOAD, follow_redirects=True)
        await asyncio.sleep(2)

        logger.info("GET %s", JOBS_URL)
        jobs_page = await client.get(JOBS_URL)

        logger.info("Begin extraction")
        html_root = html.fromstring(jobs_page.text)
        jobs_tbody_ele = html_root.find(".//table[@id='job-listings']/tbody")
        for i, job in enumerate(jobs_tbody_ele.findall(".//tr"), start=1):
            title, end_date, posted_date, *_ = job.findall(".//td")
            # Reformat dates as YYYY-MM-DD
            end_date = "-".join(end_date.text.strip().split("/")[::-1])
            posted_date = "-".join(posted_date.text.strip().split("/")[::-1])
            yield Job(title.text.strip(), end_date, posted_date)
        logger.info("Done extraction")

        await asyncio.sleep(2)
        logger.info("GET %s", LOGOUT_URL)
        await client.get(LOGOUT_URL)
        logger.info("BYE BYE!!")


if __name__ == "__main__":
    asyncio.run(save_new_jobs())
