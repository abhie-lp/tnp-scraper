import asyncio
import os
import sqlite3
import aiosqlite

from collections import namedtuple
from dotenv import load_dotenv

from logger import logger
load_dotenv()

DB_NAME = os.environ["DB_NAME"]

JobDetailShort = namedtuple("JobDetailShort", ("id", "title", "end_date"))
JobDetailFull = namedtuple("JobDetailFull",
                           ("id", "title", "end_date", "posted_date"))


def database_connection() -> aiosqlite.Connection:
    return aiosqlite.connect(DB_NAME, detect_types=sqlite3.PARSE_DECLTYPES)


def job_short_detail_factory(cursor, row):
    return JobDetailShort(*row)


async def create_table():
    logger.info("Creating table if not exist")
    async with database_connection() as db:
        await db.execute("""CREATE TABLE IF NOT EXISTS jobs(
  id INTEGER NOT NULL PRIMARY KEY,
  title VARCHAR(255) NOT NULL,
  end_date DATE,                    -- YYYY-MM-DD
  posted_date DATE,                 -- YYYY-MM-DD
  interested BOOLEAN DEFAULT FALSE,
  applied BOOLEAN DEFAULT FALSE,
  skip BOOLEAN DEFAULT FALSE,
  applied_on DATETIME,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);
""")


async def insert_job(title: str, end_date: str, posted_date: str) -> int:
    logger.info("Insert %s %s %s", title, end_date, posted_date)
    async with database_connection() as db:
        result: tuple[int] = await db.execute_insert(
            "INSERT INTO jobs(title, end_date, posted_date) VALUES "
            f"('{title}', DATE('{end_date}'), DATE('{posted_date}'));"
        )
        try:
            await db.commit()
            return result[0]
        except aiosqlite.Error:
            logger.exception("Something went wrong while inserting")


async def fetch_one(_id: int) -> JobDetailFull:
    logger.info("Get details for id-%d", _id)
    async with database_connection() as db:
        async with db.execute(
            "SELECT id, title, end_date, posted_date FROM jobs "
            f"WHERE id={_id} LIMIT 1;"
        ) as cursor:
            return JobDetailFull(*await cursor.fetchone())


async def fetch_all_jobs(only_interested=False, only_applied=False,
                         only_skip=False) -> list[JobDetailShort]:
    async with database_connection() as db:
        db.row_factory = job_short_detail_factory
        stmt = "SELECT id, title, end_date FROM jobs"
        if only_interested:
            logger.info("Get all interested jobs")
            stmt += " WHERE interested=TRUE"
        elif only_applied:
            logger.info("Get all applied jobs")
            stmt += " WHERE applied=TRUE"
        elif only_skip:
            logger.info("Get all skipped jobs")
            stmt += " WHERE skip=TRUE"
        stmt += " LIMIT 20;"
        return await db.execute_fetchall(stmt)


async def fetch_active_jobs() -> list[JobDetailShort]:
    logger.info("Get active jobs")
    async with database_connection() as db:
        # Convert the rows to list[namedtuple] instead of list[tuple]
        db.row_factory = job_short_detail_factory
        return await db.execute_fetchall(
            "SELECT id, title, end_date FROM jobs "
            "WHERE skip=FALSE AND applied=FALSE "
            "AND end_date >= DATE('now', 'localtime');"
        )


async def fetch_near_deadline_jobs() -> list[JobDetailShort]:
    logger.info("Get jobs that are near end date")
    async with database_connection() as db:
        return await db.execute_fetchall(
            "SELECT id, title, end_date FROM jobs "
            "WHERE skip=FALSE AND applied=FALSE AND "
            "end_date >= DATE('now', 'localtime') AND "
            "(JULIANDAY(end_date) - JULIANDAY('now', 'localtime')) < 2"
        )


async def update_field_to_true(_id: int, field: str) -> None:
    logger.info("Update field - %s for id - %d", field, _id)
    async with database_connection() as db:
        await db.execute(f"UPDATE jobs SET {field}=TRUE WHERE id={_id};")
        if field == "applied":
            await db.execute(
                "UPDATE jobs SET applied_on=DATETIME('now', 'localtime') "
                f"WHERE id={_id};"
            )
        try:
            await db.commit()
        except aiosqlite.Error:
            logger.exception("Error while updating field - %s for id - %d",
                             field, _id)


async def job_exists(title: str, end_date: str, posted_date: str) -> bool:
    async with database_connection() as db:
        result = await db.execute(
            f"SELECT EXISTS(SELECT 1 FROM jobs WHERE title='{title}' AND "
            f"end_date='{end_date}' AND posted_date='{posted_date}' LIMIT 1);"
        )
        return (await result.fetchone())[0] == 1


if __name__ == "__main__":
    asyncio.run(create_table())
