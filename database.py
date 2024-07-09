import asyncio
import datetime as dt
import os
import sqlite3
import aiosqlite

from dotenv import load_dotenv

from logger import logger
load_dotenv()

DB_NAME = os.environ["DB_NAME"]


def database_connection() -> aiosqlite.Connection:
    return aiosqlite.connect(DB_NAME, detect_types=sqlite3.PARSE_DECLTYPES)


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


async def insert(title: str, end_date: str, posted_date: str) -> int:
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


async def fetch_one(_id: int) -> tuple[int, str, dt.date, dt.date]:
    logger.info("Get details for id-%d", _id)
    async with database_connection() as db:
        async with db.execute(
            "SELECT id, title, end_date, posted_date FROM jobs "
            f"WHERE id={_id} LIMIT 1;"
        ) as cursor:
            return await cursor.fetchone()


async def fetch_active_jobs() -> list[tuple[int, str, dt.date]]:
    logger.info("Get active jobs")
    async with database_connection() as db:
        result: list[tuple[int, str, dt.date]] = await db.execute_fetchall(
            "SELECT id, title, end_date FROM jobs "
            "WHERE skip=FALSE AND applied=FALSE AND interested=TRUE"
            " AND end_date >= DATE('now', 'localtime');"
        )
        return result


async def fetch_near_deadline_jobs() -> list[tuple[int, str, dt.date]]:
    logger.info("Get jobs that are near end date")
    async with database_connection() as db:
        result: list[tuple[int, str, dt.date]] = await db.execute_fetchall(
            "SELECT id, title, end_date FROM jobs "
            "WHERE skip=FALSE AND applied=FALSE AND interested=TRUE AND "
            "end_date >= DATE('now', 'localtime') AND "
            "(JULIANDAY(end_date) - JULIANDAY('now', 'localtime')) < 2"
        )
        return result


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
