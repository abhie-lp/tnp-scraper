import asyncio
import os
import sqlite3
import aiosqlite

from collections import namedtuple
from dotenv import load_dotenv

from logger import logger
load_dotenv()

DB_NAME = os.environ["DB_NAME"]

JobDetailShort = namedtuple("JobDetailShort", ("id", "title"))
JobDetailFull = namedtuple("JobDetailFull", ("id", "title", "end_date", "posted_date",
                                             "interested", "applied", "skip"))
StudentDetail = namedtuple("StudentDetail", ("id", "chat_id", "username", "full_name"))


def database_connection() -> aiosqlite.Connection:
    return aiosqlite.connect(DB_NAME, detect_types=sqlite3.PARSE_DECLTYPES)


def job_short_detail_factory(_, row):
    return JobDetailShort(*row)


def job_full_detail_factory(_, row):
    if len(row) == len(JobDetailFull._fields):
        return JobDetailFull(*row)
    else:
        return JobDetailFull(*row, False, False, False)


async def create_table():
    logger.info("Creating table if not exist")
    async with database_connection() as db:
        with open("./schema.sql") as fr:
            await db.executescript(fr.read())


async def insert_job(title: str, uid: str, end_date: str, posted_date: str) -> JobDetailFull:
    logger.info("Insert %s %s %s", title, end_date, posted_date)
    async with database_connection() as db:
        result: tuple[int] = await db.execute_insert(
            "INSERT INTO job(title, uid, end_date, posted_date) VALUES "
            f"('{title}', '{uid}', DATE('{end_date}'), DATE('{posted_date}'));"
        )
        try:
            await db.commit()
            return JobDetailFull(result[0], title, end_date, posted_date,
                                 False, False, False)
        except aiosqlite.Error:
            logger.exception("Something went wrong while inserting")


async def insert_student(chat_id: str | int, username: str, full_name: str) -> StudentDetail:
    async with database_connection() as db:
        result: tuple[int] = await db.execute_insert(
            "INSERT INTO student(chat_id, username, full_name) VALUES "
            f"('{chat_id}', '{username}', '{full_name}')"
        )
        try:
            await db.commit()
            return StudentDetail(result[0], chat_id, username, full_name)
        except aiosqlite.Error:
            logger.exception("Something went wrong while inserting")


async def fetch_one(student_id: int, job_id: int) -> JobDetailFull:
    logger.info("Get details for id-%d", job_id)
    async with database_connection() as db:
        db.row_factory = job_full_detail_factory
        if await job_status_exists(student_id, job_id):
            async with db.execute(
                "SELECT JOB.id, JOB.title, JOB.end_date, JOB.posted_date, "
                "JS.interested, JS.applied, JS.skip "
                f"FROM job JOIN job_status JS ON JS.job_id={job_id} "
                f"WHERE JS.student_id={student_id};"
            ) as cursor:
                return await cursor.fetchone()
        else:
            async with db.execute(
                "SELECT id, title, end_date, posted_date FROM job "
                f"WHERE id={job_id}"
            ) as cursor:
                return await cursor.fetchone()


async def fetch_all_jobs(
    student_id: int, only_interested=False, only_applied=False, only_skip=False
) -> list[JobDetailShort]:
    stmt = ("SELECT JOB.id, JOB.title FROM job JOB "
            "LEFT JOIN job_status JS ON JS.job_id = JOB.id "
            f"AND JS.student_id={student_id} "
            f"WHERE 1=1 ")
    if only_interested:
        logger.info("Get all interested jobs for %d", student_id)
        stmt += " AND JS.interested=TRUE"
    elif only_applied:
        logger.info("Get all applied jobs for %d", student_id)
        stmt += " AND JS.applied=TRUE"
    elif only_skip:
        logger.info("Get all skipped jobs for %d", student_id)
        stmt += " AND JS.skip=TRUE"
    stmt += " ORDER BY JOB.posted_date DESC LIMIT 20;"
    async with database_connection() as db:
        db.row_factory = job_short_detail_factory
        return await db.execute_fetchall(stmt)


async def fetch_active_jobs(
    student_id: int, near_end_date: bool = False
) -> list[JobDetailShort]:
    stmt = ("SELECT JOB.id, JOB.title FROM job JOB "
            "LEFT JOIN job_status JS ON "
            f"(JS.job_id = JOB.id AND JS.student_id={student_id}) "
            "WHERE ((JS.skip=FALSE AND JS.applied=FALSE) OR JS.id IS NULL) "
            "AND JOB.end_date >= DATE('now', 'localtime') ")
    condition2 = ""
    if near_end_date:
        logger.info("Get new end_date jobs")
        condition2 = "(JULIANDAY(JOB.end_date) - JULIANDAY('now', 'localtime')) < 1.2 "
    else:
        logger.info("Get active jobs")
    async with database_connection() as db:
        # Convert the rows to list[namedtuple] instead of list[tuple]
        db.row_factory = job_short_detail_factory
        return await db.execute_fetchall(
            stmt + condition2 + " ORDER BY JOB.posted_date DESC;"
        )


async def update_job_status_field(
    student_id: int, job_id: int, field: str, value: str | bool
) -> None:
    async with database_connection() as db:
        if await job_status_exists(student_id, job_id):
            logger.info("Update job_status field-%s=>%s by %d for job-%d",
                        field, value, student_id, job_id)
            await db.execute(
                f"UPDATE job_status SET {field}={value} "
                f"WHERE student_id={student_id} AND job_id={job_id};"
            )
        else:
            logger.info("Insert job_status field-%s=>%s by %d for job-%d",
                        field, value, student_id, job_id)
            await db.execute(
                f"INSERT INTO job_status(student_id, job_id, {field}) VALUES "
                f"({student_id}, {job_id}, {value})"
            )
        if field == "applied":
            await db.execute(
                "UPDATE job_status SET applied_on=DATETIME('now', 'localtime') "
                f"WHERE student_id={student_id} AND job_id={job_id};"
            )
        try:
            await db.commit()
        except aiosqlite.Error:
            logger.exception("Error while updating field-%s=>%s by %d for job-%d",
                             field, value, student_id, job_id)


async def update_student_field(chat_id: str | int, field: str, value: bool) -> None:
    async with database_connection() as db:
        await db.execute(f"UPDATE student SET {field}={int(value)} "
                         f"WHERE chat_id='{chat_id}';")
        try:
            await db.commit()
        except aiosqlite.Error:
            logger.exception("Error while updating field-notify for chat_id=%s to %s",
                             chat_id, value)


async def job_exists(uid: str) -> bool:
    async with database_connection() as db:
        result = await db.execute(
            f"SELECT EXISTS(SELECT 1 FROM job WHERE uid='{uid}' LIMIT 1);"
        )
        return (await result.fetchone())[0] == 1


async def student_exists(chat_id: str | int) -> bool:
    async with database_connection() as db:
        result = await db.execute(
            f"SELECT EXISTS(SELECT 1 FROM student WHERE chat_id='{chat_id}')"
        )
        return (await result.fetchone())[0] == 1


async def student_is_notified(chat_id: str | int) -> bool:
    async with database_connection() as db:
        result: aiosqlite.Cursor = await db.execute(
            f"SELECT notify FROM student WHERE chat_id='{chat_id}' LIMIT 1;"
        )
        if status := await result.fetchone():
            return status[0] == 1
        return False


async def student_is_registered(chat_id: str | int) -> bool:
    async with database_connection() as db:
        result: aiosqlite.Cursor = await db.execute(
            f"SELECT register FROM student WHERE chat_id='{chat_id}' LIMIT 1;"
        )
        if status := await result.fetchone():
            return status[0] == 1
        return False


async def job_status_exists(student_id: int, job_id: int) -> bool:
    async with database_connection() as db:
        result = await db.execute(
            "SELECT EXISTS(SELECT 1 FROM job_status "
            f"WHERE student_id={student_id} AND job_id={job_id} LIMIT 1);"
        )
        return (await result.fetchone())[0] == 1


if __name__ == "__main__":
    asyncio.run(create_table())
