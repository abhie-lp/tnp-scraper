import asyncio
import database as db
import datetime as dt
import logging
import logger
import os
import sqlite3
import unittest

logger.logger.setLevel(logging.WARNING)
db.DB_NAME = "test.db"


async def list_of_table_columns(table: str) -> list[str]:
    async with db.database_connection() as con:
        con.row_factory = lambda _, row: row[0]
        return await con.execute_fetchall(
            "WITH all_tables AS (SELECT name FROM sqlite_master "
            f"WHERE type = 'table' AND name='{table}') "
            "SELECT pti.name FROM all_tables at "
            "INNER JOIN pragma_table_info(at.name) pti;"
        )


class DefaultTestCase(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls):
        asyncio.run(db.create_table())

    async def asyncTearDown(self):
        async with db.database_connection() as con:
            for table in ("job", "job_status", "student"):
                await con.executescript(
                    f"DELETE FROM {table};"
                    f"DELETE FROM SQLITE_SEQUENCE WHERE name='{table}';"
                )

    @classmethod
    def tearDownClass(cls):
        os.remove(db.DB_NAME)

    async def test_database_exists(self):
        self.assertEqual(os.path.exists(db.DB_NAME), True, "Database not created")


class DatabaseTestCase(DefaultTestCase):
    async def test_all_tables_exist(self):
        tables = {"job", "student", "job_status"}
        async with db.database_connection() as con:
            con.row_factory = lambda _, row: row[0]
            result = set(await con.execute_fetchall(
                "SELECT name FROM sqlite_master WHERE type='table';"
            ))
            result.remove("sqlite_sequence")
        self.assertSetEqual(tables, set(result))


class JobTableTestCase(DefaultTestCase):
    async def test_job_table_contains_equal_fields(self):
        fields = {"id", "title", "uid", "end_date", "posted_date", "created_at"}
        self.assertSetEqual(fields, set(await list_of_table_columns('job')),
                            "Set of fields do not match for job table")

    async def test_insert_job_returns_correct_value(self):
        title, uid = "Test", "abcd1234"
        end_date, posted_date = (str(dt.date.today()),
                                 str(dt.date.today() + dt.timedelta(days=1)))
        result: db.JobDetailFull = await db.insert_job(title, uid, end_date, posted_date)
        self.assertIsInstance(result, db.JobDetailFull)
        self.assertEqual(result.id, 1)
        self.assertEqual(result.title, title)
        self.assertEqual(result.end_date, end_date)
        self.assertEqual(result.posted_date, posted_date)
        self.assertEqual(result.interested, False)
        self.assertEqual(result.applied, False)
        self.assertEqual(result.skip, False)

    async def test_insert_job_inserts_correct_data_in_db(self):
        title, uid = "Test", "abcd1234"
        end_date, posted_date = dt.date.today(), dt.date.today()
        await db.insert_job(title, uid, end_date, posted_date)
        async with db.database_connection() as con:
            con.row_factory = sqlite3.Row
            async with con.execute("SELECT * FROM job;") as cursor:
                result = await cursor.fetchone()
        self.assertEqual(result["id"], 1)
        self.assertEqual(result["title"], title)
        self.assertEqual(result["uid"], uid)
        self.assertEqual(result["end_date"], end_date)
        self.assertEqual(result["posted_date"], posted_date)
        self.assertTrue(result["created_at"])

    async def test_table_job_raises_error_on_duplicate_uid(self):
        uid = "abcd124"
        await db.insert_job("TEST", uid, dt.date.today(), dt.date.today())
        with self.assertRaises(sqlite3.IntegrityError):
            await db.insert_job("TEST", uid,
                                str(dt.date.today()), str(dt.date.today()))


class StudentTableTestCase(DefaultTestCase):
    async def test_student_table_contains_equal_fields(self):
        fields = {"id", "chat_id", "username", "full_name", "notify", "created_at"}
        self.assertSetEqual(fields, set(await list_of_table_columns('student')),
                            "Set of fields do not match for student table")


class JobStatusTableTestCase(DefaultTestCase):
    async def test_job_status_table_contains_equal_fields(self):
        fields = {"id", "student_id", "job_id", "interested", "applied", "skip",
                  "applied_on", "created_at"}
        self.assertSetEqual(fields, set(await list_of_table_columns('job_status')),
                            "Set of fields do not match for job_status table")


if __name__ == "__main__":
    unittest.main()
