import asyncio
import datetime as dt
import database as db
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
    CHAT_ID, USERNAME, FULL_NAME = "23234", "username", "full_name"

    async def test_student_table_contains_equal_fields(self):
        fields = {"id", "chat_id", "username", "full_name", "notify", "created_at",
                  "register", "last_active"}
        self.assertSetEqual(fields, set(await list_of_table_columns('student')),
                            "Set of fields do not match for student table")

    async def test_insert_student_returns_correct_value(self):
        result = await db.insert_student(self.CHAT_ID, self.USERNAME, self.FULL_NAME)
        self.assertIsInstance(result, db.StudentDetail)
        self.assertEqual(result.id, 1)
        self.assertEqual(result.chat_id, self.CHAT_ID)
        self.assertEqual(result.username, self.USERNAME)
        self.assertEqual(result.full_name, self.FULL_NAME)

    async def test_default_values_of_new_entries(self):
        result = await db.insert_student(self.CHAT_ID, self.USERNAME, self.FULL_NAME)
        async with db.database_connection() as con:
            result = await con.execute(f"SELECT notify, register FROM student WHERE id={result.id}")
            notify, register = await result.fetchone()
            self.assertFalse(notify, "Student notify field is not False by default")
            self.assertFalse(register, "Student register field is not False by default")

    async def test_chat_id_field_is_unique_and_raises_error(self):
        await db.insert_student(self.CHAT_ID, "username1", "full_name1")
        with self.assertRaises(sqlite3.IntegrityError):
            await db.insert_student(self.CHAT_ID, "username2", "full_name2")

    async def test_username_field_is_unique_and_raises_error(self):
        await db.insert_student("1234", self.USERNAME, "full_name1")
        with self.assertRaises(sqlite3.IntegrityError):
            await db.insert_student("4567", self.USERNAME, "full_name2")

    async def test_check_student_exists_returns_true_if_exists(self):
        self.assertFalse(await db.student_exists(self.CHAT_ID), "Expected False got True")
        await db.insert_student(self.CHAT_ID, self.USERNAME, self.FULL_NAME)
        self.assertTrue(await db.student_exists(self.CHAT_ID), "Expected True got False")

    async def test_update_student_field_updates_field(self):
        await db.insert_student(self.CHAT_ID, self.USERNAME, self.FULL_NAME)
        await db.update_student_field(self.CHAT_ID, "notify", True)
        async with db.database_connection() as con:
            result = await con.execute(f"SELECT notify FROM student WHERE chat_id='{self.CHAT_ID}';")
            self.assertTrue((await result.fetchone())[0])
        await db.update_student_field(self.CHAT_ID, "notify", False)
        async with db.database_connection() as con:
            result = await con.execute(f"SELECT notify FROM student WHERE chat_id='{self.CHAT_ID}';")
            self.assertFalse((await result.fetchone())[0])
        await db.update_student_field(self.CHAT_ID, "register", True)
        async with db.database_connection() as con:
            result = await con.execute(f"SELECT register FROM student WHERE chat_id='{self.CHAT_ID}';")
            self.assertTrue((await result.fetchone())[0])
        await db.update_student_field(self.CHAT_ID, "register", False)
        async with db.database_connection() as con:
            result = await con.execute(f"SELECT register FROM student WHERE chat_id='{self.CHAT_ID}';")
            self.assertFalse((await result.fetchone())[0])

    async def test_student_is_notified_returns_true_if_marked_true(self):
        await db.insert_student(self.CHAT_ID, self.USERNAME, self.FULL_NAME)
        self.assertFalse(await db.student_is_notified(self.CHAT_ID),
                         "notify should be False but marked as True")
        await db.update_student_field(self.CHAT_ID, "notify", True)
        self.assertTrue(await db.student_is_notified(self.CHAT_ID),
                        "notify should be True but marked as False")

    async def test_student_is_registered_returns_true_if_marked_true(self):
        await db.insert_student(self.CHAT_ID, self.USERNAME, self.FULL_NAME)
        self.assertFalse(await db.student_is_registered(self.CHAT_ID),
                         "Expected False got True")
        await db.update_student_field(self.CHAT_ID, "register", True)
        self.assertTrue(await db.student_is_registered(self.CHAT_ID),
                        "Expected True got False")


class JobStatusTableTestCase(DefaultTestCase):
    async def test_job_status_table_contains_equal_fields(self):
        fields = {"id", "student_id", "job_id", "interested", "applied", "skip",
                  "applied_on", "created_at"}
        self.assertSetEqual(fields, set(await list_of_table_columns('job_status')),
                            "Set of fields do not match for job_status table")


if __name__ == "__main__":
    unittest.main()
