import datetime as dt
import os
from functools import wraps, lru_cache
from operator import attrgetter

import database as db

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, ContextTypes, CommandHandler, CallbackQueryHandler
from telegram.constants import ParseMode

from constants import MY_CHAT_ID
from helpers import job_id
from logger import logger
from scraper import get_and_save_new_jobs

SCRAPER_RUNNING = False


def restricted(func):
    @wraps(func)
    async def wrapped(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        chat_id = update.effective_user.id
        if chat_id != MY_CHAT_ID:
            logger.info(
                "Unauthorized access for %s-%s",
                update.effective_user.full_name, chat_id
            )
            await update.message.reply_text("You are not authorized to run this command.",
                                            quote=True)
            return
        return await func(update, ctx)

    return wrapped


def is_registered(func):
    @wraps(func)
    async def wrapped(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        chat_id = update.effective_user.id
        if not await db.student_is_registered(chat_id):
            logger.info("Non registered user %s-%s",
                        update.effective_user.full_name, chat_id)
            await update.message.reply_text(
                "You have not registered. Please send /register once "
                "and try again.", quote=True
            )
            return
        return await func(update, ctx)

    return wrapped


@lru_cache(4)
def start_text(registered: bool = False, notified: bool = False):
    # Return text depending upon the status of register and notify fields.
    if registered:
        handle = "/unnotify" if notified else "/notify"
        return ("/start: Get a list of commands.\n"
                "/active: Get a list of active jobs that are not applied or skipped.\n"
                "/all [interested|applied|skipped]: Get list of all jobs.\n"
                "/latest: Run the scraper and get latest data.\n"
                f"{handle}: Give alerts related to jobs.\n"
                "/unregister: Unregister")
    else:
        return ("/start: Get a list of commands.\n"
                "/register: Get info about T&P jobs.")


def jobs_inline_layout(jobs: list) -> list[tuple[InlineKeyboardButton]]:
    jobs_inline_button: list[tuple[InlineKeyboardButton]] = []
    for job in jobs:
        jobs_inline_button.append((
            InlineKeyboardButton(job.title, callback_data=f"JOB_{job.id}"),
        ))
    return jobs_inline_button


async def task_notify_active_jobs(ctx: ContextTypes.DEFAULT_TYPE):
    logger.info("Scheduled task to get new data")
    for student in await db.students_to_notify():
        if jobs_layout := jobs_inline_layout(await db.fetch_active_jobs(student.id)):
            logger.info("Send active jobs notification")
            await ctx.bot.send_message(
                student.chat_id, "Active jobs that are not applied.",
                reply_markup=InlineKeyboardMarkup(jobs_layout)
            )


async def task_get_latest_data(ctx: ContextTypes.DEFAULT_TYPE):
    global SCRAPER_RUNNING
    if not SCRAPER_RUNNING:
        SCRAPER_RUNNING = True
        logger.info("Run task to get new data from site.")
        await ctx.bot.send_message(
            MY_CHAT_ID, "Started the scraper to get latest data."
        )
        if new_jobs := await get_and_save_new_jobs():
            await ctx.bot.send_message(
                MY_CHAT_ID, "New jobs posted",
                reply_markup=InlineKeyboardMarkup(jobs_inline_layout(new_jobs))
            )
        elif ctx.job.data == "force":
            await ctx.bot.send_message(MY_CHAT_ID, "No new job posted.")
        SCRAPER_RUNNING = False


async def task_near_end_date_jobs(ctx: ContextTypes.DEFAULT_TYPE):
    logger.info("Run task to get jobs reaching end_date")
    if ctx.job.data == "force":
        student = await db.fetch_one_student(ctx.job.chat_id)
        if target_jobs := await db.fetch_active_jobs(student.id, True):
            await ctx.bot.send_message(
                student.chat_id, "Take action on below pending jobs reaching end_date.",
                reply_markup=InlineKeyboardMarkup(jobs_inline_layout(target_jobs))
            )
        else:
            await ctx.bot.send_message(student.chat_id,
                                       "There are not jobs nearing to end_date.")
    else:
        for student in await db.students_to_notify():
            if target_jobs := await db.fetch_active_jobs(student.id, True):
                await ctx.bot.send_message(
                    student.chat_id,
                    "Take action on below pending jobs reaching end_date.",
                    reply_markup=InlineKeyboardMarkup(jobs_inline_layout(target_jobs))
                )


async def start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat_id, full_name = update.effective_user.id, update.effective_user.full_name
    logger.info("Start %s-%s", full_name, chat_id)
    await update.message.reply_text(
        f"Hi {full_name}!! Please use the below commands to interact.\n" +
        start_text(await db.student_is_registered(chat_id),
                   await db.student_is_notified(chat_id))
    )


@is_registered
async def handler_active_jobs(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    logger.info("Get interested jobs")
    if jobs_layout := jobs_inline_layout(await db.fetch_active_jobs(update.effective_user.id)):
        await update.message.reply_text("Here is the list of active jobs",
                                        reply_markup=InlineKeyboardMarkup(jobs_layout))
    else:
        await update.message.reply_text("No active jobs.")


@is_registered
async def handler_all_jobs(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_user.id
    arg = ctx.args[0].lower() if ctx.args else "all"
    logger.info("Get %s %s jobs", chat_id, arg)
    only_interested, only_applied, only_skip = False, False, False
    text = "List of all the jobs"
    if arg == "interested":
        only_interested = True
        text = "List of all the interested jobs"
    elif arg == "applied":
        only_applied = True
        text = "List of the applied jobs"
    elif arg == "skip":
        only_skip = True
        text = "List of the skipped jobs"
    await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(
        jobs_inline_layout(await db.fetch_all_jobs(
            chat_id, only_interested, only_applied, only_skip
        ))
    ))


@is_registered
async def handler_update_job_field(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()  # Required

    field = query.data
    logger.info("Update field %s", field)
    job = await db.fetch_one_job(update.effective_user.id, job_id(field))
    if field.startswith("INT"):
        await db.update_job_status_field(update.effective_user.id, job.id,
                                         "interested", not job.interested)
        text = "not interested" if job.interested else "interested"
    elif field.startswith("APP"):
        await db.update_job_status_field(update.effective_user.id, job.id,
                                         "applied", not job.applied)
        text = "revoked" if job.applied else "applied"
    else:
        await db.update_job_status_field(update.effective_user.id, job.id,
                                         "skip", not job.skip)
        text = "unskip" if job.skip else "skip"
    await query.edit_message_text(f"Thank! Job {job.title} is marked as {text}.")


@is_registered
async def handler_job_details(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()  # Required
    logger.info("Get job info %s-%s", query.from_user.id, query.data)

    job = await db.fetch_one_job(update.effective_user.id, job_id(query.data))
    interested = "Not Interested" if job.interested else "Interested"
    applied = "Revoked" if job.applied else "Applied"
    skip = "Unskip" if job.skip else "Skip"
    await query.edit_message_text(
        f"*{job.title}*\n    End Date: {job.end_date}\n    "
        f"Posted Date: {job.posted_date}",
        reply_markup=InlineKeyboardMarkup((
            (
                InlineKeyboardButton(interested, callback_data=f"INT_{job.id}"),
            ),
            (
                InlineKeyboardButton(applied, callback_data=f"APP_{job.id}"),
                InlineKeyboardButton(skip, callback_data=f"SKIP_{job.id}")
            )
        )),
        parse_mode=ParseMode.MARKDOWN
    )


@is_registered
async def handler_notify(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_user.id
    logger.info("Notify %s-%s", update.effective_user.full_name, chat_id)
    if await db.student_is_notified(chat_id) is False:
        await db.update_student_field(chat_id, "notify", True)
        await update.message.reply_text("You'll receive notification regarding "
                                        "job from now on. Send /unnotify to stop.")
    else:
        await update.message.reply_text("You are already getting notifications. "
                                        "Send /unnotify to stop.")


@restricted
async def handler_get_latest(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not SCRAPER_RUNNING:
        logger.info("Force run the scraper now to get latest data")
        ctx.job_queue.run_once(task_get_latest_data, .2, data="force")
    else:
        await update.message.reply_text("Previous scraper still running. Please wait")


@is_registered
async def handler_get_near_end_date_jobs(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    logger.info("Get near end jobs %s", update.effective_user.id)
    ctx.job_queue.run_once(task_near_end_date_jobs, .2, data="force",
                           chat_id=update.effective_user.id)


async def handler_register(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat_id, username, full_name = (attrgetter("id", "username", "full_name")
                                    (update.effective_user))
    logger.info("Register user %s-%s-%s", chat_id, username, full_name)
    if await db.student_is_registered(chat_id) is False:
        if not await db.student_exists(chat_id):
            if await db.insert_student(chat_id, username, full_name) is None:
                await update.message.reply_text("Oh no! Something went wrong. "
                                                "Please try again.")
                return
        await db.update_student_field(chat_id, "register", True)
        await update.message.reply_text(
            f"Thanks {full_name}! You have been registered. "
            "To unregister send /unregister"
        )
    else:
        await update.message.reply_text("You are already registered. "
                                        "Send /unregister to unregister.")


@is_registered
async def handler_unnotify(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_user.id
    logger.info("Unnotify %s-%s", update.effective_user.full_name, chat_id)
    if await db.student_is_notified(chat_id) is True:
        await db.update_student_field(chat_id, "notify", False)
        await update.message.reply_text("You will not receive notifications. "
                                        "Send /notify to get notifications.")
    else:
        await update.message.reply_text("You are already not receiving notifications. "
                                        "Send /notify to get notifications.")


async def handler_unregister(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_user.id
    logger.info("Unregister %s-%s", update.effective_user.full_name, chat_id)
    if await db.student_is_registered(chat_id) is True:
        await db.update_student_field(chat_id, "register", False)
        await update.message.reply_text("You have been unregistered. "
                                        "Send /register to register again.")
    else:
        await update.message.reply_text("You are already unregistered. "
                                        "Send /register to register again.")


def main():
    application = Application.builder().token(os.environ["TOKEN"]).concurrent_updates(True).build()
    application.job_queue.run_repeating(task_notify_active_jobs, 200, first=1)
    application.job_queue.run_repeating(task_get_latest_data, 4 * 60 * 60, first=1)
    application.job_queue.run_daily(task_near_end_date_jobs, dt.time(8, 0))
    application.job_queue.run_daily(task_near_end_date_jobs, dt.time(19, 0))

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("active", handler_active_jobs))
    application.add_handler(CallbackQueryHandler(handler_job_details, r"^JOB_\d+"))
    application.add_handler(CallbackQueryHandler(handler_update_job_field,
                                                 r"^(INT|APP|SKIP)_\d+"))
    application.add_handler(CommandHandler("all", handler_all_jobs))
    application.add_handler(CommandHandler("end_date", handler_get_near_end_date_jobs))
    application.add_handler(CommandHandler("latest", handler_get_latest))
    application.add_handler(CommandHandler("notify", handler_notify))
    application.add_handler(CommandHandler("register", handler_register))
    application.add_handler(CommandHandler("unnotify", handler_unnotify))
    application.add_handler(CommandHandler("unregister", handler_unregister))

    logger.info("Run the application")
    application.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
