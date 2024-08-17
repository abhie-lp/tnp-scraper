import os
from dotenv import load_dotenv

load_dotenv()

MY_CHAT_ID = int(os.environ["CHAT_ID"])
DB_NAME = os.environ["DB_NAME"]
