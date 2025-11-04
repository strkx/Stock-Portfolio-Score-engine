# app/config.py
import os
from dotenv import load_dotenv


load_dotenv()

class Settings:
    DHAN_API_KEY: str = os.getenv("DHAN_API_KEY", "")
    PPLX_API_KEY: str = os.getenv("PPLX_API_KEY", "")


settings = Settings()
