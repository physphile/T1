from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi_sqlalchemy import DBSessionMiddleware
from routes import root
from settings import get_settings

app = FastAPI()
settings = get_settings()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.add_middleware(
    DBSessionMiddleware,
    db_url=str(settings.DB_DSN),
    engine_args={"pool_pre_ping": True, "isolation_level": "AUTOCOMMIT"},
)

app.include_router(root, prefix="", tags=["Root"])
