from typing import Annotated
from utils import (
    create_virtual_table,
    fuzzy_group,
    get_table_headers,
)
from fastapi import FastAPI, File
from fastapi.middleware.cors import CORSMiddleware


app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.post("/generate")
async def generate(files: Annotated[list[bytes], File()]):
    create_virtual_table(files)
    return {"message": "Database has been successfully generated"}


@app.get("/headers")
async def headers():
    return {"headers": get_table_headers()}


@app.get("/groups")
async def groups():
    return fuzzy_group()
