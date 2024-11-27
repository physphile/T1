from utils import (
    create_virtual_table,
    fuzzy_group,
    get_table_headers,
)
from typing import Annotated
from fastapi import APIRouter, File
from fastapi_pagination import add_pagination

root = APIRouter()
add_pagination(root)

@root.post("/generate")
async def generate(files: Annotated[list[bytes], File()]):
    create_virtual_table(files)
    return {"message": "Database has been successfully generated"}


@root.get("/headers")
async def headers():
    return {"headers": get_table_headers()}


@root.get("/groups")
async def groups(reference_columns: list[str]):
    return fuzzy_group(reference_columns)