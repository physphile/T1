import uvicorn
from base import app

if __name__ == "__main__":
    uvicorn.run("base:app", reload=True)
