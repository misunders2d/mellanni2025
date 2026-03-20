import asyncio
import random
import time

from fastapi import FastAPI

app = FastAPI()


@app.get("/process/{item_id}")
async def process_data(item_id: int):
    time_to_sleep = random.randint(1, 3)
    await asyncio.sleep(time_to_sleep)
    return {"id": item_id, "status": "completed", "timestamp": time.time()}


@app.post("/process/{item_id}")
async def return_data(item_id: int):
    time_to_sleep = random.randint(1, 3)
    await asyncio.sleep(time_to_sleep)

    return {
        "id": item_id,
        "status": f"You asked for {item_id}",
        "timestamp": time.time(),
    }


@app.get("/hello/{name}")
async def say_hello(name: str):
    """
    returns `name` within "hello, <name>" string
    """
    return f"Hello, {name}"


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("fastapi_server:app", host="127.0.0.1", port=8000, reload=True)
