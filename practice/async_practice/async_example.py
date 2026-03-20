import asyncio

import httpx


async def fetch_item(item_id):
    print(f"Client side message: Starting request for item {item_id}...")
    async with httpx.AsyncClient(timeout=30.0) as client:
        response = await client.get(f"http://127.0.0.1:8000/process/{item_id}")
        data = response.json()
        print(data)

        print("Waiting for 5 seconds")
        await asyncio.sleep(5)
        print(f"Finished item {item_id} at {data['timestamp']}")
    return data


async def fetch_hello(name):
    print(f"Starting to fetch hello for {name}")
    async with httpx.AsyncClient() as client:
        response = await client.get(f"http://127.0.0.1:8000/hello/{name}")
        data = response.json()
    print(data)
    return data


async def main2():
    job1 = fetch_item(4)
    print(type(job1))
    print("job1 coroutine created")

    print("starting job1")
    job1_task = asyncio.create_task(job1)  # the moment task starts to run
    await asyncio.sleep(1)

    job1result = await job1_task
    print(type(job1result))
    # job2 = fetch_item(6)
    # job3 = fetch_item(3)
    # job4 = fetch_item(7)
    # job5 = fetch_item(2)


async def main():
    job1 = asyncio.create_task(fetch_item(4))
    job2 = asyncio.create_task(fetch_item(6))
    job3 = asyncio.create_task(fetch_item(3))
    job4 = asyncio.create_task(fetch_item(7))
    job5 = asyncio.create_task(fetch_item(2))
    job6 = asyncio.create_task(fetch_hello("ruslan"))
    job7 = asyncio.create_task(fetch_hello("bohdan"))

    result1 = await job1
    result2 = await job2
    result3 = await job3
    result4 = await job4
    result5 = await job5
    result6 = await job6
    result7 = await job7


if __name__ == "__main__":
    # asyncio.run(main2())
    asyncio.run(main())
