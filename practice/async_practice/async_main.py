import asyncio
import time

import httpx


async def fetch_item(client, item_id):
    print(f"Client side message: Starting request for item {item_id}...")
    response = await client.get(f"http://127.0.0.1:8000/process/{item_id}")
    data = response.json()
    print(f"Client side message: Finished item {item_id} at {data['timestamp']}")
    return data


async def main():
    async with httpx.AsyncClient(timeout=30.0) as client:
        start_time = time.perf_counter()

        tasks = [fetch_item(client, i) for i in range(5)]

        # tasks = []
        # for i in range(5):
        #     tasks.append(fetch_item(client, i))

        await asyncio.gather(*tasks)

        end_time = time.perf_counter()
        print(
            f"\n--- Total time for 5 requests: {end_time - start_time:.2f} seconds ---"
        )


if __name__ == "__main__":
    asyncio.run(main())
