import asyncio
import time


async def main():
    print('josh')
    asyncio.get_event_loop().create_task(foo('rem'))
    print('pot')

async def foo(text):
    print(text)
    await asyncio.sleep(1)


loop = asyncio.get_event_loop()
loop.run_until_complete(main())


