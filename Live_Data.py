import aiohttp
import asyncio
import json
from datetime import datetime
import time


class BinanceFutures:
    def __init__(self, symbol):
        self.symbol = symbol.lower()
        self.url = f"wss://fstream.binance.com/stream?streams={self.symbol}@bookTicker/{self.symbol}@aggTrade"
        self.bid_price = None
        self.bid_qty = None
        self.ask_price = None
        self.ask_qty = None
        self.last_price = None
        self.acc_quantity = 0.0
        self.start_time = None

    async def get_24h_volume(self):
        async with aiohttp.ClientSession() as session:
            async with session.get(f'https://fapi.binance.com/fapi/v1/ticker/24hr?symbol={self.symbol.upper()}') as response:
                data = await response.json()
                return float(data['volume'])

    async def data_handler(self):
        async with aiohttp.ClientSession() as session:
            async with session.ws_connect(self.url) as ws:
                async for msg in ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        data = json.loads(msg.data)
                        stream_name = data['stream']
                        data = data['data']
                        if stream_name == f"{self.symbol}@bookTicker":
                            self.bid_price = data['b']
                            self.bid_qty = data['B']
                            self.ask_price = data['a']
                            self.ask_qty = data['A']
                        elif stream_name == f"{self.symbol}@aggTrade":
                            self.last_price = data['p']
                            self.acc_quantity += float(data['q'])

                    elif msg.type == aiohttp.WSMsgType.CLOSED:
                        break
                    elif msg.type == aiohttp.WSMsgType.ERROR:
                        break

    async def data_printer(self):
        if self.start_time is None:
            current_time = time.time()
            self.start_time = current_time + (1 - current_time % 1)

        while True:
            if self.bid_price and self.ask_price and self.last_price:
                print({"Time": datetime.now(), "BestBid": self.bid_price, "BidVol": self.bid_qty, "BestAsk": self.ask_price, "AskVol": self.ask_qty, "Close": self.last_price, "Volume": self.acc_quantity})

            elapsed_time = time.time() - self.start_time
            next_interval = 0.1 - (elapsed_time % 0.1)
            await asyncio.sleep(next_interval)

    async def main(self):
        self.acc_quantity = await self.get_24h_volume()
        task1 = asyncio.create_task(self.data_handler())
        task2 = asyncio.create_task(self.data_printer())
        await task1
        await task2


if __name__ == '__main__':
    binance_futures = BinanceFutures("btcusdt")
    asyncio.run(binance_futures.main())
