from dataLoader import dataLoader
import asyncio

file_trade = '../dataProc/data/BTCUSDT-trades-2023-12-20.csv'
file_depth = '../dataProc/data/btc_depth_local'
file_write = 'btc_output_local.csv'



async def main():
    a = dataLoader( 
    file_depth=file_depth,
    file_trade=file_trade,
    file_write=file_write,
    ob_level=20,
    first_tick_ts=1,
    last_tick_ts=2,
)
    i = 0
    async for res in a.tick_depth_data_feed():
        i += 1
        if i ==5:  break
        # print(res)

asyncio.run(main())
