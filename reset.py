"""清空目前這個 Redis DB，重新開始展示。會刪除所有資料，執行前請先關掉 simulator 與 dispatcher。

    python reset.py --yes
"""
import asyncio
import sys

import config as C


async def main():
    C.utf8_console()
    if "--yes" not in sys.argv:
        print(f"這會清空 {C.REDIS_URL} 的所有資料。確定的話請加上 --yes")
        sys.exit(1)
    r = C.get_redis()
    await r.flushdb()
    await C.ensure_group(r)
    await C.ensure_stations(r)
    await r.aclose()
    print("已清空，並重建 Consumer Group 與捷運站資料。")


if __name__ == "__main__":
    asyncio.run(main())
