"""把官方的「臺北捷運車站出入口座標」整理成每站一筆的 mrt_stations.csv。

來源：臺北大眾捷運股份有限公司，政府資料開放平臺資料集 128428
      https://data.gov.tw/dataset/128428 （政府資料開放授權條款－第 1 版）
原始檔：data/raw/trtc_exits_big5.csv（Big5 編碼，每個出入口一筆）

做法：同一站的所有出入口座標取平均當作車站位置，只保留模擬範圍（config.BBOX）內的車站。

    python data/build_stations.py
"""
import csv
import re
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
import config as C  # noqa: E402

HERE = Path(__file__).parent
exits = defaultdict(list)
with open(HERE / "raw" / "trtc_exits_big5.csv", encoding="big5", newline="") as f:
    for row in csv.DictReader(f):
        name = re.split(r"出口|M\d", row["出入口名稱"])[0]
        if name.endswith("站") and name != "台北車站":
            name = name[:-1]
        exits[name].append((float(row["經度"]), float(row["緯度"])))

rows = []
for name, pts in exits.items():
    lon = sum(p[0] for p in pts) / len(pts)
    lat = sum(p[1] for p in pts) / len(pts)
    if C.BBOX[0] <= lon <= C.BBOX[2] and C.BBOX[1] <= lat <= C.BBOX[3]:
        rows.append((name, f"{lon:.6f}", f"{lat:.6f}", len(pts)))
rows.sort(key=lambda r: (r[1], r[2]))

with open(HERE / "mrt_stations.csv", "w", encoding="utf-8", newline="") as f:
    w = csv.writer(f)
    w.writerow(["name", "lon", "lat", "exits"])
    w.writerows(rows)
print(f"原始 {sum(len(v) for v in exits.values())} 個出入口、{len(exits)} 站 → 模擬範圍內 {len(rows)} 站")
