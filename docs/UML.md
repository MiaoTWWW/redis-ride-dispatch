# UML 圖

以下圖表用 Mermaid 繪製，GitHub 會直接顯示；對應的 PNG 在 `docs/img/`。

## 系統架構圖

![系統架構圖](img/uml_architecture.png)

<details><summary>Mermaid 原始碼</summary>

```mermaid
flowchart LR
  rider["乘客端<br/>瀏覽器 + Leaflet"]
  dash["監控儀表板<br/>瀏覽器 + Leaflet"]
  api["後端 app.py<br/>FastAPI + WebSocket"]
  sim["司機模擬器 simulator.py<br/>cmd 文字模式"]
  disp["派車服務 dispatcher.py<br/>可開多個"]
  subgraph R["Redis 7"]
    direction TB
    geo[("GEO<br/>drivers:all / drivers:available")]
    stream[("Streams + Consumer Group<br/>stream:ride_requests")]
    hash[("Hash / String + TTL<br/>trip:* driver:* offer:*")]
    pubsub(["Pub/Sub<br/>ch:map / ch:trip:*"])
    lua[["Lua 腳本 × 9<br/>原子狀態轉換"]]
  end
  rider -- "1 POST /api/ride" --> api
  api -- "2 MULTI/EXEC + XADD" --> stream
  stream -- "3 XREADGROUP" --> disp
  disp -- "4 GEOSEARCH + dispatch.lua" --> geo
  sim -- "5 Pipeline：位置 + 心跳" --> geo
  sim -- "6 PUBLISH ch:map" --> pubsub
  pubsub -- "7 SUBSCRIBE" --> api
  api -- "8 WebSocket 推播" --> rider
  api -- "8 WebSocket 推播" --> dash
  hash -. "offer / 心跳過期通知" .-> disp
```

</details>

## 用例圖

![用例圖](img/uml_usecase.png)

<details><summary>Mermaid 原始碼</summary>

```mermaid
flowchart LR
  P(["👤 乘客"])
  S(["👤 司機<br/>（模擬器）"])
  A(["👤 管理員"])
  subgraph SYS["即時派車模擬系統"]
    direction TB
    U1(["設定上車點與目的地"])
    U2(["叫車"])
    U3(["查看司機位置與預估到達時間"])
    U4(["回報位置與心跳"])
    U5(["接單／拒單"])
    U6(["載客並完成行程"])
    U7(["查看全部車輛與即時統計"])
    U8(["查看動態加價熱力圖"])
    U9(["執行壓力測試與正確性檢查"])
    U10(["自動派車：找最近空車"])
    U11(["逾時改派"])
    U12(["計算動態加價"])
  end
  P --- U1
  P --- U2
  P --- U3
  A --- U7
  A --- U8
  A --- U9
  U4 --- S
  U5 --- S
  U6 --- S
  U2 -. include .-> U10
  U2 -. include .-> U12
  U10 -. extend .-> U11
```

</details>

## 循序圖：叫車、派單、逾時改派

![循序圖：叫車、派單、逾時改派](img/uml_sequence.png)

<details><summary>Mermaid 原始碼</summary>

```mermaid
sequenceDiagram
  autonumber
  actor P as 乘客
  participant API as FastAPI
  participant R as Redis
  participant D as dispatcher
  participant A as 司機 A（最近）
  participant B as 司機 B（次近）
  P->>API: POST /api/ride
  API->>R: SET ratelimit NX EX 5、INCR seq:trip
  API->>R: MULTI：HSET trip、XADD 佇列、HINCRBY stats、EXEC
  API-->>P: tid、車資
  D->>R: XREADGROUP 取得請求
  D->>R: GEOSEARCH drivers:available 1 km ASC
  D->>R: EVALSHA dispatch.lua（司機 A）
  Note over R: 原子：檢查 available → ZREM 空車清單<br/>→ 狀態 offered → SET offer EX 10
  R-->>P: Pub/Sub → WebSocket「已通知司機 A」
  Note over A: 沒有回應
  R-->>D: 過期通知 offer:tid expired（10 秒後）
  D->>R: EVALSHA release.lua（timeout）
  Note over R: 原子：司機 A 放回空車清單、加入 tried<br/>行程回到 searching、重新 XADD
  D->>R: XREADGROUP → GEOSEARCH（排除 tried）
  D->>R: EVALSHA dispatch.lua（司機 B）
  B->>R: EVALSHA accept.lua
  Note over R: 原子：確認 offer 還是自己 → DEL offer<br/>→ 狀態 to_pickup
  R-->>P: 「司機 B 已接單」
  B->>R: 每秒回報位置（Pipeline）→ pickup.lua → complete.lua
  R-->>P: 「已上車」→「已抵達，車資 N 元」
```

</details>

## 狀態圖：司機的五個狀態

![狀態圖：司機的五個狀態](img/uml_state.png)

<details><summary>Mermaid 原始碼</summary>

```mermaid
stateDiagram-v2
  direction LR
  [*] --> available: 上線（update_position.lua）
  available --> offered: 被派單（dispatch.lua）
  offered --> to_pickup: 接單（accept.lua）
  offered --> available: 拒單／10 秒逾時（release.lua）
  to_pickup --> on_trip: 抵達上車點（pickup.lua）
  on_trip --> available: 抵達目的地（complete.lua）
  available --> offline: 心跳 5 秒過期（offline.lua）
  offered --> offline: 斷線，行程改派
  to_pickup --> offline: 斷線，行程改派
  on_trip --> offline: 斷線，行程中止
  offline --> available: 重新回報位置
```

</details>

## 活動圖：擴大半徑重試

![活動圖：擴大半徑重試](img/uml_activity.png)

<details><summary>Mermaid 原始碼</summary>

```mermaid
flowchart TD
  s([收到叫車請求]) --> c{行程還在 searching？}
  c -- 否 --> ack([XACK，結束])
  c -- 是 --> g["GEOSEARCH 目前半徑內的空車<br/>由近到遠，排除已拒單／逾時的司機"]
  g --> h{有候選司機？}
  h -- 有 --> l["對最近的一位執行 dispatch.lua"]
  l --> ok{成功？}
  ok -- 成功 --> o["等待接單（offer TTL 10 秒）"] --> ack
  ok -- 被別的 dispatcher 搶走 --> more{還有下一位？<br/>最多 5 位}
  more -- 有 --> l
  more -- 沒有 --> e
  h -- 沒有 --> e{還能擴大半徑？<br/>1 → 3 → 5 km}
  e -- 可以 --> z["ZADD queue:delayed<br/>2 秒後用更大的半徑重試"] --> ack
  e -- 5 km 也沒車 --> f["fail.lua：標記 failed<br/>通知乘客"] --> ack
```

</details>

## 部署圖

![部署圖](img/uml_deployment.png)

<details><summary>Mermaid 原始碼</summary>

```mermaid
flowchart LR
  subgraph PC["使用者電腦"]
    B1["瀏覽器：乘客端"]
    B2["瀏覽器：監控儀表板"]
  end
  subgraph HOST["主機（Windows 11 / 任何有 Python 與 Docker 的機器）"]
    direction TB
    subgraph PY["Python 3.11+ 行程"]
      U["uvicorn app:app<br/>:8000"]
      D1["dispatcher-1"]
      D2["dispatcher-2 …"]
      S["simulator<br/>（cmd 視窗）"]
      T["simulate_riders / verify"]
    end
    subgraph DK["Docker"]
      R[("redis:7-alpine<br/>:6379")]
    end
  end
  OSM["OpenStreetMap<br/>圖磚伺服器"]
  B1 -- "HTTP + WebSocket" --> U
  B2 -- "HTTP + WebSocket" --> U
  B1 -. HTTPS .-> OSM
  U -- RESP --> R
  D1 -- RESP --> R
  D2 -- RESP --> R
  S -- RESP --> R
  T -- RESP --> R
```

</details>
