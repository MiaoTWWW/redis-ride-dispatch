---
title: Redis Ride Dispatch
emoji: 🚕
colorFrom: indigo
colorTo: yellow
sdk: docker
app_port: 7860
pinned: false
---

# 即時派車模擬系統

仿叫車平台的即時派車系統。台北市地圖上有 100 台模擬車輛移動，乘客叫車後系統找出最近的空車並派單，
並保證同一台車不會同時被派給兩位乘客。

- 乘客端：`/`
- 監控儀表板：`/dashboard`

這個 Space 把 Redis、兩個派車服務、司機模擬器和網頁後端放在同一個容器裡執行。
資料只存在記憶體，Space 重啟後會清空。

原始碼與設計說明：<https://github.com/MiaoTWWW/redis-ride-dispatch>
