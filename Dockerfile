# 單一容器版：Redis + 兩個派車服務 + 司機模擬器 + 網頁後端
# 給 Hugging Face Spaces（Docker SDK）或任何只能跑一個容器的平台使用。
# 本機開發仍建議用 docker-compose.yml + start_all.bat。
#
#   docker build -t ride-dispatch .
#   docker run --rm -p 7860:7860 ride-dispatch
#   → http://localhost:7860/  與  http://localhost:7860/dashboard
FROM python:3.12-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends redis-server \
    && rm -rf /var/lib/apt/lists/*

# Hugging Face Spaces 以 uid 1000 執行容器
RUN useradd -m -u 1000 user
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY --chown=user config.py rides.py simulator.py dispatcher.py app.py ./
COPY --chown=user lua ./lua
COPY --chown=user static ./static
COPY --chown=user data/mrt_stations.csv ./data/mrt_stations.csv
COPY --chown=user deploy/start.sh ./start.sh
# 在 Windows 上 checkout 可能變成 CRLF，這裡統一轉回 LF
RUN sed -i 's/\r$//' start.sh && chown user /app

USER user
ENV PYTHONUNBUFFERED=1 \
    PYTHONIOENCODING=utf-8 \
    PORT=7860 \
    CARS=100 \
    SPEED=150

EXPOSE 7860
CMD ["bash", "/app/start.sh"]
