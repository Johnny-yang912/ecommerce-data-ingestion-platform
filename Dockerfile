FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# 先裝依賴 → 吃 layer cache，改原始碼不會讓依賴層失效
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 非 root 執行
RUN useradd --create-home --uid 1000 appuser
COPY --chown=appuser:appuser . .
USER appuser

EXPOSE 8000

# 預設啟動 api；migrate 服務在 compose 以 command 覆寫
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]
