FROM python:3.12-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1
WORKDIR /app
RUN groupadd --system --gid 10001 bankapp \
 && useradd --system --uid 10001 --gid bankapp --home-dir /nonexistent --shell /usr/sbin/nologin bankapp \
 && mkdir -p /app/state \
 && chown -R bankapp:bankapp /app
COPY requirements.txt /app/requirements.txt
RUN python -m pip install --no-cache-dir -r /app/requirements.txt
COPY --chown=bankapp:bankapp bank_platform /app/bank_platform
COPY --chown=bankapp:bankapp config /app/config
USER bankapp
EXPOSE 8000
HEALTHCHECK --interval=15s --timeout=3s --start-period=10s --retries=3 \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/health', timeout=2).close()" || exit 1
CMD ["python", "-m", "uvicorn", "bank_platform.gateway.app:app", "--host", "0.0.0.0", "--port", "8000"]
