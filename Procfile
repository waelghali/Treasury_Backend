web: MALLOC_ARENA_MAX=2 MALLOC_TRIM_THRESHOLD_=65536 gunicorn app.main:app -k uvicorn.workers.UvicornWorker --bind 0.0.0.0:8000 --workers 1 --max-requests 1500 --max-requests-jitter 150 --timeout 120
