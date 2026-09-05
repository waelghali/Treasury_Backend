web: MALLOC_ARENA_MAX=2 gunicorn app.main:app -k uvicorn.workers.UvicornWorker --bind 0.0.0.0:8000 --workers 1 --max-requests 5000 --max-requests-jitter 200 --timeout 120
