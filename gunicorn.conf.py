# gunicorn.conf.py
"""
Gunicorn configuration automatically loaded from the working directory on Render.
Overrides Render's default GUNICORN_CMD_ARGS (--preload) and WEB_CONCURRENCY (2)
to prevent Copy-on-Write (CoW) memory duplication between the Master and Worker
processes on a 512MB container.
"""
import os

# Strip Render's injected GUNICORN_CMD_ARGS (e.g. --preload) before Gunicorn parses env args
os.environ.pop("GUNICORN_CMD_ARGS", None)
os.environ["WEB_CONCURRENCY"] = "1"
os.environ.setdefault("MALLOC_ARENA_MAX", "2")
os.environ.setdefault("MALLOC_TRIM_THRESHOLD_", "65536")
os.environ.setdefault("MALLOC_MMAP_THRESHOLD_", "65536")

# Single Uvicorn worker without master preload so the master process stays ~25MB
# instead of holding a duplicate ~180MB copy of the application in RAM.
workers = int(os.getenv("GUNICORN_WORKERS", "1"))
worker_class = "uvicorn.workers.UvicornWorker"
preload_app = False
timeout = 120
max_requests = 1500
max_requests_jitter = 150


def _apply_glibc_malloc_tuning():
    """Applies Linux glibc mallopt tuning to return freed >=64KB chunks immediately to the OS."""
    try:
        import ctypes
        libc = ctypes.CDLL("libc.so.6")
        # M_TRIM_THRESHOLD = -1, M_MMAP_THRESHOLD = -3, M_ARENA_MAX = -8
        libc.mallopt(-1, 65536)
        libc.mallopt(-3, 65536)
        libc.mallopt(-8, 2)
        libc.malloc_trim(0)
    except Exception:
        pass


def on_starting(server):
    _apply_glibc_malloc_tuning()


def post_fork(server, worker):
    _apply_glibc_malloc_tuning()
