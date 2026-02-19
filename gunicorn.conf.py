# Gunicorn configuration for Azure App Service
import multiprocessing

# Bind to the standard Azure port
bind = "0.0.0.0:8000"

# Enable Uvicorn workers for ASGI (Shiny) support
worker_class = "uvicorn.workers.UvicornWorker"

# Timeout adjustment for large dashboard loads
timeout = 600

# Worker count (optional, Azure usually manages this)
workers = multiprocessing.cpu_count() * 2 + 1
