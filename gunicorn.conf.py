# Gunicorn configuration for Azure Linux
import multiprocessing

# Port binding
bind = "0.0.0.0:8000"

# FORCE Uvicorn worker for ASGI (Shiny)
worker_class = "uvicorn.workers.UvicornWorker"

# Increase timeout for slow dashboard loads
timeout = 600

# Worker count
workers = multiprocessing.cpu_count() * 2 + 1

# Print config for debugging
print(f"Starting Gunicorn with worker_class: {worker_class}")
