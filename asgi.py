# Standard ASGI entry point for Azure / Linux
import os
import sys

# Add the dashboard folder to Python path
DASHBOARD_PATH = os.path.join(os.path.dirname(__file__), "Frontend", "ShinyApps")
if DASHBOARD_PATH not in sys.path:
    sys.path.append(DASHBOARD_PATH)

# Import the Shiny app object
from app import app as asgi_app

# For Azure/Gunicorn to find the app
app = asgi_app
