import os
import sys
import platform
import subprocess

def run():
    print(f"--- QBA Dashboard Launcher (OS: {platform.system()}) ---")
    
    # Ensure port 8000 is clear on Linux/macOS
    if platform.system() != "Windows":
        print("Cleaning up port 8000...")
        subprocess.run("fuser -k 8000/tcp", shell=True, stderr=subprocess.DEVNULL)

    if platform.system() == "Windows":
        print("Running on Windows: Using Uvicorn (Fast & Native)")
        # On Windows, we run uvicorn directly through python
        import uvicorn
        from app import app
        uvicorn.run(app, host="0.0.0.0", port=8000)
    else:
        print("Running on Linux/macOS: Using Gunicorn (Production Grade)")
        # On Linux, we use the gunicorn command that Azure uses
        cmd = "gunicorn --bind=0.0.0.0:8000 --timeout 600 asgi:app -k uvicorn.workers.UvicornWorker"
        subprocess.run(cmd, shell=True)

if __name__ == "__main__":
    run()
