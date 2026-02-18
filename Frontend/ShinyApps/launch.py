import subprocess
import sys
import time
import webbrowser
import socket
import os
import threading

# Configuration
HOST = "127.0.0.1"
PORT = 8000
APP_FILE = "app.py"
URL = f"http://{HOST}:{PORT}"

def is_port_open(host, port):
    """Check if the server is accepting connections."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(1)
    try:
        s.connect((host, port))
        s.close()
        return True
    except:
        return False

def stream_output(process):
    """Stream subprocess output to console."""
    try:
        for line in iter(process.stdout.readline, ""):
            sys.stdout.write(line)
            sys.stdout.flush()
    except:
        pass

def check_dependencies():
    """Verify and automatically install required packages."""
    required = ["shiny", "pandas", "plotly", "shinywidgets", "pyarrow", "faicons"]
    missing = []
    
    for pkg in required:
        try:
            __import__(pkg)
        except ImportError:
            missing.append(pkg)
            
    if missing:
        print(f"\n[INFO] Installing missing packages: {', '.join(missing)}...")
        try:
            subprocess.check_call([sys.executable, "-m", "pip", "install", *missing])
            print("[INFO] Dependencies installed successfully.")
            return True
        except Exception as e:
            print(f"[ERROR] Auto-installation failed: {e}")
            print(f"Please run manually: pip install {' '.join(missing)}")
            return False
    return True

def main():
    print(f"--- SHINY LAUNCHER STARTED ---")
    
    if not check_dependencies():
        sys.exit(1)
    
    # 2. Start the Shiny Server
    # Run from the current directory
    cmd = [sys.executable, "-m", "shiny", "run", APP_FILE, "--host", HOST, "--port", str(PORT), "--reload"]
    
    print(f"Starting server: {' '.join(cmd)}")
    process = subprocess.Popen(
        cmd, 
        stdout=subprocess.PIPE, 
        stderr=subprocess.STDOUT, 
        cwd=os.path.dirname(os.path.abspath(__file__)) or ".",
        text=True,
        bufsize=1
    )

    # Start a thread to print output
    t = threading.Thread(target=stream_output, args=(process,), daemon=True)
    t.start()

    # 3. Wait for Server to be Ready
    print("Waiting for server to be ready...", end="", flush=True)
    max_retries = 30 # 30 seconds
    server_ready = False
    
    for i in range(max_retries):
        if is_port_open(HOST, PORT):
            server_ready = True
            print("\nServer is READY!")
            break
        time.sleep(0.2)
        if i % 5 == 0:
            print(".", end="", flush=True)

    if server_ready:
        print(f"Opening browser at {URL}")
        webbrowser.open(URL)
    else:
        print("\n\n[ERROR] Server failed to start within 30 seconds.")
        process.terminate()
        sys.exit(1)

    # 4. Keep alive
    try:
        process.wait()
        print(f"\n[INFO] Server exited with code {process.returncode}")
    except KeyboardInterrupt:
        print("\nStopping server...")
        process.terminate()
    except Exception as e:
        print(f"\n[ERROR] Unexpected error: {e}")
        
    if process.returncode != 0:
        sys.exit(process.returncode)

if __name__ == "__main__":
    main()
