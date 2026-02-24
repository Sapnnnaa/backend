import os
import cv2
import time
import torch
import socket
import threading
import traceback
import mysql.connector
from ultralytics import YOLO
from dotenv import load_dotenv

# ================= ENV LOAD =================
load_dotenv()

# ================= CONFIG =================
CONF_THRESHOLD = float(os.getenv("CONF_THRESHOLD", 0.4))
EVENT_COOLDOWN = int(os.getenv("EVENT_COOLDOWN", 5))
RECONNECT_DELAY = int(os.getenv("RECONNECT_DELAY", 5))
USE_GPU = os.getenv("USE_GPU", "true").lower() == "true"
FRAME_SIZE = (
    int(os.getenv("FRAME_WIDTH", 640)),
    int(os.getenv("FRAME_HEIGHT", 480))
)
DB_RETRY_DELAY = int(os.getenv("DB_RETRY_DELAY", 5))
MODEL_PATH = os.getenv("MODEL_PATH", "models/LatestMixed.pt")

DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")
DB_TIMEOUT = int(os.getenv("DB_TIMEOUT", 5))



MAX_CAMERA_THREADS = 20
stop_event = threading.Event()

# ================= CLASS MAP =================
CLASS_MAP = {
    0: "fire",
    1: "smoke",
    2: "spill"
}

# ================= LOGGER =================
def log(msg):
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {msg}")

# ================= TCP ALERT =================

USR_IP = "127.0.0.1"   
USR_PORT = 9000        
def send_alert(message):
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(2)
            s.connect((USR_IP, USR_PORT))
            s.sendall((message + "\n").encode())
            log(f"[TCP SENT] {message}")
    except Exception as e:
        log(f"[TCP ERROR] {e}")

# ================= DB =================
def get_db_connection():
    try:
        return mysql.connector.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME,
            connection_timeout=DB_TIMEOUT
        )
    except Exception as e:
        log(f"[DB ERROR] {e}")
        return None

# ================= FETCH CAMERAS =================
def fetch_camera():
    while True:
        try:
            conn = get_db_connection()
            if conn is None:
                time.sleep(DB_RETRY_DELAY)
                continue

            cursor = conn.cursor(dictionary=True)
            cursor.execute("""
                SELECT * FROM addcamera
                WHERE status = 1
                ORDER BY id DESC
            """)
            data = cursor.fetchall()
            cursor.close()
            conn.close()
            return data

        except Exception as e:
            log(f"[DB ERROR] fetch_camera(): {e}")
            time.sleep(DB_RETRY_DELAY)

# ================= SAVE EVENT =================
def save_to_mysql(cam_name, frame, event_type):
    try:
        conn = get_db_connection()
        if conn is None:
            return

        cursor = conn.cursor()
        _, buffer = cv2.imencode('.jpg', frame)

        sql = """
            INSERT INTO fire_detections (cam_name, capture_frame, event_type)
            VALUES (%s, %s, %s)
        """
        cursor.execute(sql, (cam_name, buffer.tobytes(), event_type))
        conn.commit()

        cursor.close()
        conn.close()

        # ================= ALERT =================
        timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
        alert_msg = f"{timestamp} | CAMERA: {cam_name} | EVENT: {event_type.upper()}"
        send_alert(alert_msg)

        log(f"[MYSQL] {event_type.upper()} saved from {cam_name}")
        log(f"[ALERT] {alert_msg}")

    except Exception as e:
        log(f"[DB ERROR] save_to_mysql(): {e}")

# ================= CAMERA MAP =================
def build_camera_map():
    CAMERA_MAP = {}
    data = fetch_camera()

    for row in data:
        cam_ip = str(row["camera_ip"]).strip()
        cam_name = str(row["camera_name"]).strip()

        if cam_ip.lower().startswith(("rtsp://", "http://", "https://")):
            cam_source = cam_ip
        else:
            continue

        if cam_source in CAMERA_MAP:
            continue

        CAMERA_MAP[cam_source] = cam_name
        log(f"[CAMERA LOADED] {cam_name}")

    return CAMERA_MAP

# ================= DEVICE =================
def get_device():
    if USE_GPU and torch.cuda.is_available():
        log(f"[GPU] {torch.cuda.get_device_name(0)}")
        return 0
    else:
        log("[CPU MODE]")
        return "cpu"

# ================= MODEL =================
def load_model():
    if not os.path.exists(MODEL_PATH):
        raise SystemExit(f"Model not found: {MODEL_PATH}")

    log(f"[MODEL] Loading {MODEL_PATH}")
    return YOLO(MODEL_PATH)

# ================= WORKER =================
def camera_worker(cam_source, cam_name, model, device):
    last_event_time = {}
    frame_count = 0
    start_time = time.time()
    desired_fps = 1  # 1 FPS
    frame_interval = 1.0 / desired_fps

    while not stop_event.is_set():
        try:
            cap = cv2.VideoCapture(cam_source, cv2.CAP_FFMPEG)
            cap.set(cv2.CAP_PROP_BUFFERSIZE, 2)

            if not cap.isOpened():
                log(f"[RECONNECTING] {cam_name} - Unable to open stream")
                time.sleep(RECONNECT_DELAY)
                continue

            log(f"[{cam_name}] Stream connected")

            while not stop_event.is_set():
                frame_start_time = time.time()

                ret, frame = cap.read()
                if not ret:
                    log(f"[FRAME LOST] {cam_name}")
                    break

                frame = cv2.resize(frame, FRAME_SIZE)

                # YOLO inference
                results = model(frame, device=device, verbose=False)

                detected = set()
                for box in results[0].boxes:
                    cls = int(box.cls[0])
                    conf = float(box.conf[0])
                    if conf >= CONF_THRESHOLD and cls in CLASS_MAP:
                        detected.add(CLASS_MAP[cls])

                now = time.time()
                for event in detected:
                    last = last_event_time.get(event, 0)
                    if now - last >= EVENT_COOLDOWN:
                        save_to_mysql(cam_name, frame, event)
                        last_event_time[event] = now

                frame_count += 1
                if now - start_time >= 1.0:
                    log(f"[{cam_name}] Real-time FPS: {frame_count}")
                    frame_count = 0
                    start_time = now

                elapsed = time.time() - frame_start_time
                if elapsed < frame_interval:
                    time.sleep(frame_interval - elapsed)

            cap.release()
            time.sleep(RECONNECT_DELAY)

        except Exception as e:
            log(f"[THREAD ERROR] {cam_name}: {e}")
            traceback.print_exc()
            time.sleep(RECONNECT_DELAY)

# ================= START =================
def start_detection():
    CAMERA_MAP = build_camera_map()
    device = get_device()
    model = load_model()

    threads = []

    for i, (src, name) in enumerate(CAMERA_MAP.items()):
        if i >= MAX_CAMERA_THREADS:
            log("[LIMIT] Only 20 cameras allowed")
            break

        t = threading.Thread(
            target=camera_worker,
            args=(src, name, model, device),
            daemon=True
        )
        t.start()

        threads.append(t)
        log(f"[THREAD STARTED] {name}")

    log(f"[TOTAL RUNNING CAMERAS] {len(threads)}")
    return threads

# ================= MAIN =================
if __name__ == "__main__":
    try:
        log("[SYSTEM STARTING]")
        start_detection()

        while True:
            time.sleep(1)

    except KeyboardInterrupt:
        log("[SYSTEM STOPPING]")
        stop_event.set()