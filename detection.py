import os
os.environ["OPENCV_FFMPEG_CAPTURE_OPTIONS"] = "rtsp_transport;tcp|stimeout;5000000"
import cv2
import time
import torch
import socket
import threading
import traceback
import mysql.connector
from ultralytics import YOLO
from dotenv import load_dotenv
from urllib.parse import quote

load_dotenv()

# ================= CONFIG =================
CONF_THRESHOLD = float(os.getenv("CONF_THRESHOLD", 0.3))
EVENT_COOLDOWN = int(os.getenv("EVENT_COOLDOWN", 5))
RECONNECT_DELAY = int(os.getenv("RECONNECT_DELAY", 5))
DB_RETRY_DELAY = int(os.getenv("DB_RETRY_DELAY", 5))
USE_GPU = os.getenv("USE_GPU", "true").lower() == "true"

FRAME_SIZE = (
    int(os.getenv("FRAME_WIDTH", 640)),
    int(os.getenv("FRAME_HEIGHT", 480))
)

MODEL_PATH = os.getenv("MODEL_PATH", "models/Final.pt")

DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")
DB_TIMEOUT = int(os.getenv("DB_TIMEOUT", 5))

USR_IP = os.getenv("USR_IP", "127.0.0.1")
USR_PORT = int(os.getenv("USR_PORT", 9000))

CAMERA_REFRESH_INTERVAL = int(os.getenv("CAMERA_REFRESH_INTERVAL", 10))
MAX_CAMERA_THREADS = int(os.getenv("MAX_CAMERA_THREADS", 20))

stop_event = threading.Event()

DB_CONNECTED = False
ACTIVE_CAMERAS = {}

CLASS_MAP = {
    0: "fire",
    1: "smoke",
    2: "spill"
}

# ================= LOGGER =================
def log(msg):
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {msg}")

# ================= TCP ALERT =================
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

# ================= DB WATCHER =================
def db_watcher():
    global DB_CONNECTED
    while not stop_event.is_set():
        conn = get_db_connection()
        if conn:
            if not DB_CONNECTED:
                log("[DB CONNECTED] Cameras can start")
            DB_CONNECTED = True
            conn.close()
        else:
            if DB_CONNECTED:
                log("[DB DISCONNECTED] Cameras paused")
            DB_CONNECTED = False
        time.sleep(DB_RETRY_DELAY)

# ================= UPDATE CAMERA STATUS =================
def update_camera_status(cam_ip, status):
    try:
        if not DB_CONNECTED:
            return

        conn = get_db_connection()
        if conn is None:
            return

        cursor = conn.cursor()

        cursor.execute("""
        UPDATE addcamera
        SET status=%s, updated_at=CURRENT_TIMESTAMP
        WHERE camera_ip=%s
        """,(status, cam_ip))

        conn.commit()
        cursor.close()
        conn.close()

        state = "ACTIVE" if status == 1 else "INACTIVE"
        log(f"[DB STATUS] {cam_ip} → {state}")

    except Exception as e:
        log(f"[DB ERROR] update_camera_status(): {e}")

# ================= FETCH CAMERAS =================
def fetch_camera():
    if not DB_CONNECTED:
        return None

    try:
        conn = get_db_connection()
        if conn is None:
            return None

        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT * FROM addcamera ORDER BY id DESC")

        data = cursor.fetchall()

        cursor.close()
        conn.close()

        return data

    except Exception as e:
        log(f"[DB ERROR] fetch_camera(): {e}")
        return None

# ================= SAVE EVENT =================
def save_to_mysql(cam_name, frame, event_type):
    try:
        if not DB_CONNECTED:
            return

        conn = get_db_connection()
        if conn is None:
            return

        cursor = conn.cursor()

        _, buffer = cv2.imencode('.jpg', frame)

        sql = """
        INSERT INTO fire_detections (cam_name,capture_frame,event_type)
        VALUES (%s,%s,%s)
        """

        cursor.execute(sql,(cam_name,buffer.tobytes(),event_type))

        conn.commit()

        cursor.close()
        conn.close()

        timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
        alert_msg = f"{timestamp} | CAMERA: {cam_name} | EVENT: {event_type.upper()}"

        send_alert(alert_msg)

        log(f"[MYSQL] {event_type.upper()} saved from {cam_name}")

    except Exception as e:
        log(f"[DB ERROR] save_to_mysql(): {e}")

# ================= BUILD CAMERA MAP =================
def build_camera_map():

    CAMERA_MAP = {}

    data = fetch_camera()

    if data is None:
        return CAMERA_MAP

    for row in data:

        cam_id = row["id"]
        cam_ip = str(row["camera_ip"]).strip()
        cam_name = str(row["camera_name"]).strip()

        rtsp = str(row["rtsp_url"]).strip()

        if not rtsp:
            log(f"[INVALID RTSP] Camera {cam_name}")
            continue

        CAMERA_MAP[cam_id] = {
            "rtsp": rtsp,
            "name": cam_name,
            "ip": cam_ip
        }

    return CAMERA_MAP

# ================= DEVICE =================
def get_device():

    if USE_GPU and torch.cuda.is_available():
        log(f"[GPU] {torch.cuda.get_device_name(0)}")
        return "cuda:0"
    else:
        log("[CPU MODE]")
        return "cpu"

# ================= LOAD MODEL =================
def load_model():

    if not os.path.exists(MODEL_PATH):
        raise SystemExit(f"Model not found: {MODEL_PATH}")

    log(f"[MODEL] Loading {MODEL_PATH}")

    return YOLO(MODEL_PATH)

MODEL = load_model()

# ================= CAMERA WORKER =================
def camera_worker(rtsp, cam_name, cam_ip, device, stop_flag):

    last_event_time = {}
    inactive = False
    fail_count = 0
    MAX_FAIL = 50

    desired_fps = 1
    frame_interval = 1.0 / desired_fps

    while not stop_event.is_set() and not stop_flag.is_set():

        try:

            log(f"[CONNECTING CAMERA] {cam_name} ({cam_ip})")

            

            cap = cv2.VideoCapture(rtsp, cv2.CAP_FFMPEG)

            cap.set(cv2.CAP_PROP_BUFFERSIZE, 1)

            if not cap.isOpened():

                log(f"[STREAM DOWN] {cam_name}")

                if not inactive:
                    update_camera_status(cam_ip,0)
                    inactive = True

                time.sleep(RECONNECT_DELAY)
                continue


            if inactive:
                update_camera_status(cam_ip,1)
                inactive = False

            log(f"[STREAM CONNECTED] {cam_name}")


            while not stop_event.is_set() and not stop_flag.is_set():

                start = time.time()


                ret, frame = cap.read()

                if not ret or frame is None:

                    fail_count += 1

                    log(f"[FRAME MISS {fail_count}] {cam_name}")

                    if fail_count >= MAX_FAIL:

                        log(f"[STREAM LOST] {cam_name}")

                        if not inactive:
                            update_camera_status(cam_ip,0)
                            inactive = True

                        break

                    time.sleep(0.2)
                    continue


                if ret and frame is not None:
                     fail_count = 0

                frame = cv2.resize(frame, FRAME_SIZE)

                results = MODEL(frame, device=device, verbose=False)

                detected = set()

                for box in results[0].boxes:

                    cls = int(box.cls[0])
                    conf = float(box.conf[0])

                    if conf >= CONF_THRESHOLD and cls in CLASS_MAP:
                        detected.add(CLASS_MAP[cls])


                frame = results[0].plot()

                now = time.time()

                for event in detected:

                    last = last_event_time.get(event,0)

                    if now - last >= EVENT_COOLDOWN:

                        save_to_mysql(cam_name, frame, event)

                        last_event_time[event] = now


                # maintain 1 FPS processing
                elapsed = time.time() - start

                if elapsed < frame_interval:
                    time.sleep(frame_interval - elapsed)


            cap.release()
            time.sleep(RECONNECT_DELAY)

        except Exception as e:

            log(f"[THREAD ERROR] {cam_name}: {e}")

            traceback.print_exc()

            if not inactive:
                update_camera_status(cam_ip,0)
                inactive = True

            time.sleep(RECONNECT_DELAY)

# ================= CAMERA MANAGER =================
def camera_manager():

    device = get_device()

    while not stop_event.is_set():

        cameras = build_camera_map()

        existing_ids = set(ACTIVE_CAMERAS.keys())
        db_ids = set(cameras.keys())

        removed = existing_ids - db_ids

        # STOP REMOVED CAMERAS
        for cam_id in removed:

            log(f"[REMOVED CAMERA] {cam_id}")

            cam_data = ACTIVE_CAMERAS[cam_id]

            cam_data["stop"].set()

            cam_thread = cam_data["thread"]

            if cam_thread.is_alive():
                log(f"[STOPPING THREAD] {cam_id}")
                cam_thread.join(timeout=2)

            del ACTIVE_CAMERAS[cam_id]

        # START NEW CAMERAS
        for cam_id, cam in cameras.items():

            if cam_id not in ACTIVE_CAMERAS:

                if len(ACTIVE_CAMERAS) >= MAX_CAMERA_THREADS:
                    log("[LIMIT] Max camera threads reached")
                    continue

                stop_flag = threading.Event()

                t = threading.Thread(
                    target=camera_worker,
                    args=(cam["rtsp"], cam["name"], cam["ip"], device, stop_flag),
                    daemon=True
                )

                ACTIVE_CAMERAS[cam_id] = {
                    "thread": t,
                    "stop": stop_flag
                }

                log(f"[NEW CAMERA] {cam['name']}")

                t.start()

        log(f"[ACTIVE CAMERAS] {len(ACTIVE_CAMERAS)}")

        time.sleep(CAMERA_REFRESH_INTERVAL)

# ================= START DETECTION =================
def start_detection():

    threading.Thread(target=db_watcher, daemon=True).start()

    while not DB_CONNECTED:

        log("[WAITING FOR DB]")

        time.sleep(DB_RETRY_DELAY)

    manager_thread = threading.Thread(
        target=camera_manager,
        daemon=True
    )

    manager_thread.start()

    log("[CAMERA MANAGER STARTED]")

    return [manager_thread]

# ================= MAIN =================
if __name__=="__main__":

    try:

        log("[SYSTEM STARTING]")

        start_detection()

        while True:
            time.sleep(1)

    except KeyboardInterrupt:

        log("[SYSTEM STOPPING]")

        stop_event.set()