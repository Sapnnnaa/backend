import os
import threading
import time
import mysql.connector
from flask import Flask, jsonify, Response, request
from flask_cors import CORS
from dotenv import load_dotenv
from detection import start_detection, stop_event

# ================= ENV =================
load_dotenv()

API_HOST = os.getenv("API_HOST", "0.0.0.0")
API_PORT = int(os.getenv("API_PORT", 5001))

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_USER = os.getenv("DB_USER", "root")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")
DB_NAME = os.getenv("DB_NAME", "fire_detection")
DB_TIMEOUT = int(os.getenv("DB_TIMEOUT", 5))

# ================= APP =================
app = Flask(__name__)
CORS(app)

threads = []
system_started = False
system_lock = threading.Lock()

# ================= LOG =================
def log(msg):
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {msg}")

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

# ================= BOOT SYSTEM =================
def boot_system():
    global threads, system_started
    with system_lock:
        if not system_started:
            log("[SYSTEM] Starting detection service")
            threads = start_detection()
            system_started = True
            log("[SYSTEM] Detection started")

# ================= ROUTES =================
@app.route("/")
def root():
    return jsonify({"service": "Fire Detection API", "status": "running"})

@app.route("/health")
def health():
    return jsonify({
        "status": "running",
        "detection": system_started,
        "threads": len(threads)
    })

@app.route("/event-logs")
def event_logs():
    db = get_db_connection()
    if not db:
        return jsonify({"success": False, "data": []})

    cursor = db.cursor(dictionary=True)
    cursor.execute("""
        SELECT id, cam_name, event_type, detected_at
        FROM fire_detections
        ORDER BY detected_at DESC
        LIMIT 50
    """)
    rows = cursor.fetchall()
    cursor.close()
    db.close()

    logs = [{
        "id": r["id"],
        "camera": r["cam_name"],
        "eventType": r["event_type"],
        "detectedAt": r["detected_at"],
        "imageUrl": f"http://127.0.0.1:{API_PORT}/event-image/{r['id']}"
    } for r in rows]

    return jsonify({"success": True, "data": logs})

@app.route("/event-image/<int:log_id>")
def event_image(log_id):
    db = get_db_connection()
    if not db:
        return "DB Error", 500

    cursor = db.cursor()
    cursor.execute("SELECT capture_frame FROM fire_detections WHERE id=%s", (log_id,))
    row = cursor.fetchone()
    cursor.close()
    db.close()

    if row and row[0]:
        return Response(row[0], mimetype="image/jpeg")
    return "Not found", 404

#================Alert=======================#

@app.route("/alerts")
def get_alerts():
    db = get_db_connection()
    if not db:
        return jsonify({"success": False, "data": []})

    cursor = db.cursor(dictionary=True)

    # 1️⃣ Inactive Cameras
    cursor.execute("""
        SELECT camera_name
        FROM addcamera
        WHERE status = 0
    """)
    inactive = cursor.fetchall()

    # 2️⃣ Latest Detection Events (Last 5)
    cursor.execute("""
        SELECT cam_name, event_type, detected_at
        FROM fire_detections
        ORDER BY detected_at DESC
        LIMIT 5
    """)
    events = cursor.fetchall()

    cursor.close()
    db.close()

    alerts = []

    # Camera inactive alerts
    for cam in inactive:
        alerts.append({
            "type": "camera_inactive",
            "message": f"{cam['camera_name']} is INACTIVE"
        })

    # Detection alerts
    for e in events:
        alerts.append({
            "type": "detection",
            "message": f"{e['event_type'].upper()} detected in {e['cam_name']}"
        })

    return jsonify({"success": True, "data": alerts})    

# ================= CAMERAS API =================

@app.route("/api/cameras", methods=["GET"])
def get_cameras():
    db = get_db_connection()
    cursor = db.cursor(dictionary=True)
    cursor.execute("""
        SELECT 
            id,
            camera_name,
            camera_ip,
            status,
            install_date
        FROM addcamera
        ORDER BY id DESC
    """)
    rows = cursor.fetchall()
    cursor.close()
    db.close()
    return jsonify({"success": True, "data": rows})


@app.route("/api/cameras", methods=["POST"])
def add_camera():
    data = request.json
    db = get_db_connection()
    cursor = db.cursor()

    rtsp_url = f"rtsp://{data['camera_username']}:{data['camera_password']}@{data['camera_ip']}:{data.get('camera_port',554)}/stream"

    cursor.execute("""
        INSERT INTO addcamera 
        (camera_name, camera_username, camera_password, camera_ip, camera_port, rtsp_url, status, install_date)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
    """, (
        data["camera_name"],
        data["camera_username"],
        data["camera_password"],
        data["camera_ip"],
        int(data.get("camera_port", 554)),
        rtsp_url,
        int(data["status"]),
        data["install_date"]
    ))

    db.commit()
    cursor.close()
    db.close()

    return jsonify({"success": True, "message": "Camera added"})


@app.route("/api/cameras", methods=["PUT"])
def update_camera():
    data = request.json
    db = get_db_connection()
    cursor = db.cursor()

    rtsp_url = f"rtsp://{data['camera_username']}:{data['camera_password']}@{data['camera_ip']}:{data.get('camera_port',554)}/stream"

    cursor.execute("""
        UPDATE addcamera
        SET 
            camera_name=%s,
            camera_username=%s,
            camera_password=%s,
            camera_ip=%s,
            camera_port=%s,
            rtsp_url=%s,
            status=%s,
            install_date=%s
        WHERE id=%s
    """, (
        data["camera_name"],
        data["camera_username"],
        data["camera_password"],
        data["camera_ip"],
        int(data.get("camera_port", 554)),
        rtsp_url,
        int(data["status"]),
        data["install_date"],
        data["id"]
    ))

    db.commit()
    cursor.close()
    db.close()

    return jsonify({"success": True, "message": "Camera updated"})


@app.route("/api/cameras", methods=["DELETE"])
def delete_camera():
    cam_id = request.args.get("id")
    db = get_db_connection()
    cursor = db.cursor()
    cursor.execute("DELETE FROM addcamera WHERE id=%s", (cam_id,))
    db.commit()
    cursor.close()
    db.close()
    return jsonify({"success": True, "message": "Camera deleted"})

# ================= BOOT =================
if __name__ == "__main__":
    boot_system()
    log("[API] Server starting")
    app.run(host=API_HOST, port=API_PORT, debug=False, use_reloader=False)
    