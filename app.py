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

# ================= CAMERAS API =================

@app.route("/api/cameras", methods=["GET"])
def get_cameras():
    db = get_db_connection()
    cursor = db.cursor(dictionary=True)
    cursor.execute("SELECT * FROM addcamera ORDER BY id DESC")
    rows = cursor.fetchall()
    cursor.close()
    db.close()
    return jsonify({"success": True, "data": rows})

@app.route("/api/cameras", methods=["POST"])
def add_camera():
    data = request.json
    db = get_db_connection()
    cursor = db.cursor()
    cursor.execute("""
        INSERT INTO addcamera (camera_name, camera_ip, status, date)
        VALUES (%s, %s, %s, %s)
    """, (
        data["name"],
        data["ip"],
        int(data["status"]),
        data["installDate"]
    ))
    db.commit()
    cursor.close()
    db.close()
    return jsonify({"success": True})

@app.route("/api/cameras", methods=["PUT"])
def update_camera():
    data = request.json
    db = get_db_connection()
    cursor = db.cursor()
    cursor.execute("""
        UPDATE addcamera
        SET camera_name=%s, camera_ip=%s, status=%s, date=%s
        WHERE id=%s
    """, (
        data["name"],
        data["ip"],
        int(data["status"]),
        data["installDate"],
        data["id"]
    ))
    db.commit()
    cursor.close()
    db.close()
    return jsonify({"success": True})

@app.route("/api/cameras", methods=["DELETE"])
def delete_camera():
    cam_id = request.args.get("id")
    db = get_db_connection()
    cursor = db.cursor()
    cursor.execute("DELETE FROM addcamera WHERE id=%s", (cam_id,))
    db.commit()
    cursor.close()
    db.close()
    return jsonify({"success": True})

# ================= BOOT =================
if __name__ == "__main__":
    boot_system()
    log("[API] Server starting")
    app.run(host=API_HOST, port=API_PORT, debug=False, use_reloader=False)
    