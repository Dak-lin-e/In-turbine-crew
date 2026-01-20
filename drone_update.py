#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import time
import os
import math
import subprocess
from threading import Thread, Lock, Event
from datetime import datetime, timezone

from google.cloud import pubsub_v1
# google-cloud-storage 임포트 가드
try:
    from google.cloud import storage  # type: ignore
except Exception as e:
    storage = None
    print("[boot] google-cloud-storage 미로딩: 업로드는 스킵됨. 이유:", e)

from dronekit import connect, VehicleMode, Command
from pymavlink import mavutil

# EXIF 메타데이터 삽입용
try:
    import piexif
    from PIL import Image  # Pillow
except Exception as e:
    piexif = None
    Image = None
    print("[boot] EXIF 메타데이터 모듈(piexif/Pillow) 미로딩: 메타데이터 삽입은 스킵됨. 이유:", e)

# ================== 구성값 ==================
MAX_AGE_SEC = 5
SERVICE_ACCOUNT_PATH = "/home/pi/Desktop/tlatfarm-project-f77303540ef2.json"

PROJECT_ID = "tlatfarm-project"
COMMAND_SUBSCRIPTION_ID = "drone-commands-sub"
STATUS_TOPIC_ID = "drone-status"
IMAGE_STATUS_TOPIC_ID = "drone-image-status"

CONNECTION_MODE = os.getenv("CONNECTION_MODE", "udp").lower()
SERIAL_PORT = os.getenv("SERIAL_PORT", "/dev/serial0")
SERIAL_BAUD = int(os.getenv("SERIAL_BAUD", "57600"))
UDP_CONNECTION_STRING = os.getenv("UDP_CONNECTION_STRING", "udp:127.0.0.1:14550")

MISSION_ALTITUDE = float(os.getenv("MISSION_ALTITUDE", "3"))       # m
DEFAULT_GROUNDSPEED = float(os.getenv("DEFAULT_GROUNDSPEED", "2.0")) # m/s



# LOITER/촬영 타이밍
LOITER_SEC = int(os.getenv("LOITER_SEC", "5"))
CAPTURE_OFFSET_SEC = float(os.getenv("CAPTURE_OFFSET_SEC", "1.0"))

# 카메라 백엔드: "libcamera" 또는 "picamera2"
CAMERA_BACKEND = os.getenv("CAMERA_BACKEND", "libcamera").lower()
PHOTO_CAPTURE_TIMEOUT = int(os.getenv("PHOTO_CAPTURE_TIMEOUT", "10"))

# ========== 라이다 센서 설정 ==========
LIDAR_SERIAL_PORT = "/dev/ttyAMA0"      # 라이다 연결 포트
LIDAR_BAUD = 57600
LIDAR_MIN_DISTANCE_CM = 10              # 최소 유효 거리(cm)
LIDAR_MAX_DISTANCE_CM = 1200            # 최대 유효 거리(cm)
LIDAR_TIMEOUT_SEC = 2.0                 # 센서 타임아웃(초)
LIDAR_USE_FOR_ALTITUDE = True           # 라이다 고도 제어 활성화

TARGET_LIDAR_ALT = 3.0  # m
LIDAR_KP = 0.8          # 비례 계수 (튜닝)
MAX_CLIMB_RATE = 0.8   # m/s
MAX_DESCEND_RATE = 1.0 # m/s
# 로컬 저장 경로
try:
    SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
except NameError:
    SCRIPT_DIR = os.getcwd()
PHOTO_DIR = os.getenv("PHOTO_DIR", SCRIPT_DIR)

def _utc_ts_name(dt_utc: datetime) -> str:
    return dt_utc.strftime("%Y%m%dT%H%M%SZ") + ".jpg"

# ================== 환경/클라이언트 ==================
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SERVICE_ACCOUNT_PATH
os.makedirs(PHOTO_DIR, exist_ok=True)

subscriber = pubsub_v1.SubscriberClient()
publisher = pubsub_v1.PublisherClient()
storage_client = storage.Client() if (storage is not None) else None

command_subscription_path = subscriber.subscription_path(PROJECT_ID, COMMAND_SUBSCRIPTION_ID)
status_topic_path = publisher.topic_path(PROJECT_ID, STATUS_TOPIC_ID)
image_status_topic_path = publisher.topic_path(PROJECT_ID, IMAGE_STATUS_TOPIC_ID)

# ================== 라이다 센서 전역 변수 ==================
current_lidar_distance = None       # 현재 라이다 거리(cm)
lidar_last_update = None            # 마지막 업데이트 시간
lidar_lock = Lock()
lidar_thread_stop = Event()
lidar_connection = None

# ================== 최근 수신 원문 보관 ==================
_last_received_message = None
_last_received_at = None
_msg_lock = Lock()
def set_last_received_message(msg: str):
    global _last_received_message, _last_received_at
    with _msg_lock:
        _last_received_message = (msg or "").strip()
        _last_received_at = datetime.now(timezone.utc)

# ================== 라즈베리파이 시리얼 ==================
def get_pi_serial_full():
    serial = None
    try:
        with open("/proc/cpuinfo", "r") as f:
            for line in f:
                if line.lower().startswith("serial"):
                    parts = line.strip().split(":")
                    if len(parts) == 2:
                        serial = parts[1].strip().lower()
                    break
    except Exception:
        pass
    return serial

def get_pi_serial_last4():
    s = get_pi_serial_full()
    return s[-4:] if s and len(s) >= 4 else None

LOCAL_SERIAL_FULL = (get_pi_serial_full() or "")
LOCAL_SERIAL_LAST4 = (LOCAL_SERIAL_FULL[-4:] if LOCAL_SERIAL_FULL else "").lower()
DRONE_SERIAL = LOCAL_SERIAL_FULL or LOCAL_SERIAL_LAST4 or "unknown"

def _extract_target_last4(payload: dict):
    v = payload.get("serialNum")
    drone = payload.get("droneId")
    farm = payload.get("farmId")
    ndvi = None
    if not v:
        return None
    s = str(v).strip().lower()
    try:
        ndvi = payload.get("ndviId")
    finally:
        pass
    return (s[-4:] if len(s) >= 4 else None), farm, drone, ndvi

# ================== 라이다 센서 스레드 ==================
def lidar_receiver_thread():
    """
    별도 스레드에서 /dev/ttyAMA0의 DISTANCE_SENSOR 메시지를 수신하여
    current_lidar_distance를 업데이트
    """
    global current_lidar_distance, lidar_last_update, lidar_connection
    
    print(f"[lidar] 센서 연결 시도: {LIDAR_SERIAL_PORT}@{LIDAR_BAUD}")
    
    try:
        # ✅ 올바른 시리얼 연결 방법
        lidar_connection = mavutil.mavlink_connection(
            LIDAR_SERIAL_PORT,      # '/dev/ttyAMA0'
            baud=LIDAR_BAUD,        # 57600
            source_system=255       # 고유 시스템 ID
        )
        
        print("[lidar] 연결 성공, 하트비트 대기 중...")
        
        # 하트비트 대기 (선택적, 센서가 보내면)
        try:
            lidar_connection.wait_heartbeat(timeout=3)
            print(f"[lidar] 하트비트 수신: System {lidar_connection.target_system}")
        except Exception:
            print("[lidar] 하트비트 없음 (센서 직접 연결)")
        
        # DISTANCE_SENSOR 스트림 요청
        print("[lidar] DISTANCE_SENSOR 스트림 요청...")
        lidar_connection.mav.request_data_stream_send(
            lidar_connection.target_system,
            lidar_connection.target_component,
            mavutil.mavlink.MAV_DATA_STREAM_EXTRA3,
            10,  # 10Hz
            1    # 활성화
        )
        
        print("[lidar] 데이터 수신 대기 중...")
        
        while not lidar_thread_stop.is_set():
            msg = lidar_connection.recv_match(
                type='DISTANCE_SENSOR', 
                blocking=True, 
                timeout=1.0
            )
            
            if msg:
                distance_cm = msg.current_distance
                
                # 유효성 검사
                if LIDAR_MIN_DISTANCE_CM <= distance_cm <= LIDAR_MAX_DISTANCE_CM:
                    with lidar_lock:
                        current_lidar_distance = distance_cm
                        lidar_last_update = time.time()
                    print(f"[lidar] 거리: {distance_cm} cm")
                else:
                    print(f"[lidar] 범위 밖 거리 무시: {distance_cm} cm")
            else:
                # 타임아웃은 조용히 넘어감 (너무 많이 출력되므로)
                pass
                
    except Exception as e:
        print(f"[lidar] 센서 연결/수신 실패: {e}")
        print("[lidar] 라이다 없이 barometer 고도로 대체됩니다.")
    
    print("[lidar] 수신 스레드 종료")

def lidar_altitude_control():
    lidar_alt = get_lidar_altitude_m()
    if lidar_alt is None:
        return 0.0  # 보정 안 함

    error = TARGET_LIDAR_ALT - lidar_alt
    vz = LIDAR_KP * error

    # 상승/하강 속도 제한
    vz = max(-MAX_CLIMB_RATE, min(MAX_CLIMB_RATE, vz))
    return vz

    
def get_lidar_altitude_m():
    """
    라이다 센서로 측정한 고도(m) 반환
    - 센서 데이터가 유효하면 cm → m 변환
    - 타임아웃/범위 밖이면 None 반환
    """
    with lidar_lock:
        if current_lidar_distance is None:
            return None
        if lidar_last_update is None:
            return None
        
        # 타임아웃 체크
        age = time.time() - lidar_last_update
        if age > LIDAR_TIMEOUT_SEC:
            print(f"[lidar] 데이터 오래됨 ({age:.1f}초) → 무효화")
            return None
        
        # cm → m 변환
        return current_lidar_distance / 100.0

# ================== 드론 연결 ==================
def connect_vehicle():
    print(f"[boot] 드론 연결 중... (mode={CONNECTION_MODE})")
    if CONNECTION_MODE == "udp":
        v = connect(UDP_CONNECTION_STRING, wait_ready=False)
        print(f"[boot] 연결 완료 (UDP={UDP_CONNECTION_STRING}, Pi serial={LOCAL_SERIAL_LAST4 or 'unknown'})")
        return v
    elif CONNECTION_MODE == "serial":
        v = connect(SERIAL_PORT, baud=SERIAL_BAUD, wait_ready=False)
        print(f"[boot] 연결 완료 (SERIAL={SERIAL_PORT}@{SERIAL_BAUD}, Pi serial={LOCAL_SERIAL_LAST4 or 'unknown'})")
        return v
    else:
        raise ValueError(f"알 수 없는 CONNECTION_MODE: {CONNECTION_MODE}")

vehicle = connect_vehicle()

# ================== 전역 상태/이벤트 ==================
saved_waypoints = []
vehicle._resumed = False
vehicle._resume_wp_index = None

drone_id = 1

_pause_lock = Lock()
_pause_resume_evt = Event()
_pause_cancel_evt = Event()
_pause_token = 0

_loiter_to_wpindex_map = {}
_loiter_capture_tokens = {}
_capture_cancel_evt = Event()
_wp_watch_stop = Event()

_mission_active = Event()
_captured_photos = []
_captured_lock = Lock()
_upload_thread_running = Event()

# ======== 세션 메타데이터 ========
CURRENT_NDVI_ID = "0"
CURRENT_FARM_ID = "0"
CURRENT_DRONE_ID = "0"
CURRENT_FLIGHT_SESSION_ID = "FLIGHT_0"

def _normalize_new_value(v):
    if v is None:
        return None
    s = str(v).strip()
    if s == "":
        return None
    return s

def _update_session_meta_from_cmd(cmd: dict):
    global CURRENT_NDVI_ID, CURRENT_FARM_ID, CURRENT_DRONE_ID, CURRENT_FLIGHT_SESSION_ID

    ndvi_candidate = cmd.get("ndvi_id") or cmd.get("ndviId")
    ndvi_norm = _normalize_new_value(ndvi_candidate)
    if ndvi_norm is not None:
        CURRENT_NDVI_ID = ndvi_norm
        print(f"[meta] ndvi_id={CURRENT_NDVI_ID}")

    farm_candidate = cmd.get("farm_id") or cmd.get("farmId")
    farm_norm = _normalize_new_value(farm_candidate)
    if farm_norm is not None:
        CURRENT_FARM_ID = farm_norm
        print(f"[meta] farm_id={CURRENT_FARM_ID}")

    drone_candidate = cmd.get("drone_id") or cmd.get("droneId")
    drone_norm = _normalize_new_value(drone_candidate)
    if drone_norm is not None:
        CURRENT_DRONE_ID = drone_norm
        print(f"[meta] drone_id={CURRENT_DRONE_ID}")

    flight_candidate = cmd.get("flight_session_id") or cmd.get("flightSessionId")
    flight_norm = _normalize_new_value(flight_candidate)
    if flight_norm is not None:
        CURRENT_FLIGHT_SESSION_ID = flight_norm
        print(f"[meta] flight_session_id={CURRENT_FLIGHT_SESSION_ID}")

# ================== 파라미터/속도 유틸 ==================
def set_param_and_wait(name: str, value, timeout=5.0):
    try:
        vehicle.parameters[name] = value
        t0 = time.time()
        while time.time() - t0 < timeout:
            cur = vehicle.parameters.get(name, None)
            try:
                if cur is not None and float(cur) == float(value):
                    return True
            except Exception:
                pass
            time.sleep(0.2)
    except Exception as e:
        print(f"[warn] set_param({name}) 실패: {e}")
    print(f"[warn] param set timeout: {name} -> {value}")
    return False

def apply_groundspeed_limits_and_set(speed_m_s: float):
    sp_cms = int(max(0.1, float(speed_m_s)) * 100)
    print(f"[speed] 목표 지상속도 {speed_m_s:.2f} m/s 적용 중 (상한 {sp_cms} cm/s)")
    set_param_and_wait("WPNAV_SPEED", sp_cms)
    try: set_param_and_wait("PSC_VELXY_MAX", sp_cms)
    except Exception: pass
    try: set_param_and_wait("WPNAV_ACCEL", 300)
    except Exception: pass
    try: set_param_and_wait("PSC_ACC_XY", 300.0)
    except Exception: pass
    change_speed(speed_m_s)

def change_speed(speed_m_s):
    msg = vehicle.message_factory.command_long_encode(
        0, 0, mavutil.mavlink.MAV_CMD_DO_CHANGE_SPEED, 0,
        1, float(speed_m_s), -1, 0, 0, 0, 0
    )
    vehicle.send_mavlink(msg)
    vehicle.flush()
    print(f"[speed] DO_CHANGE_SPEED → {speed_m_s:.2f} m/s")

# ================== 지오펜스 ==================
def apply_home_geofence(radius_m: float):
    try:
        vehicle.parameters["FENCE_ENABLE"] = 1 if radius_m > 0 else 0
    except Exception:
        pass
    if radius_m <= 0:
        print("[geofence] 비활성화됨")
        return False
    ok = True
    ok &= set_param_and_wait("FENCE_ENABLE", 1)
    ok &= set_param_and_wait("FENCE_TYPE", 1)
    ok &= set_param_and_wait("FENCE_RADIUS", float(radius_m))
    print(f"[geofence] 적용 결과: {ok} (반경={radius_m}m)")
    return ok

def _meters_per_deg_lon(lat_deg: float) -> float:
    return 111320.0 * math.cos(math.radians(lat_deg))

def _clamp_to_radius(lat0, lon0, lat, lon, radius_m):
    if None in (lat0, lon0, lat, lon):
        return lat, lon, False
    dlat_m = (lat - lat0) * 111320.0
    dlon_m = (lon - lon0) * _meters_per_deg_lon(lat0)
    dist = math.hypot(dlat_m, dlon_m)
    if dist <= radius_m or dist == 0.0:
        return lat, lon, False
    scale = radius_m / dist
    dlat_m *= scale; dlon_m *= scale
    new_lat = lat0 + (dlat_m / 111320.0)
    new_lon = lon0 + (dlon_m / _meters_per_deg_lon(lat0))
    return new_lat, new_lon, True

def _get_home_latlon():
    try:
        hl = getattr(vehicle, "home_location", None)
        if hl and getattr(hl, "lat", None) is not None and getattr(hl, "lon", None) is not None:
            return float(hl.lat), float(hl.lon)
    except Exception:
        pass
    loc = vehicle.location.global_frame
    return getattr(loc, "lat", None), getattr(loc, "lon", None)

# ================== 상태 유틸 ==================
def is_recent(timestamp_str):
    try:
        ts = datetime.fromisoformat((timestamp_str or "").replace("Z", "+00:00"))
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)
        age = (now - ts).total_seconds()
        print(f"[ts] 메시지:{ts.isoformat()} 현재:{now.isoformat()} 경과:{age:.2f}s")
        return age <= MAX_AGE_SEC
    except Exception as e:
        print("[ts] timestamp 파싱 실패:", e)
        return False

def get_state(vehicle):
    """
    드론 상태 가져오기 (라이다 고도 포함)
    """
    loc = vehicle.location.global_relative_frame
    
    # 라이다 고도 가져오기
    lidar_alt = get_lidar_altitude_m()
    
    with _msg_lock:
        last_msg = _last_received_message
        last_at_fmt = None
        if _last_received_at:
            last_at_fmt = _last_received_at.strftime("%Y-%m-%d %H:%M:%S")
    
    state = {
        "serial_num": LOCAL_SERIAL_LAST4 or None,
        "arming_status": vehicle.armed,
        "mode": str(vehicle.mode.name),
        "altitude": getattr(loc, "alt", None),          # Barometer 고도
        "lidar_altitude": lidar_alt,                     # 라이다 고도 (신규)
        "latitude": getattr(loc, "lat", None),
        "longitude": getattr(loc, "lon", None),
        "heading": getattr(vehicle, "heading", None),
        "airspeed": getattr(vehicle, "airspeed", None),
        "groundspeed": int(round(getattr(vehicle, "groundspeed", 0.0))),
        "battery_voltage": getattr(getattr(vehicle, "battery", None), "voltage", None),
        "battery_current": getattr(getattr(vehicle, "battery", None), "current", None),
        "battery_charge": getattr(getattr(vehicle, "battery", None), "level", None),
        "last_received_message": last_msg,
        "last_received_at": last_at_fmt,
        "geofence_radius_m": None,
    }
    return state

# ================== EXIF 메타데이터 ==================
def _deg_to_dms_rational(deg_float):
    deg = abs(float(deg_float))
    d = int(deg)
    m_float = (deg - d) * 60
    m = int(m_float)
    s = round((m_float - m) * 60 * 100, 0)
    return ((d, 1), (m, 1), (int(s), 100))

def embed_gps_time_metadata(filepath, lat, lon, alt, timestamp_utc: datetime):
    if piexif is None or Image is None:
        print("[metadata] piexif/Pillow 미로딩 → 메타데이터 삽입 스킵")
        return False
    try:
        exif_dict = piexif.load(filepath)
        gps_ifd = {}
        if lat is not None and lon is not None:
            gps_ifd[piexif.GPSIFD.GPSLatitudeRef] = 'N' if lat >= 0 else 'S'
            gps_ifd[piexif.GPSIFD.GPSLatitude] = _deg_to_dms_rational(lat)
            gps_ifd[piexif.GPSIFD.GPSLongitudeRef] = 'E' if lon >= 0 else 'W'
            gps_ifd[piexif.GPSIFD.GPSLongitude] = _deg_to_dms_rational(lon)
        if alt is not None:
            gps_ifd[piexif.GPSIFD.GPSAltitudeRef] = 0
            gps_ifd[piexif.GPSIFD.GPSAltitude] = (int(abs(float(alt) * 100)), 100)
        gps_ifd[piexif.GPSIFD.GPSTimeStamp] = (
            (timestamp_utc.hour, 1),
            (timestamp_utc.minute, 1),
            (timestamp_utc.second, 1)
        )
        gps_ifd[piexif.GPSIFD.GPSDateStamp] = timestamp_utc.strftime("%Y:%m:%d")
        exif_dict["GPS"] = gps_ifd

        exif_ifd = exif_dict.get("Exif", {})
        dt_str = timestamp_utc.strftime("%Y:%m:%d %H:%M:%S")
        exif_ifd[piexif.ExifIFD.DateTimeOriginal] = dt_str
        exif_ifd[piexif.ExifIFD.DateTimeDigitized] = dt_str
        exif_dict["Exif"] = exif_ifd

        exif_bytes = piexif.dump(exif_dict)
        piexif.insert(exif_bytes, filepath)
        print(f"[metadata] GPS+시간 삽입 완료: {filepath}")
        return True
    except Exception as e:
        print(f"[metadata] 삽입 실패: {e}")
        return False

# ================== GCS 유틸 ==================
GCS_BUCKET = "tlatfarm-image"

def _safe_dir(v: str | None) -> str:
    if v is None:
        return "null"
    s = str(v).strip()
    return s if s != "" else "null"

def _gcs_rel_path_for_timestamp(ts_utc: datetime) -> str:
    ts_name = _utc_ts_name(ts_utc)
    farm_seg = _safe_dir(CURRENT_FARM_ID)
    drone_seg = _safe_dir(CURRENT_DRONE_ID)
    serial_seg = _safe_dir(DRONE_SERIAL)
    return f"rgb/src/farm-{farm_seg}/drone-{drone_seg}/{ts_name}"

def upload_to_gcs(local_path: str, gcs_rel_path: str):
    if storage_client is None:
        print("[gcs] storage_client 없음 → 업로드 스킵")
        return None, None
    bucket = storage_client.bucket(GCS_BUCKET)
    blob = bucket.blob(gcs_rel_path)
    last_err = None
    for attempt in range(3):
        try:
            blob.upload_from_filename(local_path, content_type="image/jpeg")
            return f"gs://{GCS_BUCKET}/{gcs_rel_path}", None
        except Exception as e:
            last_err = e
            print(f"[gcs] 업로드 리트라이 {attempt+1}/3: {e}")
            time.sleep(0.8)
    print(f"[gcs] 업로드 실패 최종: {local_path} → {last_err}")
    return None, None

# ================== 카메라 촬영 ==================
_picamera2 = None
def _init_picamera2_if_needed():
    global _picamera2
    if CAMERA_BACKEND != "picamera2":
        return False
    if _picamera2 is not None:
        return True
    try:
        from picamera2 import Picamera2
        _picamera2 = Picamera2()
        config = _picamera2.create_still_configuration()
        _picamera2.configure(config)
        _picamera2.start()
        print("[camera] Picamera2 초기화 완료")
        return True
    except Exception as e:
        print(f"[camera] Picamera2 초기화 실패: {e}")
        _picamera2 = None
        return False

def take_photo_for_waypoint(wp_reached_index: int):
    """
    LOITER 종료 1초 전: 로컬 저장(+메타데이터)
    고도는 라이다 우선, 없으면 barometer 사용
    """
    loc_abs = vehicle.location.global_frame
    loc_rel = vehicle.location.global_relative_frame
    lat = getattr(loc_abs, "lat", None)
    lon = getattr(loc_abs, "lon", None)
    
    # 라이다 고도 우선, 없으면 barometer
    alt = get_lidar_altitude_m()
    if alt is None:
        alt = getattr(loc_rel, "alt", None)
        print(f"[camera] 라이다 없음 → barometer 고도 사용: {alt:.2f}m")
    else:
        print(f"[camera] 라이다 고도 사용: {alt:.2f}m")

    ts_utc = datetime.now(timezone.utc)
    filename = _utc_ts_name(ts_utc)
    local_filepath = os.path.join(PHOTO_DIR, filename)

    print(f"[camera] 촬영 시작: WP#{wp_reached_index}, 파일={local_filepath}")
    try:
        if CAMERA_BACKEND == "picamera2" and _init_picamera2_if_needed():
            _picamera2.capture_file(local_filepath)
        else:
            cmd = ["libcamera-still", "-n", "-o", local_filepath]
            subprocess.run(cmd, timeout=PHOTO_CAPTURE_TIMEOUT, check=True)
        print(f"[camera] 촬영 성공: {local_filepath}")

        embed_gps_time_metadata(local_filepath, lat, lon, alt, ts_utc)
        gcs_rel_path = _gcs_rel_path_for_timestamp(ts_utc)

        with _captured_lock:
            _captured_photos.append((
                local_filepath, gcs_rel_path, wp_reached_index, lat, lon, alt, ts_utc.isoformat()
            ))
            print(f"[camera] 큐 적재: 총 {len(_captured_photos)}개")
    except Exception as e:
        print(f"[camera] 촬영 실패: {e}")
        local_filepath = None

    try:
        payload = {
            "event": "photo_captured_local",
            "serial_num": DRONE_SERIAL,
            "wp_index_reached": int(wp_reached_index),
            "file_path": local_filepath,
            "lat": lat, "lon": lon, "alt": alt,
            "timestamp_utc": ts_utc.isoformat(),
        }
        publisher.publish(status_topic_path, data=json.dumps(payload).encode("utf-8"))
        print(f"[pubsub] photo_captured_local 이벤트 발행: wp={wp_reached_index}")
    except Exception as e:
        print(f"[pubsub] 이벤트 발행 실패: {e}")

# ================== LOITER 시작 감지 → 종료 1초 전 촬영 ==================
def waypoint_watcher():
    last_next = None
    print("[wpwatch] 웨이포인트 감시 시작")
    while not _wp_watch_stop.is_set():
        try:
            cur_next = getattr(vehicle.commands, "next", None)
            if isinstance(cur_next, int):
                if cur_next in _loiter_to_wpindex_map and cur_next != last_next:
                    loiter_index = cur_next
                    wp_num = _loiter_to_wpindex_map[loiter_index]
                    wait_sec = max(0.0, LOITER_SEC - CAPTURE_OFFSET_SEC)
                    print(f"[wpwatch] LOITER 시작 @idx {loiter_index} (WP#{wp_num}) → {wait_sec:.2f}s 후 촬영 예약")

                    token = _loiter_capture_tokens.get(loiter_index, 0) + 1
                    _loiter_capture_tokens[loiter_index] = token

                    def _delayed_capture(lo_idx, wp_no, my_token, delay_s):
                        t_end = time.time() + delay_s
                        while time.time() < t_end:
                            if _capture_cancel_evt.is_set():
                                print(f"[wpwatch] 캡처 예약 취소(글로벌), loiter={lo_idx}")
                                return
                            if _loiter_capture_tokens.get(lo_idx) != my_token:
                                print(f"[wpwatch] 캡처 예약 무효화(새 토큰), loiter={lo_idx}")
                                return
                            time.sleep(0.05)
                        cur_n = getattr(vehicle.commands, "next", None)
                        if cur_n != lo_idx:
                            print(f"[wpwatch] LOITER 종료 감지(조기), 촬영 스킵 loiter={lo_idx}")
                            return
                        if _capture_cancel_evt.is_set():
                            print(f"[wpwatch] 캡처 직전 취소, loiter={lo_idx}")
                            return
                        print(f"[wpwatch] LOITER 종료 1초 전 촬영 실행 (WP#{wp_no})")
                        take_photo_for_waypoint(wp_no)

                    Thread(target=_delayed_capture, args=(loiter_index, wp_num, token, wait_sec), daemon=True).start()

                last_next = cur_next
        except Exception as e:
            print(f"[wpwatch] 오류: {e}")
        time.sleep(0.05)
    print("[wpwatch] 웨이포인트 감시 종료")

# ================== 업로드 & 이미지 상태 발행 ==================
def _publish_image_status_batch_done():
    msg = {
        "image_type": "rgb",
        "upload_status": "done",
        "ndvi_id": CURRENT_NDVI_ID,
        "farm_id": CURRENT_FARM_ID,
        "drone_id": CURRENT_DRONE_ID,
        "drone_serial": DRONE_SERIAL,
        "flight_session_id": CURRENT_FLIGHT_SESSION_ID
    }
    try:
        publisher.publish(
            image_status_topic_path,
            data=json.dumps(msg).encode("utf-8")
        )
        print(f"[pubsub] drone-image-status 발행(배치 완료): {msg}")
    except Exception as e:
        print(f"[pubsub] drone-image-status 발행 실패: {e}")

def _do_batch_upload(tag: str = "manual_or_disarm"):
    if _upload_thread_running.is_set():
        print("[uploader] 이미 동작 중, 중복 실행 스킵")
        return

    def _worker():
        _upload_thread_running.set()
        try:
            with _captured_lock:
                items = list(_captured_photos)
            print(f"[uploader] 시작({tag}): 큐 {len(items)}개 → bucket={GCS_BUCKET}")
            if not items:
                return

            results = []
            for (local_path, gcs_rel_path, wp_no, lat, lon, alt, ts) in items:
                gcs_uri, _ = upload_to_gcs(local_path, gcs_rel_path)
                results.append({
                    "wp_index": wp_no,
                    "local_path": local_path,
                    "gcs_uri": gcs_uri,
                    "lat": lat, "lon": lon, "alt": alt,
                    "captured_at": ts,
                    "uploaded_at": datetime.now(timezone.utc).isoformat()
                })
                print(f"[uploader] {os.path.basename(local_path)} → {gcs_uri or 'UPLOAD_FAIL'}")

            payload = {
                "event": "photos_uploaded_batch",
                "serial_num": DRONE_SERIAL,
                "total": len(results),
                "bucket": GCS_BUCKET,
                "items": results,
                "timestamp_utc": datetime.now(timezone.utc).isoformat(),
                "tag": tag,
            }
            publisher.publish(status_topic_path, data=json.dumps(payload).encode("utf-8"))
            print(f"[pubsub] photos_uploaded_batch 이벤트 발행 ({len(results)}개, tag={tag})")

            _publish_image_status_batch_done()

            with _captured_lock:
                _captured_photos.clear()
        finally:
            _upload_thread_running.clear()

    Thread(target=_worker, daemon=True).start()

def _armed_listener(vehicle_obj, attr_name, value):
    try:
        print(f"[armed] 상태 변화: {value}")
        if value is False:
            print("[armed] Disarm 감지 → 업로드 트리거")
            _do_batch_upload(tag="disarm")
    except Exception as e:
        print(f"[armed] 리스너 오류: {e}")

try:
    vehicle.add_attribute_listener('armed', _armed_listener)
    print("[boot] armed 리스너 등록 완료")
except Exception as e:
    print("[boot] armed 리스너 등록 실패:", e)

# ================== 비행 관련 (라이다 고도 제어) ==================
def arm_and_takeoff(aTargetAltitude):
    """
    이륙 함수 - 라이다 센서 기반 고도 제어
    """
    print("[flight] GUIDED로 전환 및 이륙 준비")
    vehicle.mode = VehicleMode("GUIDED")
    while vehicle.mode.name != "GUIDED":
        print("[flight] GUIDED 모드 대기...")
        time.sleep(1)
    
    vehicle.armed = True
    while not vehicle.armed:
        print("[flight] 시동 대기...")
        time.sleep(1)
    
    if aTargetAltitude > 0:
        print(f"[flight] 이륙 중... 목표: {aTargetAltitude}m")
        vehicle.simple_takeoff(aTargetAltitude)
        
        while True:
            # 라이다 고도 우선 사용
            if LIDAR_USE_FOR_ALTITUDE:
                lidar_alt = get_lidar_altitude_m()
                if lidar_alt is not None:
                    alt = lidar_alt
                    alt_source = "LIDAR"
                else:
                    alt = vehicle.location.global_relative_frame.alt
                    alt_source = "BARO"
            else:
                alt = vehicle.location.global_relative_frame.alt
                alt_source = "BARO"
            
            print(f"[flight] 현재 고도: {alt:.2f}m ({alt_source})")
            
            if alt >= aTargetAltitude * 0.95:
                print("[flight] 목표 고도 도달")
                break
            
            time.sleep(1)

# ================== 명령 처리 ==================
def handle_command(command_data):
    global saved_waypoints, _pause_token
    command = (command_data.get("command") or "").lower().strip()

    _update_session_meta_from_cmd(command_data)

    if command not in ("resume", "stop_mission"):
        _pause_cancel_evt.set()

    if command == "arm":
        arm_and_takeoff(0)

    elif command == "disarm":
        print("[cmd] 모터 시동 해제 중...")
        _capture_cancel_evt.set(); time.sleep(0.05); _capture_cancel_evt.clear()
        vehicle.armed = False
        while vehicle.armed:
            print("[cmd] 시동 해제 대기...")
            time.sleep(1)
        print("[cmd] 시동 해제 완료")

    elif command == "upload_now":
        print("[cmd] 수동 업로드 트리거")
        _do_batch_upload(tag="manual_command")

    elif command == "waypoints":
        waypoints = command_data.get("waypoints", [])
        if waypoints:
            saved_waypoints = waypoints
            print(f"[cmd] {len(saved_waypoints)}개 웨이포인트 저장")
        else:
            print("[cmd] 웨이포인트 정보 없음")

    elif command == "start_mission":
        if not saved_waypoints:
            print("[cmd] 웨이포인트가 없습니다.")
            return

        with _captured_lock:
            _captured_photos.clear()
        _mission_active.set()

        apply_home_geofence( float(os.getenv("GEOFENCE_RADIUS_M", "50.0")) )
        home_lat, home_lon = _get_home_latlon()
        if None in (home_lat, home_lon):
            print("[geofence] 홈 좌표 확인 실패 → 클램프 생략")
        else:
            print(f"[geofence] 홈: lat={home_lat:.7f}, lon={home_lon:.7f}")

        print("[cmd] 미션 업로드...")
        cmds = vehicle.commands
        cmds.clear()
        cmds.wait_ready()

        global _loiter_to_wpindex_map, _loiter_capture_tokens
        _loiter_to_wpindex_map = {}
        _loiter_capture_tokens = {}

        adjusted = 0
        mission_index = 0

        for i, wp in enumerate(saved_waypoints, start=1):
            lat = wp.get("lat")
            lon = wp.get("lon") or wp.get("lng")
            alt = wp.get("alt", MISSION_ALTITUDE)

            if None not in (home_lat, home_lon, lat, lon):
                new_lat, new_lon, changed = _clamp_to_radius(
                    home_lat, home_lon,
                    float(lat), float(lon),
                    float(os.getenv("GEOFENCE_RADIUS_M", "50.0"))
                )
                if changed:
                    adjusted += 1
                    print(f"[geofence] WP 클램프: ({lat:.7f},{lon:.7f}) → ({new_lat:.7f},{new_lon:.7f})")
                    lat, lon = new_lat, new_lon

            cmds.add(Command(
                0,0,0,
                mavutil.mavlink.MAV_FRAME_GLOBAL_RELATIVE_ALT,
                mavutil.mavlink.MAV_CMD_NAV_WAYPOINT,
                0,0,0,0,0,0,
                lat, lon, alt
            ))
            mission_index += 1

            cmds.add(Command(
                0,0,0,
                mavutil.mavlink.MAV_FRAME_GLOBAL_RELATIVE_ALT,
                mavutil.mavlink.MAV_CMD_NAV_LOITER_TIME,
                0,0, float(LOITER_SEC), 0,0,0,
                lat, lon, alt
            ))
            mission_index += 1

            _loiter_to_wpindex_map[mission_index] = i

        if adjusted > 0:
            print(f"[geofence] 총 {adjusted}개 WP 보정됨")

        cmds.add(Command(
            0,0,0,
            mavutil.mavlink.MAV_FRAME_GLOBAL_RELATIVE_ALT,
            mavutil.mavlink.MAV_CMD_NAV_RETURN_TO_LAUNCH,
            0,0,0,0,0,0,
            0,0,0
        ))
        cmds.upload()
        print(f"[cmd] 업로드 완료: WP {len(saved_waypoints)}개 × (WP+LOITER) + RTL")

        apply_groundspeed_limits_and_set(DEFAULT_GROUNDSPEED)

        _wp_watch_stop.clear()
        Thread(target=waypoint_watcher, daemon=True).start()

        arm_and_takeoff(MISSION_ALTITUDE)
        print("[flight] AUTO 전환 중...")
        vehicle.mode = VehicleMode("AUTO")
        while vehicle.mode.name != "AUTO":
            print("[flight] AUTO 대기...")
            time.sleep(1)
        print("[flight] 미션 시작")

    elif command == "stop_mission":
        print("[cmd] 미션 일시정지 요청")
        _capture_cancel_evt.set(); time.sleep(0.05); _capture_cancel_evt.clear()

        vehicle.commands.download()
        vehicle.commands.wait_ready()
        vehicle._resume_wp_index = vehicle.commands.next
        vehicle._resumed = False

        vehicle.mode = VehicleMode("GUIDED")
        while vehicle.mode.name != "GUIDED":
            print("[cmd] GUIDED 대기...")
            time.sleep(1)

        with _pause_lock:
            _pause_token += 1
            local_token = _pause_token
            _pause_resume_evt.clear()
            _pause_cancel_evt.clear()

        print(f"[cmd] 10초 재개 대기 (resume_idx={vehicle._resume_wp_index}, token={local_token})")

        def wait_for_resume_or_cancel(token, timeout=10):
            t_end = time.time() + timeout
            while time.time() < t_end:
                if token != _pause_token:
                    print("[cmd] 새로운 stop 감지 → 이전 대기 종료")
                    return
                if _pause_cancel_evt.is_set():
                    print("[cmd] 다른 명령 감지 → 일시정지 취소")
                    return
                if _pause_resume_evt.is_set():
                    print("[cmd] 재개 → AUTO 복귀")
                    if vehicle._resume_wp_index is not None:
                        vehicle.commands.next = vehicle._resume_wp_index
                    vehicle.mode = VehicleMode("AUTO")
                    return
                time.sleep(0.2)
            if (
                token == _pause_token
                and not _pause_cancel_evt.is_set()
                and not _pause_resume_evt.is_set()
            ):
                print("[cmd] 재개 없음 → RTL")
                vehicle.mode = VehicleMode("RTL")
                while vehicle.mode.name != "RTL":
                    time.sleep(0.5)

        Thread(target=wait_for_resume_or_cancel, args=(local_token, 10), daemon=True).start()

    elif command == "resume":
        print("[cmd] resume 수신")
        vehicle._resumed = True
        _pause_resume_evt.set()

    elif command == "set_speed":
        speed = command_data.get("speed")
        if speed is not None:
            sp = float(speed)
            print(f"[cmd] 지상속도 {sp:.2f} m/s 설정")
            apply_groundspeed_limits_and_set(sp)
        else:
            print("[cmd] 속도 값 없음")

    else:
        print(f"[cmd] 알 수 없는 명령: {command}")

# ================== Pub/Sub 콜백 ==================
def callback(message):
    print("[sub] 메시지 수신")
    try:
        data = message.data.decode("utf-8")
        json_data = json.loads(data)
        print("[sub] 명령:", json.dumps(json_data, indent=2))
        print("==========\n")
        print(data)
        print("==========\n")
        set_last_received_message(data)

        target_last4, farm_id_recv, drone_id_recv, ndvi_id_recv = _extract_target_last4(json_data)

        if not LOCAL_SERIAL_LAST4:
            print("[sub] local last4 없음 → 무시")
            return
        if not target_last4:
            print("[sub] serialNum 없음 → 무시")
            return
        if target_last4 != LOCAL_SERIAL_LAST4:
            print(f"[sub] 시리얼 불일치 target={target_last4}, local={LOCAL_SERIAL_LAST4} → 무시")
            return

        if not is_recent(json_data.get("timestamp")):
            print("[sub] 오래된 메시지 → 무시")
            return

        Thread(target=handle_command, args=(json_data,), daemon=True).start()

    except Exception as e:
        print("[sub] 처리 실패:", e)
        print("[sub] 원본:", message.data)
    finally:
        message.ack()

# ================== 상태 주기 전송 ==================
def publish_state_loop():
    while True:
        try:
            state = get_state(vehicle)
            message_json = json.dumps(state)
            publisher.publish(status_topic_path, data=message_json.encode("utf-8"))
        except Exception as e:
            print("드론 상태 전송 실패:", e)
        time.sleep(2)

# ================== 시작 ==================
apply_home_geofence(float(os.getenv("GEOFENCE_RADIUS_M", "50.0")))

# 라이다 센서 스레드 시작
print("[boot] 라이다 센서 스레드 시작")
lidar_thread = Thread(target=lidar_receiver_thread, daemon=True)
lidar_thread.start()

print(f"[boot] Pub/Sub 명령 구독 시작: {command_subscription_path}")
streaming_future = subscriber.subscribe(command_subscription_path, callback=callback)
Thread(target=publish_state_loop, daemon=True).start()

try:
    while True:
        time.sleep(0.5)
except KeyboardInterrupt:
    print("[boot] 종료 시퀀스")
    
    # 라이다 스레드 종료
    lidar_thread_stop.set()
    
    _wp_watch_stop.set()
    try:
        streaming_future.cancel()
    except Exception:
        pass
    try:
        vehicle.remove_attribute_listener('armed', _armed_listener)
    except Exception:
        pass
    try:
        if _picamera2:
            _picamera2.stop()
    except Exception:
        pass
    
    # 라이다 연결 종료
    if lidar_connection:
        try:
            lidar_connection.close()
        except Exception:
            pass
    
    vehicle.close()
    print("[boot] 종료 완료")