#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
ArduCopter 자율비행 + 라이다 기반 고도 판단 + 클라우드 웨이포인트 + 웨이포인트 촬영
- 라이다: Pixhawk가 처리한 DISTANCE_SENSOR(MAVLink) 수신
- 웨이포인트: Google Cloud Pub/Sub 명령 구독
- 미션: AUTO로 FC에 업로드 후 실행
- 촬영: LOITER 종료 1초 전 자동 촬영 + GCS 업로드
- DroneKit 제거, pymavlink-only
"""

import os
import json
import time
import math
import subprocess
from dataclasses import dataclass
from threading import Thread, Lock, Event
from datetime import datetime, timezone

from pymavlink import mavutil

# Google Cloud
from google.cloud import pubsub_v1
try:
    from google.cloud import storage  # type: ignore
except Exception as e:
    storage = None
    print(f"[boot] google-cloud-storage 미로딩: {e}")

# EXIF 메타데이터
try:
    import piexif
    from PIL import Image  # noqa: F401
except Exception as e:
    piexif = None
    Image = None
    print(f"[boot] EXIF 메타데이터 모듈 미로딩: {e}")

# ========================================
# 전역 설정
# ========================================

# Google Cloud
SERVICE_ACCOUNT_PATH = "/home/jh-ras/tlatfarm-project-f77303540ef2.json"
PROJECT_ID = "tlatfarm-project"
COMMAND_SUBSCRIPTION_ID = "drone-commands-sub"
STATUS_TOPIC_ID = "drone-status"
IMAGE_STATUS_TOPIC_ID = "drone-image-status"
GCS_BUCKET = "tlatfarm-image"

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SERVICE_ACCOUNT_PATH

# 드론 연결 (사용자 확정)
CONNECTION_STRING = "/dev/ttyAMA0"
BAUD_RATE = 57600

# 라이다 설정 (Pixhawk가 송신하는 DISTANCE_SENSOR 기반)
current_lidar_distance = None  # cm
lidar_last_update = None
lidar_lock = Lock()

LIDAR_MIN_DISTANCE_CM = 10
LIDAR_MAX_DISTANCE_CM = 1200
LIDAR_TIMEOUT_SEC = 2.0

# 비행 파라미터
MISSION_ALTITUDE = 3.0  # m (미션 altitude는 여전히 필요)
DEFAULT_GROUNDSPEED = 2.0  # m/s
WAYPOINT_RADIUS = 1.0  # m (도달 판정용)

# LOITER/촬영 타이밍
LOITER_SEC = 5
CAPTURE_OFFSET_SEC = 1.0

# 카메라
CAMERA_BACKEND = os.getenv("CAMERA_BACKEND", "libcamera").lower()
PHOTO_CAPTURE_TIMEOUT = 10

try:
    SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
except NameError:
    SCRIPT_DIR = os.getcwd()
PHOTO_DIR = os.getenv("PHOTO_DIR", SCRIPT_DIR)
os.makedirs(PHOTO_DIR, exist_ok=True)

# 지오펜스
GEOFENCE_RADIUS_M = 50.0

# 메시지 타임스탬프 검증
MAX_AGE_SEC = 5


# ========================================
# 라즈베리파이 시리얼(드론 식별)
# ========================================

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

LOCAL_SERIAL_FULL = get_pi_serial_full() or ""
LOCAL_SERIAL_LAST4 = (LOCAL_SERIAL_FULL[-4:] if LOCAL_SERIAL_FULL else "").lower()
DRONE_SERIAL = LOCAL_SERIAL_FULL or LOCAL_SERIAL_LAST4 or "unknown"
print(f"[boot] Pi Serial: {DRONE_SERIAL} (last4: {LOCAL_SERIAL_LAST4})")


# ========================================
# 세션 메타데이터
# ========================================

CURRENT_NDVI_ID = "0"
CURRENT_FARM_ID = "0"
CURRENT_DRONE_ID = "0"
CURRENT_FLIGHT_SESSION_ID = "FLIGHT_0"

def _normalize_new_value(v):
    if v is None:
        return None
    s = str(v).strip()
    return s if s != "" else None

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


# ========================================
# Google Cloud 클라이언트
# ========================================

subscriber = pubsub_v1.SubscriberClient()
publisher = pubsub_v1.PublisherClient()
storage_client = storage.Client() if (storage is not None) else None

command_subscription_path = subscriber.subscription_path(PROJECT_ID, COMMAND_SUBSCRIPTION_ID)
status_topic_path = publisher.topic_path(PROJECT_ID, STATUS_TOPIC_ID)
image_status_topic_path = publisher.topic_path(PROJECT_ID, IMAGE_STATUS_TOPIC_ID)


# ========================================
# EXIF 메타데이터 유틸
# ========================================

def _utc_ts_name(dt_utc: datetime) -> str:
    return dt_utc.strftime("%Y%m%dT%H%M%SZ") + ".jpg"

def _deg_to_dms_rational(deg_float):
    deg = abs(float(deg_float))
    d = int(deg)
    m_float = (deg - d) * 60
    m = int(m_float)
    s = round((m_float - m) * 60 * 100, 0)
    return ((d, 1), (m, 1), (int(s), 100))

def embed_gps_time_metadata(filepath, lat, lon, alt, timestamp_utc: datetime):
    if piexif is None:
        print("[metadata] piexif 미로딩 → 메타데이터 삽입 스킵")
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


# ========================================
# GCS 업로드 유틸
# ========================================

def _safe_dir(v: str | None) -> str:
    if v is None:
        return "null"
    s = str(v).strip()
    return s if s != "" else "null"

def _gcs_rel_path_for_timestamp(ts_utc: datetime) -> str:
    ts_name = _utc_ts_name(ts_utc)
    farm_seg = _safe_dir(CURRENT_FARM_ID)
    drone_seg = _safe_dir(CURRENT_DRONE_ID)
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


# ========================================
# pymavlink Vehicle 래퍼
# ========================================

@dataclass
class VehicleMode:
    name: str

@dataclass
class LocationGlobal:
    lat: float | None = None
    lon: float | None = None
    alt: float | None = None

@dataclass
class Battery:
    voltage: float | None = None
    current: float | None = None
    level: int | None = None

def _copter_mode_name(custom_mode: int) -> str:
    # ArduCopter mode mapping (주요 모드만)
    m = {
        0: "STABILIZE",
        2: "ALT_HOLD",
        3: "AUTO",
        4: "GUIDED",
        5: "LOITER",
        6: "RTL",
        9: "LAND",
        16: "POSHOLD",
        17: "BRAKE",
        20: "GUIDED_NOGPS",
    }
    return m.get(int(custom_mode), f"MODE_{custom_mode}")

def _copter_custom_mode(name: str) -> int:
    name = name.upper()
    m = {
        "STABILIZE": 0,
        "ALT_HOLD": 2,
        "AUTO": 3,
        "GUIDED": 4,
        "LOITER": 5,
        "RTL": 6,
        "LAND": 9,
        "POSHOLD": 16,
        "BRAKE": 17,
        "GUIDED_NOGPS": 20,
    }
    if name not in m:
        raise ValueError(f"Unsupported mode name: {name}")
    return m[name]

class MavVehicle:
    def __init__(self, port: str, baud: int):
        self.master = mavutil.mavlink_connection(port, baud=baud)
        print("[mav] heartbeat waiting...")
        self.master.wait_heartbeat(timeout=60)
        print("[mav] heartbeat OK")

        self.target_system = self.master.target_system
        self.target_component = self.master.target_component

        self.mode = VehicleMode("UNKNOWN")
        self.armed = False

        self.location = type("LocWrap", (), {})()
        self.location.global_frame = LocationGlobal()
        self.location.global_relative_frame = LocationGlobal()

        self.heading = None
        self.airspeed = None
        self.groundspeed = 0.0
        self.battery = Battery()

        self.parameters_cache = {}

        # mission state
        self.mission_current_seq = None  # 0-based
        self._resume_seq = None

        # armed edge detection (업로드 트리거)
        self._armed_prev = None
        self._on_disarm = None  # callback

        self._rx_stop = Event()
        self._rx_thread = Thread(target=self._rx_loop, daemon=True)
        self._rx_thread.start()

    def set_on_disarm(self, fn):
        self._on_disarm = fn

    def wait_ready(self, timeout=30):
        t0 = time.time()
        while time.time() - t0 < timeout:
            if self.location.global_frame.lat is not None:
                return True
            time.sleep(0.2)
        return True

    def close(self):
        self._rx_stop.set()
        try:
            self.master.close()
        except Exception:
            pass

    def flush(self):
        return

    def send_mavlink(self, msg):
        self.master.mav.send(msg)

    # ---- basic control ----
    def set_mode(self, mode_name: str):
        cm = _copter_custom_mode(mode_name)
        self.master.mav.set_mode_send(
            self.target_system,
            mavutil.mavlink.MAV_MODE_FLAG_CUSTOM_MODE_ENABLED,
            cm
        )

    def arm(self, do_arm: bool):
        self.master.mav.command_long_send(
            self.target_system, self.target_component,
            mavutil.mavlink.MAV_CMD_COMPONENT_ARM_DISARM,
            0,
            1 if do_arm else 0, 0, 0, 0, 0, 0, 0
        )

    def simple_takeoff(self, alt_m: float):
        self.master.mav.command_long_send(
            self.target_system, self.target_component,
            mavutil.mavlink.MAV_CMD_NAV_TAKEOFF,
            0,
            0, 0, 0, 0, 0, 0, float(alt_m)
        )

    def do_change_speed(self, speed_m_s: float):
        self.master.mav.command_long_send(
            self.target_system, self.target_component,
            mavutil.mavlink.MAV_CMD_DO_CHANGE_SPEED,
            0,
            1, float(speed_m_s), -1, 0, 0, 0, 0
        )

    def set_param(self, name: str, value: float):
        self.master.mav.param_set_send(
            self.target_system, self.target_component,
            name.encode("utf-8"),
            float(value),
            mavutil.mavlink.MAV_PARAM_TYPE_REAL32
        )
        self.parameters_cache[name] = value

    def mission_set_current(self, seq: int):
        self.master.mav.command_long_send(
            self.target_system, self.target_component,
            mavutil.mavlink.MAV_CMD_MISSION_SET_CURRENT,
            0,
            float(seq), 0, 0, 0, 0, 0, 0
        )

    # ---- mission upload ----
    def mission_upload(self, items: list[dict], timeout_s=25):
        """
        items: list of dict keys:
          command, frame, current, autocontinue, param1..param4, x(lat), y(lon), z(alt)
        uses MISSION_ITEM_INT
        """
        n = len(items)
        print(f"[mav] mission upload start: {n} items")

        # clear existing mission first
        self.master.mav.mission_clear_all_send(self.target_system, self.target_component)
        time.sleep(0.3)

        # send count
        self.master.mav.mission_count_send(self.target_system, self.target_component, n)

        sent = set()
        t_end = time.time() + timeout_s

        while time.time() < t_end and len(sent) < n:
            req = self.master.recv_match(
                type=['MISSION_REQUEST_INT', 'MISSION_REQUEST'],
                blocking=True, timeout=2
            )
            if not req:
                continue
            seq = int(req.seq)
            if seq < 0 or seq >= n or seq in sent:
                continue

            it = items[seq]
            self.master.mav.mission_item_int_send(
                self.target_system,
                self.target_component,
                seq,
                it["frame"],
                it["command"],
                it["current"],
                it["autocontinue"],
                it["param1"], it["param2"], it["param3"], it["param4"],
                int(it["x"] * 1e7),
                int(it["y"] * 1e7),
                float(it["z"])
            )
            sent.add(seq)

        ack = self.master.recv_match(type='MISSION_ACK', blocking=True, timeout=5)
        if ack:
            print("[mav] mission upload ACK:", ack.type)
            return True
        print("[mav] mission upload ACK timeout")
        return False

    # ---- internal rx ----
    def _rx_loop(self):
        global current_lidar_distance, lidar_last_update
        while not self._rx_stop.is_set():
            msg = self.master.recv_match(blocking=True, timeout=1.0)
            if not msg:
                continue
            mtype = msg.get_type()

            if mtype == "HEARTBEAT":
                try:
                    self.armed = bool(msg.base_mode & mavutil.mavlink.MAV_MODE_FLAG_SAFETY_ARMED)
                    self.mode = VehicleMode(_copter_mode_name(msg.custom_mode))

                    # disarm edge
                    if self._armed_prev is None:
                        self._armed_prev = self.armed
                    else:
                        if self._armed_prev is True and self.armed is False:
                            if callable(self._on_disarm):
                                try:
                                    self._on_disarm()
                                except Exception as e:
                                    print("[armed] on_disarm callback error:", e)
                        self._armed_prev = self.armed
                except Exception:
                    pass

            elif mtype == "GLOBAL_POSITION_INT":
                try:
                    lat = msg.lat / 1e7
                    lon = msg.lon / 1e7
                    alt_abs = msg.alt / 1000.0
                    alt_rel = msg.relative_alt / 1000.0
                    self.location.global_frame.lat = lat
                    self.location.global_frame.lon = lon
                    self.location.global_frame.alt = alt_abs
                    self.location.global_relative_frame.lat = lat
                    self.location.global_relative_frame.lon = lon
                    self.location.global_relative_frame.alt = alt_rel
                    self.heading = msg.hdg / 100.0 if msg.hdg != 65535 else None
                    self.groundspeed = math.hypot(msg.vx, msg.vy) / 1000.0
                except Exception:
                    pass

            elif mtype == "VFR_HUD":
                try:
                    self.airspeed = float(msg.airspeed)
                    self.groundspeed = float(msg.groundspeed)
                except Exception:
                    pass

            elif mtype == "SYS_STATUS":
                try:
                    self.battery.voltage = msg.voltage_battery / 1000.0 if msg.voltage_battery != 65535 else None
                    self.battery.current = msg.current_battery / 100.0 if msg.current_battery != -1 else None
                    self.battery.level = msg.battery_remaining if msg.battery_remaining != -1 else None
                except Exception:
                    pass

            elif mtype == "DISTANCE_SENSOR":
                # Pixhawk에서 오는 라이다 거리(cm)
                try:
                    distance_cm = int(msg.current_distance)
                    if LIDAR_MIN_DISTANCE_CM <= distance_cm <= LIDAR_MAX_DISTANCE_CM:
                        with lidar_lock:
                            current_lidar_distance = distance_cm
                            lidar_last_update = time.time()
                except Exception:
                    pass

            elif mtype == "MISSION_CURRENT":
                try:
                    self.mission_current_seq = int(msg.seq)
                except Exception:
                    pass


def make_mission_item(lat, lon, alt, command, frame=mavutil.mavlink.MAV_FRAME_GLOBAL_RELATIVE_ALT, param1=0.0):
    return {
        "frame": frame,
        "command": int(command),
        "current": 0,
        "autocontinue": 1,
        "param1": float(param1),
        "param2": 0.0,
        "param3": 0.0,
        "param4": 0.0,
        "x": float(lat),
        "y": float(lon),
        "z": float(alt),
    }


# ========================================
# LiDAR 고도 함수
# ========================================

def get_lidar_altitude_m():
    with lidar_lock:
        if current_lidar_distance is None or lidar_last_update is None:
            return None
        age = time.time() - lidar_last_update
        if age > LIDAR_TIMEOUT_SEC:
            return None
        return current_lidar_distance / 100.0

def get_current_altitude(vehicle: MavVehicle):
    lidar_alt = get_lidar_altitude_m()
    if lidar_alt is not None:
        return lidar_alt, "LIDAR"
    baro_alt = vehicle.location.global_relative_frame.alt
    return baro_alt, "BARO"


# ========================================
# 파라미터/속도 설정
# ========================================

def set_param_and_wait(vehicle: MavVehicle, name: str, value, timeout=5.0):
    # MAVLink param_set은 ACK 확인이 번거로워 여기서는 set만 하고 짧게 대기
    try:
        vehicle.set_param(name, float(value))
        time.sleep(0.2)
        return True
    except Exception as e:
        print(f"[warn] set_param({name}) 실패: {e}")
        return False

def apply_groundspeed_limits_and_set(vehicle: MavVehicle, speed_m_s: float):
    sp_cms = int(max(0.1, float(speed_m_s)) * 100)
    print(f"[speed] 목표 지상속도 {speed_m_s:.2f} m/s 적용 (상한 {sp_cms} cm/s)")
    set_param_and_wait(vehicle, "WPNAV_SPEED", sp_cms)
    # 아래는 있는 경우만
    set_param_and_wait(vehicle, "PSC_VELXY_MAX", sp_cms)
    set_param_and_wait(vehicle, "WPNAV_ACCEL", 300)
    set_param_and_wait(vehicle, "PSC_ACC_XY", 300.0)
    vehicle.do_change_speed(speed_m_s)
    print(f"[speed] DO_CHANGE_SPEED → {speed_m_s:.2f} m/s")


# ========================================
# 지오펜스
# ========================================

def apply_home_geofence(vehicle: MavVehicle, radius_m: float):
    if radius_m <= 0:
        print("[geofence] 비활성화")
        set_param_and_wait(vehicle, "FENCE_ENABLE", 0)
        return False
    ok = True
    ok &= set_param_and_wait(vehicle, "FENCE_ENABLE", 1)
    ok &= set_param_and_wait(vehicle, "FENCE_TYPE", 1)
    ok &= set_param_and_wait(vehicle, "FENCE_RADIUS", float(radius_m))
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
    dlat_m *= scale
    dlon_m *= scale
    new_lat = lat0 + (dlat_m / 111320.0)
    new_lon = lon0 + (dlon_m / _meters_per_deg_lon(lat0))
    return new_lat, new_lon, True

def _get_home_latlon(vehicle: MavVehicle):
    loc = vehicle.location.global_frame
    return getattr(loc, "lat", None), getattr(loc, "lon", None)


# ========================================
# 촬영/업로드 큐
# ========================================

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

if CAMERA_BACKEND == "picamera2":
    _init_picamera2_if_needed()
else:
    print("[camera] libcamera-still 사용")

_captured_photos = []
_captured_lock = Lock()
_upload_thread_running = Event()

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
        publisher.publish(image_status_topic_path, data=json.dumps(msg).encode("utf-8"))
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


# ========================================
# 촬영 함수
# ========================================

def take_photo_for_waypoint(vehicle: MavVehicle, wp_reached_index: int):
    loc_abs = vehicle.location.global_frame
    loc_rel = vehicle.location.global_relative_frame
    lat = getattr(loc_abs, "lat", None)
    lon = getattr(loc_abs, "lon", None)

    alt, alt_source = get_current_altitude(vehicle)

    ts_utc = datetime.now(timezone.utc)
    filename = _utc_ts_name(ts_utc)
    local_filepath = os.path.join(PHOTO_DIR, filename)

    print(f"[camera] 촬영 시작: WP#{wp_reached_index}, 고도={alt:.2f}m ({alt_source}), 파일={local_filepath}")
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
            _captured_photos.append((local_filepath, gcs_rel_path, wp_reached_index, lat, lon, alt, ts_utc.isoformat()))
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
            "lat": lat, "lon": lon, "alt": alt, "alt_source": alt_source,
            "timestamp_utc": ts_utc.isoformat(),
        }
        publisher.publish(status_topic_path, data=json.dumps(payload).encode("utf-8"))
        print(f"[pubsub] photo_captured_local 이벤트 발행: wp={wp_reached_index}")
    except Exception as e:
        print(f"[pubsub] 이벤트 발행 실패: {e}")


# ========================================
# 웨이포인트/LOITER 감시 (AUTO에서 MISSION_CURRENT 기반)
# ========================================

_loiter_to_wpindex_map = {}       # key: loiter_seq (0-based) -> wp_num (1-based)
_loiter_capture_tokens = {}
_capture_cancel_evt = Event()
_wp_watch_stop = Event()

def waypoint_watcher(vehicle: MavVehicle):
    last_seq = None
    print("[wpwatch] MISSION_CURRENT 감시 시작")
    while not _wp_watch_stop.is_set():
        try:
            cur_seq = vehicle.mission_current_seq
            if isinstance(cur_seq, int) and cur_seq != last_seq:
                # loiter 시작 감지
                if cur_seq in _loiter_to_wpindex_map:
                    wp_num = _loiter_to_wpindex_map[cur_seq]
                    wait_sec = max(0.0, LOITER_SEC - CAPTURE_OFFSET_SEC)
                    print(f"[wpwatch] LOITER 시작 seq={cur_seq} (WP#{wp_num}) → {wait_sec:.2f}s 후 촬영 예약")

                    token = _loiter_capture_tokens.get(cur_seq, 0) + 1
                    _loiter_capture_tokens[cur_seq] = token

                    def _delayed_capture(seq, wp_no, my_token, delay_s):
                        t_end = time.time() + delay_s
                        while time.time() < t_end:
                            if _capture_cancel_evt.is_set():
                                print(f"[wpwatch] 캡처 취소(글로벌), seq={seq}")
                                return
                            if _loiter_capture_tokens.get(seq) != my_token:
                                print(f"[wpwatch] 캡처 무효(새 토큰), seq={seq}")
                                return
                            time.sleep(0.05)

                        # 아직 같은 loiter seq인지 확인
                        if vehicle.mission_current_seq != seq:
                            print(f"[wpwatch] LOITER 조기 종료 감지 → 촬영 스킵 seq={seq}")
                            return
                        if _capture_cancel_evt.is_set():
                            print(f"[wpwatch] 캡처 직전 취소 seq={seq}")
                            return

                        print(f"[wpwatch] LOITER 종료 1초 전 촬영 실행 (WP#{wp_no})")
                        take_photo_for_waypoint(vehicle, wp_no)

                    Thread(target=_delayed_capture, args=(cur_seq, wp_num, token, wait_sec), daemon=True).start()

                last_seq = cur_seq

        except Exception as e:
            print(f"[wpwatch] 오류: {e}")
        time.sleep(0.05)
    print("[wpwatch] 감시 종료")


# ========================================
# 이륙 함수
# ========================================

def arm_and_takeoff(vehicle: MavVehicle, target_altitude):
    print("[flight] GUIDED 모드 전환")
    vehicle.set_mode("GUIDED")
    time.sleep(1.0)

    print("[flight] ARM")
    vehicle.arm(True)
    # ARM 확인은 heartbeat 캐시로 확인하되, 짧게 대기
    t0 = time.time()
    while not vehicle.armed and time.time() - t0 < 15:
        print("[flight] ARM 대기...")
        time.sleep(1.0)

    if not vehicle.armed:
        print("[flight] ARM 실패/타임아웃")
        return

    if target_altitude > 0:
        print(f"[flight] TAKEOFF {target_altitude}m")
        vehicle.simple_takeoff(target_altitude)

        while True:
            alt, source = get_current_altitude(vehicle)
            if alt is None:
                alt = 0.0
            print(f"[flight] 현재 고도: {alt:.2f}m ({source})")
            if alt >= target_altitude * 0.95:
                print("[flight] 목표 고도 도달")
                break
            time.sleep(1.0)

    print("[flight] 이륙 완료")


# ========================================
# 타임스탬프 검증
# ========================================

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


# ========================================
# 상태 발행
# ========================================

_last_received_message = None
_last_received_at = None
_msg_lock = Lock()

def set_last_received_message(msg: str):
    global _last_received_message, _last_received_at
    with _msg_lock:
        _last_received_message = (msg or "").strip()
        _last_received_at = datetime.now(timezone.utc)

def get_state(vehicle: MavVehicle):
    loc = vehicle.location.global_relative_frame
    alt, alt_source = get_current_altitude(vehicle)

    with lidar_lock:
        lidar_raw_cm = current_lidar_distance
        lidar_age = None
        if lidar_last_update is not None:
            lidar_age = time.time() - lidar_last_update

    with _msg_lock:
        last_msg = _last_received_message
        last_at_fmt = None
        if _last_received_at:
            last_at_fmt = _last_received_at.strftime("%Y-%m-%d %H:%M:%S")

    state = {
        "serial_num": LOCAL_SERIAL_LAST4 or None,
        "arming_status": vehicle.armed,
        "mode": str(vehicle.mode.name),
        "altitude": alt,
        "altitude_source": alt_source,
        "latitude": getattr(loc, "lat", None),
        "longitude": getattr(loc, "lon", None),
        "heading": getattr(vehicle, "heading", None),
        "airspeed": getattr(vehicle, "airspeed", None),
        "groundspeed": int(round(getattr(vehicle, "groundspeed", 0.0))),
        "battery_voltage": getattr(getattr(vehicle, "battery", None), "voltage", None),
        "battery_current": getattr(getattr(vehicle, "battery", None), "current", None),
        "battery_charge": getattr(getattr(vehicle, "battery", None), "level", None),
        "lidar": {
            "distance_cm": lidar_raw_cm,
            "altitude_m": get_lidar_altitude_m(),
            "data_age_sec": lidar_age,
            "active": (alt_source == "LIDAR")
        },
        "last_received_message": last_msg,
        "last_received_at": last_at_fmt,
        "geofence_radius_m": GEOFENCE_RADIUS_M,
        "mission_current_seq": vehicle.mission_current_seq,
    }
    return state

def publish_state_loop(vehicle: MavVehicle):
    while True:
        try:
            state = get_state(vehicle)
            publisher.publish(status_topic_path, data=json.dumps(state).encode("utf-8"))
        except Exception as e:
            print("[state] 전송 실패:", e)
        time.sleep(2)


# ========================================
# 명령 처리
# ========================================

def _extract_target_last4(payload: dict):
    v = payload.get("serialNum")
    drone = payload.get("droneId")
    farm = payload.get("farmId")
    ndvi = None
    if not v:
        return None, None, None, None
    s = str(v).strip().lower()
    try:
        ndvi = payload.get("ndviId")
    finally:
        pass
    return (s[-4:] if len(s) >= 4 else None), farm, drone, ndvi

saved_waypoints = []

_pause_lock = Lock()
_pause_resume_evt = Event()
_pause_cancel_evt = Event()
_pause_token = 0

def handle_command(vehicle: MavVehicle, command_data):
    global saved_waypoints, _pause_token
    command = (command_data.get("command") or "").lower().strip()

    _update_session_meta_from_cmd(command_data)

    if command not in ("resume", "stop_mission"):
        _pause_cancel_evt.set()

    if command == "arm":
        arm_and_takeoff(vehicle, 0)

    elif command == "disarm":
        print("[cmd] DISARM")
        _capture_cancel_evt.set(); time.sleep(0.05); _capture_cancel_evt.clear()
        vehicle.arm(False)

    elif command == "upload_now":
        print("[cmd] 수동 업로드")
        _do_batch_upload(tag="manual_command")

    elif command == "waypoints":
        waypoints = command_data.get("waypoints", [])
        if waypoints:
            saved_waypoints = waypoints
            print(f"[cmd] {len(saved_waypoints)}개 웨이포인트 저장")
        else:
            print("[cmd] 웨이포인트 없음")

    elif command == "start_mission":
        if not saved_waypoints:
            print("[cmd] 웨이포인트가 없습니다.")
            return

        with _captured_lock:
            _captured_photos.clear()

        apply_home_geofence(vehicle, GEOFENCE_RADIUS_M)
        home_lat, home_lon = _get_home_latlon(vehicle)
        if None in (home_lat, home_lon):
            print("[geofence] 홈 좌표 확인 실패 → 클램프 생략")
        else:
            print(f"[geofence] 홈: lat={home_lat:.7f}, lon={home_lon:.7f}")

        # 미션 구성 (AUTO용)
        print("[cmd] 미션 구성/업로드...")
        items = []
        global _loiter_to_wpindex_map, _loiter_capture_tokens
        _loiter_to_wpindex_map = {}
        _loiter_capture_tokens = {}

        adjusted = 0

        for i, wp in enumerate(saved_waypoints, start=1):
            lat = wp.get("lat")
            lon = wp.get("lon") or wp.get("lng")
            alt = wp.get("alt", MISSION_ALTITUDE)

            if lat is None or lon is None:
                print(f"[cmd] WP#{i} 좌표 없음 → 스킵")
                continue

            if None not in (home_lat, home_lon, lat, lon):
                new_lat, new_lon, changed = _clamp_to_radius(
                    float(home_lat), float(home_lon),
                    float(lat), float(lon),
                    float(GEOFENCE_RADIUS_M)
                )
                if changed:
                    adjusted += 1
                    print(f"[geofence] WP 클램프: ({lat:.7f},{lon:.7f}) → ({new_lat:.7f},{new_lon:.7f})")
                    lat, lon = new_lat, new_lon

            # WP
            items.append(make_mission_item(lat, lon, alt, mavutil.mavlink.MAV_CMD_NAV_WAYPOINT, param1=0.0))
            # LOITER_TIME (param1 = loiter seconds)
            items.append(make_mission_item(lat, lon, alt, mavutil.mavlink.MAV_CMD_NAV_LOITER_TIME, param1=float(LOITER_SEC)))

            loiter_seq = len(items) - 1  # 0-based seq of LOITER item
            _loiter_to_wpindex_map[loiter_seq] = i

        # RTL
        items.append(make_mission_item(0, 0, 0, mavutil.mavlink.MAV_CMD_NAV_RETURN_TO_LAUNCH, param1=0.0))

        if adjusted > 0:
            print(f"[geofence] 총 {adjusted}개 WP 보정됨")

        ok = vehicle.mission_upload(items)
        if not ok:
            print("[cmd] 미션 업로드 실패")
            return

        apply_groundspeed_limits_and_set(vehicle, DEFAULT_GROUNDSPEED)

        # watcher start
        _wp_watch_stop.clear()
        Thread(target=waypoint_watcher, args=(vehicle,), daemon=True).start()

        # takeoff then AUTO
        arm_and_takeoff(vehicle, MISSION_ALTITUDE)
        print("[flight] AUTO 전환")
        vehicle.set_mode("AUTO")
        time.sleep(1.0)
        print("[flight] 미션 시작")

    elif command == "stop_mission":
        print("[cmd] 미션 일시정지 요청")
        _capture_cancel_evt.set(); time.sleep(0.05); _capture_cancel_evt.clear()

        # 현재 미션 seq 저장
        vehicle._resume_seq = vehicle.mission_current_seq
        print(f"[cmd] resume_seq={vehicle._resume_seq}")

        vehicle.set_mode("GUIDED")
        time.sleep(1.0)

        with _pause_lock:
            _pause_token += 1
            local_token = _pause_token
            _pause_resume_evt.clear()
            _pause_cancel_evt.clear()

        print(f"[cmd] 10초 재개 대기 (token={local_token})")

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
                    if vehicle._resume_seq is not None:
                        vehicle.mission_set_current(int(vehicle._resume_seq))
                    vehicle.set_mode("AUTO")
                    return
                time.sleep(0.2)

            if token == _pause_token and not _pause_cancel_evt.is_set() and not _pause_resume_evt.is_set():
                print("[cmd] 재개 없음 → RTL")
                vehicle.set_mode("RTL")

        Thread(target=wait_for_resume_or_cancel, args=(local_token, 10), daemon=True).start()

    elif command == "resume":
        print("[cmd] resume 수신")
        _pause_resume_evt.set()

    elif command == "set_speed":
        speed = command_data.get("speed")
        if speed is not None:
            sp = float(speed)
            print(f"[cmd] 지상속도 {sp:.2f} m/s 설정")
            apply_groundspeed_limits_and_set(vehicle, sp)
        else:
            print("[cmd] 속도 값 없음")

    else:
        print(f"[cmd] 알 수 없는 명령: {command}")


# ========================================
# Pub/Sub 콜백
# ========================================

def callback(message, vehicle: MavVehicle):
    print("[sub] 메시지 수신")
    try:
        data = message.data.decode("utf-8")
        json_data = json.loads(data)
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

        Thread(target=handle_command, args=(vehicle, json_data), daemon=True).start()

    except Exception as e:
        print("[sub] 처리 실패:", e)
        print("[sub] 원본:", message.data)
    finally:
        message.ack()


# ========================================
# main
# ========================================

def _on_disarm_upload():
    print("[armed] Disarm 감지 → 업로드 트리거")
    _do_batch_upload(tag="disarm")

if __name__ == "__main__":
    vehicle = None
    streaming_future = None
    try:
        print(f"[boot] 드론 연결 중: {CONNECTION_STRING}@{BAUD_RATE}")
        vehicle = MavVehicle(CONNECTION_STRING, BAUD_RATE)
        vehicle.set_on_disarm(_on_disarm_upload)
        vehicle.wait_ready(timeout=30)
        print("[boot] MAVLink vehicle ready")

        apply_home_geofence(vehicle, GEOFENCE_RADIUS_M)
        apply_groundspeed_limits_and_set(vehicle, DEFAULT_GROUNDSPEED)

        print("\n[test] 라이다 데이터 수신 테스트 (5초)...")
        for _ in range(5):
            lidar_alt = get_lidar_altitude_m()
            if lidar_alt is not None:
                print(f"[test] 라이다 고도: {lidar_alt:.2f}m ✓")
            else:
                print("[test] 라이다 데이터 없음 (barometer 대체 가능)")
            time.sleep(1)

        print(f"[boot] Pub/Sub 명령 구독 시작: {command_subscription_path}")
        streaming_future = subscriber.subscribe(command_subscription_path, callback=lambda m: callback(m, vehicle))
        Thread(target=publish_state_loop, args=(vehicle,), daemon=True).start()

        print("\n[boot] 명령 대기 중... (Ctrl+C로 종료)")
        while True:
            time.sleep(0.5)

    except KeyboardInterrupt:
        print("\n[boot] 종료 시퀀스")
    except Exception as e:
        print("[boot] 예외:", e)
    finally:
        _wp_watch_stop.set()
        try:
            if streaming_future:
                streaming_future.cancel()
        except Exception:
            pass
        try:
            if _picamera2:
                _picamera2.stop()
        except Exception:
            pass
        try:
            if vehicle:
                vehicle.close()
        except Exception:
            pass
        print("[boot] 종료 완료")
