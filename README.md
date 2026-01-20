Pixhawk(ArduPilot)가 “비행을 실제로 실행”하고,
Raspberry Pi가 “자동화 로직 + 클라우드 연동”을 맡고,
노트북은 “모니터링/개발(VNC, Mission Planner)”용이다.

1) 구성 요소별 역할
✅ Pixhawk 2.4.8 (FC + ArduPilot)

드론의 “운전수/반사신경”

IMU/EKF/PID로 자세 안정화

모터 출력 계산

AUTO/GUIDED/RTL 같은 비행모드 실행

LiDAR 값을 받아 RNGFND / DISTANCE_SENSOR로 관리

✅ LiDAR (거리 센서)

Pixhawk에 직접 연결

지면까지 거리(고도 판단용) 제공

✅ Raspberry Pi (컴패니언 컴퓨터)

드론의 “뇌(계획/자동화)”

Google Pub/Sub로 클라우드 명령(웨이포인트, ARM/START/STOP 등) 수신

받은 웨이포인트를 Pixhawk에 미션으로 업로드하고 비행 시작 명령

비행 상태/사진 업로드 결과를 Pub/Sub로 다시 전송

카메라 촬영 + EXIF(GPS/시간/고도) 삽입 + GCS 업로드

✅ 카메라

Pi가 촬영 (libcamera/picamera2)

웨이포인트마다 LOITER 끝나기 직전에 촬영하도록 자동화

✅ 클라우드 (Pub/Sub + GCS)

Pub/Sub: “명령 내려주는 통로” + “상태 받아보는 통로”

GCS: 촬영 이미지 저장소

✅ 노트북

Pi를 VNC로 보고 코드 실행/로그 확인

필요 시 Mission Planner로 Pixhawk 설정/디버깅(주로 USB로)

2) 연결(통신) 구조
(A) Pixhawk ↔ Raspberry Pi

TELEM2 시리얼(MAVLink) 로 연결

Pi가 Pixhawk에 명령을 보내고, Pixhawk가 센서/상태를 Pi에 보내줌

(B) Raspberry Pi ↔ Cloud

Wi-Fi/LAN으로 인터넷 연결

Pub/Sub는 TCP/TLS(gRPC), GCS도 HTTPS(TCP)

(C) 노트북 ↔ Raspberry Pi

VNC로 원격 접속해서 Pi 화면을 보며 실행

(D) 노트북 ↔ Pixhawk (선택)

꼭 필요하진 않지만,

캘리브레이션/파라미터/로그 확인할 때 Mission Planner로 연결

꼬임 방지를 위해 보통 USB를 사용 (Pi와 포트 분리)

3) “데이터가 흐르는 길”을 순서대로
① 클라우드에서 웨이포인트/명령 내려옴

Cloud Pub/Sub → Raspberry Pi Python

② Pi가 드론에 비행 명령

Raspberry Pi → (MAVLink/TELEM2) → Pixhawk

미션 업로드(AUTO)

ARM / 모드 전환 / RTL 등

③ Pixhawk가 실제로 비행 수행

Pixhawk가 모터를 제어해서 웨이포인트로 이동
(비행은 Pi가 아니라 Pixhawk가 “실행”)

④ Pi가 상태와 센서값을 받음

Pixhawk → Pi로

위치/모드/배터리

LiDAR(DISTANCE_SENSOR)

⑤ 웨이포인트 구간에서 촬영

Pi가 LOITER 타이밍에 맞춰 촬영 → EXIF 삽입 → 로컬 저장

⑥ 사진 업로드 + 상태 전송

Pi → GCS 업로드
Pi → Pub/Sub로 상태/업로드 결과 전송
