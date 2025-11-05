import os
import json
from datetime import datetime
from flask import Flask, render_template, request, redirect, url_for
from flask_sqlalchemy import SQLAlchemy

BASE_DIR = os.path.abspath(os.path.dirname(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
STATIC_DIR = os.path.join(BASE_DIR, "static")
GALLERY_DIR = os.path.join(STATIC_DIR, "gallery", "events")

# 폴더 자동 생성
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(GALLERY_DIR, exist_ok=True)

app = Flask(__name__)

# SQLite 설정
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///voc_db.sqlite3'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)


# ---------------- DB 모델 ----------------
class VOC(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    writer = db.Column(db.String(50), nullable=False)
    title = db.Column(db.String(200), nullable=False)
    content = db.Column(db.Text, nullable=False)
    summary = db.Column(db.Text, nullable=False)
    priority = db.Column(db.String(20), nullable=False, default="중")
    created_at = db.Column(db.DateTime, default=datetime.now)


# ---------------- 유틸 ----------------
ALLOWED_EXT = {".jpg", ".jpeg", ".png", ".gif"}

def list_event_images():
    if not os.path.isdir(GALLERY_DIR):
        return []
    files = []
    for name in os.listdir(GALLERY_DIR):
        ext = os.path.splitext(name)[1].lower()
        if ext in ALLOWED_EXT:
            full = os.path.join(GALLERY_DIR, name)
            mtime = os.path.getmtime(full)
            files.append((name, mtime))
    # 최근 파일 먼저
    files.sort(key=lambda x: x[1], reverse=True)
    # static 상대 경로로 변환
    return [f"gallery/events/{name}" for name, _ in files]

def load_morning_brief():
    """data/morning_brief.json을 읽어 카드에 표시. 없으면 공란."""
    path = os.path.join(DATA_DIR, "morning_brief.json")
    default = {"생산량": "", "불량률": "", "다운타임": "", "긴급이슈": ""}
    if not os.path.isfile(path):
        return default
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        # 기본 키 보장
        for k in default:
            data.setdefault(k, "")
        return data
    except Exception:
        return default

def load_announcements():
    """data/announcements.txt 줄단위 공지. 없으면 빈 리스트."""
    path = os.path.join(DATA_DIR, "announcements.txt")
    if not os.path.isfile(path):
        return []
    try:
        with open(path, "r", encoding="utf-8") as f:
            lines = [ln.strip() for ln in f.readlines() if ln.strip()]
        return lines
    except Exception:
        return []


# ---------------- LLM 요약/우선순위(더미) ----------------
def summarize_with_llm(text: str) -> str:
    text = text.strip()
    if len(text) <= 160:
        return text
    return text[:160] + " ..."

def classify_priority(text: str) -> str:
    keywords_high = ["불량", "라인멈춤", "downtime", "긴급", "고장", "정지", "화재", "안전"]
    for k in keywords_high:
        if k in text:
            return "상"
    return "중"

def priority_rank(p: str) -> int:
    return {"상": 2, "중": 1, "하": 0}.get(p, 0)


# ---------------- 라우트 ----------------
@app.route("/")
def home():
    """메인 화면: 전달사항, 모닝브리프, 행사 사진 갤러리"""
    announcements = load_announcements()      # 리스트[str]
    mb = load_morning_brief()                 # 딕셔너리
    images = list_event_images()              # static 상대경로 리스트
    return render_template(
        "home.html",
        announcements=announcements,
        mb=mb,
        images=images
    )

@app.route("/submit", methods=["GET", "POST"])
def submit_voc():
    if request.method == "POST":
        writer = request.form.get("writer", "").strip()
        title = request.form.get("title", "").strip()
        content = request.form.get("content", "").strip()

        if not (writer and title and content):
            return render_template("submit.html", error="모든 항목을 입력해주세요.")

        summary = summarize_with_llm(content)
        priority = classify_priority(content)

        voc = VOC(writer=writer, title=title, content=content, summary=summary, priority=priority)
        db.session.add(voc)
        db.session.commit()
        return redirect(url_for("voc_board"))

    return render_template("submit.html")

@app.route("/voc")
def voc_board():
    rows = VOC.query.order_by(VOC.created_at.desc()).all()
    voc_list = sorted(rows, key=lambda v: (priority_rank(v.priority), v.created_at), reverse=True)
    return render_template("dashboard.html", voc_list=voc_list)

@app.route("/voc/<int:voc_id>")
def voc_detail(voc_id):
    voc = VOC.query.get_or_404(voc_id)
    return render_template("detail.html", voc=voc)

# 호환용(예전 /dashboard 경로로 접속해도 동작)
@app.route("/dashboard")
def dashboard_compat():
    return redirect(url_for("voc_board"))


if __name__ == "__main__":
    with app.app_context():
        db.create_all()
    # 내부망 개발 모드
    app.run(debug=True)
