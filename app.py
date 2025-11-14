import os
import json
from math import ceil
from datetime import datetime, date, timedelta
from flask import Flask, render_template, request, redirect, url_for, jsonify
from flask_sqlalchemy import SQLAlchemy

# 기본 경로 설정
BASE_DIR = os.path.abspath(os.path.dirname(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
STATIC_DIR = os.path.join(BASE_DIR, "static")
GALLERY_DIR = os.path.join(STATIC_DIR, "gallery", "events")
BIRTHDAYS_JSON = os.path.join(DATA_DIR, "birthdays.json")
MTBI_JSON = os.path.join(DATA_DIR, "mtbi.json")

# 필요한 폴더 자동 생성
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(GALLERY_DIR, exist_ok=True)

app = Flask(__name__)

# SQLite DB 설정
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///dept_portal.sqlite3'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)


# ====================== DB 모델 ======================

class VOC(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    writer = db.Column(db.String(50), nullable=False)
    title = db.Column(db.String(200), nullable=False)
    content = db.Column(db.Text, nullable=False)
    summary = db.Column(db.Text, nullable=False)
    priority = db.Column(db.String(20), nullable=False, default="중")
    created_at = db.Column(db.DateTime, default=datetime.now)


class GalleryImage(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    filename = db.Column(db.String(255), unique=True, nullable=False)
    likes = db.Column(db.Integer, default=0, nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.now)


class Announcement(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    title = db.Column(db.String(200), nullable=False)
    body = db.Column(db.Text, nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.now)


# ====================== 유틸: 행사 사진 ======================

ALLOWED_EXT = {".jpg", ".jpeg", ".png", ".gif"}


def list_event_images():
    """행사 사진 목록 (수정시간 기준 최신순)"""
    files = []
    if not os.path.isdir(GALLERY_DIR):
        return files
    for name in os.listdir(GALLERY_DIR):
        ext = os.path.splitext(name)[1].lower()
        if ext in ALLOWED_EXT:
            full = os.path.join(GALLERY_DIR, name)
            mtime = os.path.getmtime(full)
            files.append({
                "name": name,
                "src": f"gallery/events/{name}",
                "mtime": mtime,
            })
    files.sort(key=lambda x: x["mtime"], reverse=True)
    return files


def get_or_create_image_row(filename: str) -> GalleryImage:
    row = GalleryImage.query.filter_by(filename=filename).first()
    if row is None:
        row = GalleryImage(filename=filename, likes=0)
        db.session.add(row)
        db.session.commit()
    return row


# ====================== 유틸: VOC 요약/우선순위 ======================

def summarize_with_llm(text: str) -> str:
    """간단 요약(앞부분 자르기)"""
    text = text.strip()
    if len(text) <= 160:
        return text
    return text[:160] + " ..."


def classify_priority(text: str) -> str:
    """키워드 기반 우선순위"""
    keywords_high = ["불량", "라인멈춤", "downtime", "긴급", "고장", "정지", "화재", "안전"]
    for k in keywords_high:
        if k in text:
            return "상"
    return "중"


def priority_rank(p: str) -> int:
    return {"상": 2, "중": 1, "하": 0}.get(p, 0)


# ====================== 유틸: 생일/기념일 ======================

WEEKDAY_KR = ["월", "화", "수", "목", "금", "토", "일"]


def safe_load_json(path):
    if not os.path.isfile(path):
        return []
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f) or []
    except Exception:
        return []


def mmdd_from_str(s: str):
    """'YYYY-MM-DD' 또는 'MM-DD' -> (m, d)"""
    if not s:
        return None
    s = s.strip()
    try:
        if len(s) == 10 and s[4] == '-' and s[7] == '-':
            return int(s[5:7]), int(s[8:10])
        if len(s) == 5 and s[2] == '-':
            return int(s[0:2]), int(s[3:5])
    except Exception:
        return None
    return None


def this_week_dates(today=None):
    if today is None:
        today = date.today()
    start = today - timedelta(days=today.weekday())  # 월요일
    return [start + timedelta(days=i) for i in range(7)]


def format_display(d: date):
    return f"{d.month}월 {d.day}일 ({WEEKDAY_KR[d.weekday()]})"


def load_birthdays_this_week():
    raw = safe_load_json(BIRTHDAYS_JSON)
    week_dates = this_week_dates()
    today = date.today()
    md_map = {(d.month, d.day): d for d in week_dates}

    events = []

    for r in raw:
        name = (r.get("name") or "").strip()
        if not name:
            continue

        # 생일
        b = mmdd_from_str(r.get("birthday"))
        if b and b in md_map:
            d = md_map[b]
            diff = (d - today).days
            when = "오늘" if diff == 0 else f"D-{diff}" if diff > 0 else f"D+{abs(diff)}"
            events.append({
                "type": "생일",
                "name": name,
                "date": d,
                "display": format_display(d),
                "when": when,
                "is_today": diff == 0,
            })

        # 기념일
        a = mmdd_from_str(r.get("anniversary"))
        if a and a in md_map:
            d = md_map[a]
            diff = (d - today).days
            when = "오늘" if diff == 0 else f"D-{diff}" if diff > 0 else f"D+{abs(diff)}"
            events.append({
                "type": "기념일",
                "name": name,
                "date": d,
                "display": format_display(d),
                "when": when,
                "is_today": diff == 0,
            })

    events.sort(key=lambda x: (x["date"], x["type"], x["name"]))
    return events


# ====================== 유틸: MTBI 데이터 ======================

def load_mtbi_data():
    """
    data/mtbi.json에서 MTBI 시계열을 읽어온다.
    형식 예:
    {
      "daily":   [ {"date":"2025-10-01","mtbi":123.4}, ... ],
      "weekly":  [ {"label":"2025-W40","mtbi":140.8}, ... ],
      "monthly": [ {"label":"2025-10","mtbi":160.1}, ... ]
    }
    """
    default = {"daily": [], "weekly": [], "monthly": []}
    if not os.path.isfile(MTBI_JSON):
        return default
    try:
        with open(MTBI_JSON, "r", encoding="utf-8") as f:
            data = json.load(f)
        for k in default:
            data.setdefault(k, [])
        return data
    except Exception:
        return default


# ====================== 라우트: 메인 ======================

@app.route("/")
def home():
    # 최신 전달사항 5개
    top_ann = Announcement.query.order_by(Announcement.created_at.desc()).limit(5).all()

    # 행사 사진 + 좋아요
    images = list_event_images()
    likes_map = {}
    for it in images:
        row = GalleryImage.query.filter_by(filename=it["name"]).first()
        likes_map[it["name"]] = row.likes if row else 0

    # 이번 주 생일/기념일
    bday_events = load_birthdays_this_week()

    # MTBI 데이터
    mtbi = load_mtbi_data()

    return render_template(
        "home.html",
        top_ann=top_ann,
        images=images,
        likes=likes_map,
        bday_events=bday_events,
        mtbi=mtbi
    )


# ====================== 라우트: VOC ======================

@app.route("/submit", methods=["GET", "POST"])
def submit_voc():
    if request.method == "POST":
        writer = (request.form.get("writer") or "").strip()
        title = (request.form.get("title") or "").strip()
        content = (request.form.get("content") or "").strip()

        if not (writer and title and content):
            return render_template("submit.html", error="모든 항목을 입력해주세요.")

        summary = summarize_with_llm(content)
        priority = classify_priority(content)

        voc = VOC(
            writer=writer,
            title=title,
            content=content,
            summary=summary,
            priority=priority,
        )
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


@app.route("/dashboard")
def dashboard_compat():
    return redirect(url_for("voc_board"))


# ====================== 라우트: 행사 사진 좋아요 API ======================

@app.route("/api/gallery/like", methods=["POST"])
def api_gallery_like():
    data = request.get_json(silent=True) or {}
    filename = (data.get("img") or "").strip()
    if not filename:
        return jsonify(ok=False, error="filename required"), 400

    filename = os.path.basename(filename)
    full = os.path.join(GALLERY_DIR, filename)
    if not os.path.isfile(full):
        return jsonify(ok=False, error="file not found"), 404

    row = get_or_create_image_row(filename)
    row.likes += 1
    db.session.commit()
    return jsonify(ok=True, likes=row.likes)


# ====================== 라우트: 전달사항 ======================

@app.route("/announcements")
def announcements_list():
    per_page = 5
    page = max(1, int(request.args.get("page", 1)))
    q = Announcement.query.order_by(Announcement.created_at.desc())
    total = q.count()
    items = q.offset((page - 1) * per_page).limit(per_page).all()
    total_pages = max(1, ceil(total / per_page))

    return render_template(
        "announcements_list.html",
        items=items,
        page=page,
        total_pages=total_pages,
    )


@app.route("/announcements/<int:ann_id>")
def announcements_detail(ann_id):
    ann = Announcement.query.get_or_404(ann_id)
    return render_template("announcements_detail.html", ann=ann)


@app.route("/announcements/new", methods=["GET", "POST"])
def announcements_new():
    if request.method == "POST":
        title = (request.form.get("title") or "").strip()
        body = (request.form.get("body") or "").strip()
        if not (title and body):
            return render_template("announcements_new.html", error="제목과 내용을 입력해주세요.")
        ann = Announcement(title=title, body=body)
        db.session.add(ann)
        db.session.commit()
        return redirect(url_for("announcements_list"))

    return render_template("announcements_new.html")


# ====================== 시작 부분 ======================

if __name__ == "__main__":
    with app.app_context():
        db.create_all()
    # 사내망 접근 가능
    app.run(host="127.0.0.1", port=8000, debug=False)
