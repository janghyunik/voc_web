"""
MTBI 배치 스크립트 (D-2까지 저장/갱신 · 주/월=합계기반 · work_date 조건 추가 · 최종본)

요구사항:
- 오늘(당일)과 어제(D-1)는 제외하고, **이틀 전(D-2)**까지만 mtbi.json에 저장/갱신
- 초기 구축(--init-days N): 과거 N일치 생성 (끝= D-2)
- 일일 누적(--update): D-2 하루치만 계산/반영 (덮어쓰기 아님, 해당 날짜만 교체/추가)
- 스케줄(--schedule HH:MM): 매일 지정 시각에 --update 실행(무한 루프)
- daily 항목: {"date","work","err","mtbi"}
- weekly/monthly: daily의 work/err **합계**로 MTBI 계산 (단순 평균 아님)
- get_work_time()의 param에 work_date(YYMMDD) 조건 추가

예시:
1) 초기 구축 120일
   python mtbi_batch.py --init-days 120

2) 매일 06:05에 자동 갱신(무한 실행)
   python mtbi_batch.py --schedule 06:05

3) 작업 스케줄러에서 매일 한 번 실행(권장)
   python mtbi_batch.py --update
"""

import os
import sys
import json
import time
import argparse
import traceback
from datetime import date, datetime, timedelta
from typing import List, Dict, Optional

# ====================== TODO: 환경에 맞게 수정 ======================

# 실제 환경의 getData import (예: from common.impala_connector import getData)
from your_impala_module import getData  # ← 실제 모듈/경로로 교체하세요

TABLE_RUNTIME = "YOUR_RUNTIME_TABLE"    # 전체 설비 가동시간 테이블명
TABLE_ERROR   = "YOUR_ERROR_TABLE"      # 전체 설비 에러 테이블명
LARGE_CLASS   = "MMM"

BASE_DIR = os.path.abspath(os.path.dirname(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
os.makedirs(DATA_DIR, exist_ok=True)
MTBI_JSON_PATH = os.path.join(DATA_DIR, "mtbi.json")

# ====================== 로깅 ======================

def log(msg: str):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)

def log_error(msg: str, exc: Exception = None):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] [ERROR] {msg}", file=sys.stderr, flush=True)
    if exc:
        traceback.print_exception(type(exc), exc, exc.__traceback__)

# ====================== 유틸 ======================

def today_local() -> date:
    """서버 로컬 날짜(필요시 KST 고정으로 교체 가능)"""
    return date.today()

def yyyymmdd(d: date) -> str:
    return d.strftime("%Y-%m-%d")

def parse_ymd(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()

def daterange(start: date, end: date):
    """start~end(포함)"""
    d = start
    while d <= end:
        yield d
        d += timedelta(days=1)

def two_days_ago(today: date) -> date:
    """D-2: 오늘과 어제 제외한 마지막 저장 가능 날짜"""
    return today - timedelta(days=2)

# ====================== Impala 조회 ======================

def get_work_time(target_date: date) -> int:
    """
    target_date의 전체 설비 가동시간 합계
    - dateFrom/dateTo = YYYY-MM-DD
    - ✅ work_date = YYMMDD(예: 2025-11-14 → '251114') 조건 추가
    """
    ds = yyyymmdd(target_date)
    work_date_str = target_date.strftime("%y%m%d")  # YYMMDD
    param = {
        "table_name": TABLE_RUNTIME,
        "large_class": LARGE_CLASS,
        "dateFrom": ds,
        "dateTo": ds,
        "work_date": work_date_str,  # ← 추가된 조건
    }
    log(f"[WORK] {ds} 조회 시작: {param}")
    try:
        df = getData(param=param)
        if df is None or df.empty:
            log(f"[WORK] {ds} 결과 없음 → 0")
            return 0
        if "use_time" not in df.columns:
            log_error(f"[WORK] {ds} 'use_time' 컬럼 없음 → 0")
            return 0
        total = int(df["use_time"].astype(int).sum())
        log(f"[WORK] {ds} 합계={total}")
        return total
    except Exception as e:
        log_error(f"[WORK] {ds} 조회 예외 → 0", e)
        return 0

def get_error_count(target_date: date) -> int:
    """target_date 기준 에러 건수(전일 22:00 ~ 당일 22:00, 숫자 시작 코드만)"""
    start_dt = (target_date - timedelta(days=1)).strftime("%Y-%m-%d") + " 22:00:00"
    end_dt   = target_date.strftime("%Y-%m-%d") + " 22:00:00"
    param = {
        "table_name": TABLE_ERROR,
        "large_class": LARGE_CLASS,
        "compare_conditions": [
            f"start_time >= '{start_dt}'",
            f"start_time <= '{end_dt}'",
            "error_code >= '0'",
            "error_code < 'A'",
            "error_code != ' '"
        ]
    }
    log(f"[ERR ] {yyyymmdd(target_date)} 조회: {start_dt} ~ {end_dt} + 숫자코드 조건")
    try:
        df = getData(param=param)
        cnt = 0 if (df is None or df.empty) else int(len(df))
        log(f"[ERR ] {yyyymmdd(target_date)} 건수={cnt}")
        return cnt
    except Exception as e:
        log_error(f"[ERR ] {yyyymmdd(target_date)} 조회 예외 → 0", e)
        return 0

# ====================== MTBI 계산/집계 ======================

def calc_daily_record(d: date) -> Dict:
    """
    하루치 MTBI 계산 결과(딕셔너리)
    저장 형태: {"date": YYYY-MM-DD, "work": int, "err": int, "mtbi": float}
    """
    work = get_work_time(d)
    err  = get_error_count(d)
    mtbi = round(work / err, 2) if err > 0 else 0.0
    log(f"[DAILY] {d} MTBI={mtbi} (work={work}, err={err})")
    return {"date": yyyymmdd(d), "work": int(work), "err": int(err), "mtbi": float(mtbi)}

def _safe_num(x) -> Optional[float]:
    try:
        return float(x)
    except Exception:
        return None

def build_weekly_from_daily(daily: List[Dict]) -> List[Dict]:
    """
    daily -> weekly(ISO 주차) 집계
    - 주의 work 합 / err 합 으로 MTBI 계산 (err 합이 0이면 0.0)
    - 결과 항목: {"label": "YYYY-Www", "work": int, "err": int, "mtbi": float}
    """
    from collections import defaultdict
    agg = defaultdict(lambda: {"work": 0.0, "err": 0.0})
    for r in daily:
        try:
            d = parse_ymd(r["date"])
        except Exception:
            continue
        w = _safe_num(r.get("work"))
        e = _safe_num(r.get("err"))
        if w is None or e is None:
            # (하위호환) 과거 daily에 work/err가 없으면 해당 일은 집계 제외
            continue
        iso_year, iso_week, _ = d.isocalendar()
        key = f"{iso_year}-W{iso_week:02d}"
        agg[key]["work"] += w
        agg[key]["err"]  += e

    weekly = []
    for key in sorted(agg.keys()):
        wsum = int(round(agg[key]["work"]))
        esum = int(round(agg[key]["err"]))
        mtbi = round(wsum / esum, 2) if esum > 0 else 0.0
        weekly.append({"label": key, "work": wsum, "err": esum, "mtbi": mtbi})
    return weekly

def build_monthly_from_daily(daily: List[Dict]) -> List[Dict]:
    """
    daily -> monthly(YYYY-MM) 집계
    - 월의 work 합 / err 합 으로 MTBI 계산 (err 합이 0이면 0.0)
    - 결과 항목: {"label": "YYYY-MM", "work": int, "err": int, "mtbi": float}
    """
    from collections import defaultdict
    agg = defaultdict(lambda: {"work": 0.0, "err": 0.0})
    for r in daily:
        ym = (r.get("date") or "")[:7]
        if len(ym) != 7:
            continue
        w = _safe_num(r.get("work"))
        e = _safe_num(r.get("err"))
        if w is None or e is None:
            continue
        agg[ym]["work"] += w
        agg[ym]["err"]  += e

    monthly = []
    for key in sorted(agg.keys()):
        wsum = int(round(agg[key]["work"]))
        esum = int(round(agg[key]["err"]))
        mtbi = round(wsum / esum, 2) if esum > 0 else 0.0
        monthly.append({"label": key, "work": wsum, "err": esum, "mtbi": mtbi})
    return monthly

def clip_daily_to_d2(daily: List[Dict], today: date) -> List[Dict]:
    """
    daily 리스트에서 날짜가 D-1(어제) 또는 오늘인 항목은 제거하고
    D-2 이하만 남긴다. (work/err/mtbi 필드 유지)
    """
    max_allowed = two_days_ago(today)  # D-2
    # 정규화 및 필드 보정
    normalized = []
    for r in daily:
        try:
            d = parse_ymd(str(r.get("date", "")))
        except Exception:
            continue
        if d <= max_allowed:
            work = int(_safe_num(r.get("work")) or 0)
            err  = int(_safe_num(r.get("err")) or 0)
            mtbi = float(_safe_num(r.get("mtbi")) or (round(work/err, 2) if err > 0 else 0.0))
            normalized.append({"date": yyyymmdd(d), "work": work, "err": err, "mtbi": mtbi})

    # 날짜 기준 중복 제거(마지막 값 우선) + 정렬
    by_date = {}
    for r in normalized:
        by_date[r["date"]] = r
    out = list(by_date.values())
    out.sort(key=lambda x: x["date"])
    return out

def save_json_from_daily(daily: List[Dict]):
    weekly  = build_weekly_from_daily(daily)
    monthly = build_monthly_from_daily(daily)
    data = {"daily": daily, "weekly": weekly, "monthly": monthly}
    with open(MTBI_JSON_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    log(f"[OK] 저장 완료 → {MTBI_JSON_PATH} "
        f"(daily={len(daily)}, weekly={len(weekly)}, monthly={len(monthly)})")

def load_daily() -> List[Dict]:
    if not os.path.isfile(MTBI_JSON_PATH):
        return []
    try:
        with open(MTBI_JSON_PATH, "r", encoding="utf-8") as f:
            j = json.load(f)
        # 가능한 한 필드 보존(하위호환: work/err 누락 가능)
        out = []
        for r in (j.get("daily", []) or []):
            out.append({
                "date": str(r.get("date", "")),
                "work": r.get("work"),
                "err":  r.get("err"),
                "mtbi": r.get("mtbi"),
            })
        return out
    except Exception:
        return []

# ====================== 동작 모드 ======================

def init_days(n: int):
    """
    초기 구축: 과거 n일치 생성
    - 종료일 = D-2 (오늘/어제 제외)
    """
    today = today_local()
    end = two_days_ago(today)             # D-2
    start = end - timedelta(days=n - 1)
    log(f"[INIT] 기간: {start} ~ {end} ({n}일, 오늘·어제 제외)")

    daily = []
    total = (end - start).days + 1
    i = 0
    for d in daterange(start, end):
        i += 1
        log(f"[INIT] ({i}/{total}) {d} 계산")
        daily.append(calc_daily_record(d))

    # 안전 클립(D-2 이하만)
    daily = clip_daily_to_d2(daily, today)
    save_json_from_daily(daily)

def update_d2_only():
    """
    운영: **D-2 하루치만** 계산/반영 (누적 저장)
    - 기존 JSON에서 오늘/어제(D-1) 제거
    - D-2 레코드만 교체/추가
    """
    today = today_local()
    target = two_days_ago(today)          # D-2
    log(f"[UPDATE] 대상: {target} (오늘·어제 제외, D-2만 갱신)")

    # 새로 계산한 D-2 레코드
    rec = calc_daily_record(target)

    # 기존 daily 불러와서, 오늘/어제 제거 + D-2 교체/추가
    daily = load_daily()
    daily = clip_daily_to_d2(daily, today)        # D-2 이하만 유지
    daily = [r for r in daily if r["date"] != rec["date"]]
    daily.append(rec)
    daily.sort(key=lambda x: x["date"])

    save_json_from_daily(daily)

def run_schedule(hhmm: str):
    """
    --schedule HH:MM : 매일 지정 시각에 update_d2_only 실행(무한 루프)
    """
    try:
        hh, mm = map(int, hhmm.split(":"))
        assert 0 <= hh <= 23 and 0 <= mm <= 59
    except Exception:
        raise ValueError("시간 형식은 HH:MM (예: 06:05)")

    log(f"[SCHED] 매일 {hh:02d}:{mm:02d} 실행 대기 시작 (무한루프)")
    while True:
        now = datetime.now()
        today_run = now.replace(hour=hh, minute=mm, second=0, microsecond=0)
        if now <= today_run:
            next_run = today_run
        else:
            next_run = today_run + timedelta(days=1)

        wait_sec = (next_run - now).total_seconds()
        log(f"[SCHED] 다음 실행: {next_run} (약 {int(wait_sec)}초 후)")
        # 1분 간격으로 대기
        while wait_sec > 0:
            nap = min(60, wait_sec)
            time.sleep(nap)
            wait_sec -= nap

        try:
            update_d2_only()
        except Exception as e:
            log_error("[SCHED] update_d2_only 실행 중 예외", e)
        # 이후 다음 루프로 반복

# ====================== 메인 ======================

def main():
    parser = argparse.ArgumentParser(description="MTBI 배치 (D-2까지 저장 · 주/월=합계기반 · work_date조건 포함)")
    g = parser.add_mutually_exclusive_group()
    g.add_argument("--init-days", type=int, help="초기 구축: 과거 N일 생성 (끝= D-2)")
    g.add_argument("--update", action="store_true", help="D-2 하루치 갱신/누적 저장")
    g.add_argument("--schedule", type=str, help="매일 HH:MM에 --update 수행(무한 실행)")
    args = parser.parse_args()

    log(f"[CONFIG] RUNTIME={TABLE_RUNTIME}, ERROR={TABLE_ERROR}, CLASS={LARGE_CLASS}")
    log(f"[CONFIG] OUTPUT={MTBI_JSON_PATH}")

    if args.init_days:
        init_days(args.init_days)
        return

    if args.update:
        update_d2_only()
        return

    if args.schedule:
        run_schedule(args.schedule)
        return

    parser.print_help()

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log_error("치명적 예외로 종료", e)
        sys.exit(1)
