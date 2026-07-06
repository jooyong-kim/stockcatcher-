# -*- coding: utf-8 -*-
"""
스톡캐쳐 장기 아카이브 수집기 v1
────────────────────────────────
목적: 종목별 전체 히스토리(상장 초기까지)를 증분 방식으로 수집·보관

동작 원리:
  1) 종목마다 "현재 → 과거" 방향으로 3년 청크씩 나눠 수집
  2) 이미 모은 구간은 다시 받지 않음 (상태 파일에 진행 상황 기록)
  3) 과거로 가다가 데이터가 2회 연속 비어 있으면 → 상장 초기 도달로 판정, 완료 표시
  4) 매 실행마다 최신 날짜까지의 빈 구간도 자동 보충 (전진 수집)
  5) 실행 시간 예산(기본 50분)이 다 되면 저장 후 종료 → 다음 실행 시 이어서

실행:
  export KRX_ID='아이디'
  export KRX_PW='비밀번호'
  python3 archive_krx.py                 # 기본 (50분 예산, 3년 청크)
  python3 archive_krx.py --minutes 120   # 2시간 동안 수집
  python3 archive_krx.py --limit 100     # 이번 실행은 100종목만

저장 위치:
  data_archive/{종목코드}.csv   (date,open,high,low,close,volume)
  data_archive/archive_state.json  (종목별 진행 상황)

주의:
  아카이브 전체 용량은 수백 MB까지 커질 수 있어요.
  GitHub 저장소에는 올리지 말고 로컬(또는 외장/클라우드 드라이브)에 보관하는 걸 권해요.
"""
import argparse
import csv
import datetime
import json
import os
import sys
import time

from pykrx import stock

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ARCHIVE_DIR = os.path.join(BASE_DIR, "..", "data_archive")
STATE_PATH = os.path.join(ARCHIVE_DIR, "archive_state.json")

CHUNK_DAYS_DEFAULT = 365 * 3      # 3년 청크
MIN_CHUNK_DAYS = 180              # 서버가 거부하면 여기까지 줄여서 재시도
SLEEP = 0.35                      # 요청 간격
EMPTY_STREAK_FOR_COMPLETE = 2     # 연속 빈 청크 → 상장 초기 도달 판정
EXCLUDE_KEYWORDS = ("스팩", "리츠")
COMMON_ONLY = True


def log(msg):
    print(msg, flush=True)


def load_state():
    if os.path.exists(STATE_PATH):
        try:
            with open(STATE_PATH, encoding="utf-8") as fp:
                return json.load(fp)
        except Exception:
            pass
    return {}


def save_state(state):
    os.makedirs(ARCHIVE_DIR, exist_ok=True)
    with open(STATE_PATH, "w", encoding="utf-8") as fp:
        json.dump(state, fp, ensure_ascii=False, indent=1)


def csv_path(code):
    return os.path.join(ARCHIVE_DIR, f"{code}.csv")


def read_rows(code):
    """기존 CSV → {date(str): row(list)} 딕셔너리"""
    path = csv_path(code)
    rows = {}
    if os.path.exists(path):
        with open(path, encoding="utf-8") as fp:
            reader = csv.reader(fp)
            header = next(reader, None)
            for r in reader:
                if r:
                    rows[r[0]] = r
    return rows


def write_rows(code, rows):
    """{date: row} → 날짜 오름차순 CSV 저장"""
    os.makedirs(ARCHIVE_DIR, exist_ok=True)
    with open(csv_path(code), "w", encoding="utf-8", newline="") as fp:
        w = csv.writer(fp)
        w.writerow(["date", "open", "high", "low", "close", "volume"])
        for d in sorted(rows):
            w.writerow(rows[d])


def fetch_range(code, fromdate, todate):
    """지정 기간 일봉 조회. 성공 시 DataFrame, 서버 오류 지속 시 None"""
    for attempt in range(3):
        try:
            df = stock.get_market_ohlcv_by_date(fromdate, todate, code)
            return df
        except Exception as e:
            log(f"    재시도 {attempt+1}/3 ({code} {fromdate}~{todate}): {e}")
            time.sleep(2 + attempt * 3)
    return None


def df_to_rows(df):
    out = {}
    if df is None or df.empty:
        return out
    for idx, row in df.iterrows():
        d = idx.strftime("%Y%m%d") if hasattr(idx, "strftime") else str(idx)
        try:
            out[d] = [d, int(row["시가"]), int(row["고가"]), int(row["저가"]),
                      int(row["종가"]), int(row["거래량"])]
        except Exception:
            continue
    return out


def get_universe():
    """코스피+코스닥 보통주 (스팩·리츠 제외)"""
    today = datetime.datetime.now().strftime("%Y%m%d")
    universe = []
    for mkt in ("KOSPI", "KOSDAQ"):
        for t in stock.get_market_ticker_list(today, market=mkt):
            if COMMON_ONLY and not t.endswith("0"):
                continue
            name = stock.get_market_ticker_name(t)
            if any(k in str(name) for k in EXCLUDE_KEYWORDS):
                continue
            universe.append((t, str(name), mkt))
        time.sleep(SLEEP)
    return universe


def process_stock(code, name, st, chunk_days):
    """한 종목에 대해 이번 실행분 작업: 전진 보충 1회 + 후진 청크 1회"""
    rows = read_rows(code)
    changed = False
    today = datetime.datetime.now().strftime("%Y%m%d")

    # ── 1) 전진 보충: 마지막 수집일 다음날 ~ 오늘 ──
    latest = st.get("latest")
    if rows and latest and latest < today:
        f = (datetime.datetime.strptime(latest, "%Y%m%d")
             + datetime.timedelta(days=1)).strftime("%Y%m%d")
        df = fetch_range(code, f, today)
        time.sleep(SLEEP)
        new = df_to_rows(df)
        if new:
            rows.update(new)
            st["latest"] = max(rows)
            changed = True

    # ── 2) 후진 수집: 가장 오래된 날짜에서 3년 더 과거로 ──
    if not st.get("complete"):
        if rows:
            earliest_dt = datetime.datetime.strptime(min(rows), "%Y%m%d")
            t_end = earliest_dt - datetime.timedelta(days=1)
        else:
            t_end = datetime.datetime.now()
        t_start = t_end - datetime.timedelta(days=chunk_days)
        f, t = t_start.strftime("%Y%m%d"), t_end.strftime("%Y%m%d")

        df = fetch_range(code, f, t)
        time.sleep(SLEEP)

        # 서버 오류 지속 → 청크 절반으로 줄여 1회 더
        if df is None and chunk_days > MIN_CHUNK_DAYS:
            half = chunk_days // 2
            t_start = t_end - datetime.timedelta(days=half)
            df = fetch_range(code, t_start.strftime("%Y%m%d"), t)
            time.sleep(SLEEP)

        if df is None:
            st["error_count"] = st.get("error_count", 0) + 1
            log(f"  ⚠️ {name}({code}): 서버 오류로 이번 회차 건너뜀")
        else:
            new = df_to_rows(df)
            if new:
                rows.update(new)
                st["empty_streak"] = 0
                changed = True
            else:
                st["empty_streak"] = st.get("empty_streak", 0) + 1
                if st["empty_streak"] >= EMPTY_STREAK_FOR_COMPLETE:
                    st["complete"] = True
                    log(f"  🏁 {name}({code}): 상장 초기까지 수집 완료 "
                        f"(최초 데이터 {min(rows) if rows else '-'})")
                else:
                    # 빈 구간일 수 있으니 다음 실행에서 한 번 더 과거 탐색
                    # 탐색 기준점을 청크만큼 과거로 이동
                    probe = (t_start - datetime.timedelta(days=1)).strftime("%Y%m%d")
                    st["probe_before"] = probe
            # probe 지원: 이전 실행에서 빈 청크였다면 그 이전 구간을 기준으로
            if rows:
                st["earliest"] = min(rows)
                st["latest"] = max(rows)

    if changed:
        write_rows(code, rows)
    st["rows"] = len(rows)
    return st


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--minutes", type=int, default=50, help="실행 시간 예산(분)")
    ap.add_argument("--limit", type=int, default=0, help="이번 실행 최대 종목 수 (0=제한 없음)")
    ap.add_argument("--chunk-years", type=int, default=3, help="후진 수집 청크(년)")
    args = ap.parse_args()

    chunk_days = args.chunk_years * 365
    deadline = time.time() + args.minutes * 60

    log("📦 스톡캐쳐 장기 아카이브 수집기 시작")
    log(f"  청크: {args.chunk_years}년 / 시간 예산: {args.minutes}분")

    state = load_state()

    log("  종목 목록 불러오는 중…")
    universe = get_universe()
    log(f"  대상: {len(universe):,}종목")

    # 미완료 종목 우선 (완료 종목은 전진 보충만 빠르게)
    incomplete = [(c, n, m) for c, n, m in universe
                  if not state.get(c, {}).get("complete")]
    complete = [(c, n, m) for c, n, m in universe
                if state.get(c, {}).get("complete")]
    ordered = incomplete + complete

    done = 0
    for i, (code, name, mkt) in enumerate(ordered):
        if time.time() > deadline:
            log(f"  ⏰ 시간 예산 소진 — {done}종목 처리 후 저장하고 종료해요. "
                f"다음 실행 시 이어서 진행돼요.")
            break
        if args.limit and done >= args.limit:
            log(f"  종목 수 제한({args.limit}) 도달 — 저장 후 종료해요.")
            break

        st = state.get(code, {})
        st["name"] = name
        st["market"] = mkt
        try:
            state[code] = process_stock(code, name, st, chunk_days)
        except Exception as e:
            log(f"  ⚠️ {name}({code}) 처리 오류: {e}")
        done += 1

        if done % 50 == 0:
            save_state(state)
            n_complete = sum(1 for v in state.values() if v.get("complete"))
            log(f"  진행 {done}/{len(ordered)} · 전체 히스토리 완료 {n_complete:,}종목")

    save_state(state)

    # ── 요약 ──
    n_complete = sum(1 for v in state.values() if v.get("complete"))
    n_partial = sum(1 for v in state.values()
                    if v.get("rows") and not v.get("complete"))
    total_rows = sum(v.get("rows", 0) for v in state.values())
    earliest_all = min((v.get("earliest") for v in state.values()
                        if v.get("earliest")), default="-")
    log("━━━━━━━━━━━━━━━━━━")
    log(f"✅ 이번 실행 처리: {done:,}종목")
    log(f"  🏁 상장 초기까지 완료: {n_complete:,}종목")
    log(f"  ⏳ 수집 진행 중: {n_partial:,}종목")
    log(f"  누적 데이터: {total_rows:,}행 / 가장 오래된 날짜: {earliest_all}")
    log(f"  저장 위치: {os.path.abspath(ARCHIVE_DIR)}")
    if n_partial:
        log("  같은 명령을 다시 실행하면 이어서 더 과거 구간을 수집해요.")


if __name__ == "__main__":
    main()
