# -*- coding: utf-8 -*-
"""
스톡캐쳐 데이터 파이프라인 VOL1
────────────────────────────────
매일 장 마감 후 실행:
  1) pykrx로 코스피·코스닥 전 종목 일봉 수집 (최근 170거래일)
  2) 지표 일괄 계산 (이평/RSI/MACD/스토캐스틱/볼린저/OBV 등)
  3) data/screener.json 생성 → 스톡캐쳐 프론트가 fetch

실행: python collect_krx.py
필요: pip install pykrx pandas numpy
API 키 불필요 — pykrx는 KRX 공개 데이터를 그대로 가져와요.
"""
import json, time, datetime, os, sys, shutil
import numpy as np
import pandas as pd
from pykrx import stock

LOOKBACK_DAYS = 170        # 지표 계산용 거래일 수 (MA60 + MACD 워밍업 여유 포함)
SPARK_LEN = 60             # 프론트 미니차트 길이
CHART_LEN = 120            # 종목별 풀차트 데이터 길이 (거래일)
OUT_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "screener.json")
SLEEP = 0.35               # KRX 요청 간격 (예의상 유지)
EXCLUDE_KEYWORDS = ("스팩", "리츠")   # 이름 기준 제외
COMMON_ONLY = True         # True면 보통주만 (코드 끝자리 0) — 우선주 제외
SUPPLY_DAYS = 120          # 외국인/기관 수급 누적 조회 거래일 수


def business_days(n):
    """오늘 기준 최근 n 거래일 목록 (오래된 → 최신)

    pykrx의 get_previous_business_days는 조회 기간이 길면(약 6개월 이상)
    KRX 서버가 빈 응답을 반환해 JSONDecodeError가 나는 알려진 이슈가 있다.
    (https://github.com/sharebook-kr/pykrx/issues/148)
    그래서 90일 단위로 잘라 여러 번 조회한 뒤 합친다.
    """
    end = datetime.datetime.now()
    all_days = []
    chunk_days = 90
    cursor = end
    max_chunks = 20  # 안전장치 (90일 * 20 = 약 5년, n=170에는 충분)
    for _ in range(max_chunks):
        if len(all_days) >= n:
            break
        fromdate = cursor - datetime.timedelta(days=chunk_days)
        chunk = None
        for attempt in range(3):
            try:
                chunk = stock.get_previous_business_days(
                    fromdate=fromdate.strftime("%Y%m%d"),
                    todate=cursor.strftime("%Y%m%d"),
                )
                break
            except Exception as e:
                print(f"  거래일 조회 재시도 {attempt+1}/3 "
                      f"({fromdate:%Y%m%d}~{cursor:%Y%m%d}): {e}", flush=True)
                time.sleep(2 + attempt * 3)
        if chunk is not None and len(chunk):
            all_days = list(chunk) + all_days
        cursor = fromdate - datetime.timedelta(days=1)
        time.sleep(SLEEP)

    # 중복 제거 + 정렬
    uniq = sorted(set(all_days))
    if len(uniq) < n:
        print(f"  ⚠️ 거래일을 {n}개 채우지 못했어요 (확보: {len(uniq)}개). "
              f"확보된 만큼만 진행해요.", flush=True)
    return [d.strftime("%Y%m%d") for d in uniq][-n:]


def collect(dates):
    """날짜별 전 종목 스냅샷 수집 → 종목별 시계열로 피벗"""
    frames = []
    total = len(dates) * 2
    step = 0
    for d in dates:
        for mkt in ("KOSPI", "KOSDAQ"):
            step += 1
            for attempt in range(3):
                try:
                    df = stock.get_market_ohlcv(d, market=mkt)
                    break
                except Exception as e:
                    print(f"  재시도 {attempt+1}/3 — {d} {mkt}: {e}", flush=True)
                    time.sleep(2 + attempt * 3)
            else:
                print(f"  ⚠️ 수집 실패 스킵: {d} {mkt}", flush=True)
                continue
            if df is None or df.empty:
                continue
            df = df.reset_index()
            df["date"] = d
            df["market"] = mkt
            frames.append(df[["티커", "date", "market", "시가", "고가", "저가", "종가", "거래량", "거래대금"]])
            if step % 20 == 0:
                print(f"  수집 진행 {step}/{total} ({d} {mkt})", flush=True)
            time.sleep(SLEEP)
    raw = pd.concat(frames, ignore_index=True)
    return raw


def investor_flags(dates):
    """최근 SUPPLY_DAYS 거래일 외국인/기관합계 순매수거래대금 누적 (종목별 합계)

    KRX는 기간을 한 번에 조회할 수 있어, 하루씩 120번 부르는 대신
    시작일~종료일 범위를 시장·투자자별 1회씩 조회한다(총 4회).
    """
    frg_sum, inst_sum = {}, {}       # 종목별 기간 순매수 합계(원)
    frg_days, inst_days = {}, {}     # 종목별 순매수(양수) 일수 — 연속 판정 대체용
    span = dates[-SUPPLY_DAYS:] if len(dates) >= SUPPLY_DAYS else dates
    f_from, f_to = span[0], span[-1]
    for inv, sum_store, day_store in (("외국인", frg_sum, frg_days),
                                      ("기관합계", inst_sum, inst_days)):
        for mkt in ("KOSPI", "KOSDAQ"):
            df = None
            for attempt in range(3):
                try:
                    df = stock.get_market_net_purchases_of_equities(f_from, f_to, mkt, inv)
                    break
                except Exception as e:
                    print(f"  수급 재시도 {attempt+1}/3 — {mkt} {inv}: {e}", flush=True)
                    time.sleep(2 + attempt * 3)
            if df is None or df.empty:
                continue
            df = df.reset_index()
            for _, row in df.iterrows():
                code = str(row["티커"])
                net = float(row["순매수거래대금"])
                sum_store[code] = sum_store.get(code, 0) + net
                if net > 0:
                    day_store[code] = day_store.get(code, 0) + 1
            time.sleep(SLEEP)
    return frg_sum, inst_sum, frg_days, inst_days


def ema(arr, n):
    k = 2.0 / (n + 1)
    out = np.empty_like(arr, dtype=float)
    e = arr[0]
    for i, v in enumerate(arr):
        e = v * k + e * (1 - k)
        out[i] = e
    return out


def rsi_wilder(c, n=14):
    out = np.full(len(c), np.nan)
    g = l = 0.0
    for i in range(1, len(c)):
        d = c[i] - c[i - 1]
        up, dn = max(d, 0.0), max(-d, 0.0)
        if i <= n:
            g += up; l += dn
            if i == n:
                out[i] = 100 - 100 / (1 + (g / n) / ((l / n) or 1e-9))
        else:
            g = g * (n - 1) / n + up
            l = l * (n - 1) / n + dn
            out[i] = 100 - 100 / (1 + g / (l or 1e-9))
    return out


def analyze_ticker(code, name, market, op, h, lo, c, v, amt):
    """스톡캐쳐 지표 엔진 v4 (44종) — 프론트 JS 엔진과 동일 수식"""
    n = len(c)
    if n < 90:
        return None
    i = n - 1
    s = pd.Series(c)
    ma5 = s.rolling(5).mean().values
    ma20 = s.rolling(20).mean().values
    ma60 = s.rolling(60).mean().values
    e12, e26 = ema(c, 12), ema(c, 26)
    macd = e12 - e26
    sig = ema(macd, 9)
    hist = macd - sig
    rsi = rsi_wilder(c, 14)

    hh14 = pd.Series(h).rolling(14).max().values
    ll14 = pd.Series(lo).rolling(14).min().values
    stk = (c - ll14) / np.where((hh14 - ll14) == 0, 1e-9, (hh14 - ll14)) * 100

    sd = s.rolling(20).std(ddof=0).values
    bb_up, bb_lo = ma20 + 2 * sd, ma20 - 2 * sd
    bw = (bb_up - bb_lo) / np.where(ma20 == 0, 1e-9, ma20) * 100
    bw_win = bw[max(19, i - 59): i + 1]
    bw_win = bw_win[~np.isnan(bw_win)]
    squeeze = bool(len(bw_win) > 5 and (bw_win <= bw[i]).sum() / len(bw_win) <= 0.2)

    obv = np.zeros(n)
    for k in range(1, n):
        obv[k] = obv[k - 1] + (v[k] if c[k] > c[k - 1] else -v[k] if c[k] < c[k - 1] else 0)

    # ATR(14, Wilder)
    tr = np.empty(n); tr[0] = h[0] - lo[0]
    for k in range(1, n):
        tr[k] = max(h[k] - lo[k], abs(h[k] - c[k - 1]), abs(lo[k] - c[k - 1]))
    atr = tr[1:15].sum() / 14
    for k in range(15, n):
        atr = (atr * 13 + tr[k]) / 14
    atr_pct = atr / c[i] * 100

    # DMI/ADX(14, Wilder)
    pdm = ndm = trs = 0.0
    sp = sn = st = 0.0
    pdi_a, ndi_a, dx_a = [], [], []
    for k in range(1, n):
        up, dn = h[k] - h[k - 1], lo[k - 1] - lo[k]
        pd_ = up if (up > dn and up > 0) else 0.0
        nd_ = dn if (dn > up and dn > 0) else 0.0
        if k <= 14:
            sp += pd_; sn += nd_; st += tr[k]
            if k == 14:
                pdm, ndm, trs = sp, sn, st
        else:
            pdm = pdm - pdm / 14 + pd_
            ndm = ndm - ndm / 14 + nd_
            trs = trs - trs / 14 + tr[k]
        if k >= 14:
            pdi = 100 * pdm / (trs or 1e-9); ndi = 100 * ndm / (trs or 1e-9)
            pdi_a.append(pdi); ndi_a.append(ndi)
            dx_a.append(100 * abs(pdi - ndi) / ((pdi + ndi) or 1e-9))
    adx = sum(dx_a[:14]) / 14
    for k in range(14, len(dx_a)):
        adx = (adx * 13 + dx_a[k]) / 14
    di_pos, di_neg = pdi_a[-1], ndi_a[-1]

    # 파라볼릭 SAR (0.02 / 0.2)
    ps_up = c[1] > c[0]; af = 0.02
    ep = h[1] if ps_up else lo[1]
    sar = lo[0] if ps_up else h[0]
    for k in range(2, n):
        sar = sar + af * (ep - sar)
        if ps_up:
            if lo[k] < sar:
                ps_up = False; sar = ep; ep = lo[k]; af = 0.02
            elif h[k] > ep:
                ep = h[k]; af = min(af + 0.02, 0.2)
        else:
            if h[k] > sar:
                ps_up = True; sar = ep; ep = h[k]; af = 0.02
            elif lo[k] < ep:
                ep = lo[k]; af = min(af + 0.02, 0.2)

    # 일목균형표
    def _hh(a, m, k): return float(np.max(a[max(0, k - m + 1): k + 1]))
    def _ll(a, m, k): return float(np.min(a[max(0, k - m + 1): k + 1]))
    conv = lambda k: (_hh(h, 9, k) + _ll(lo, 9, k)) / 2
    base = lambda k: (_hh(h, 26, k) + _ll(lo, 26, k)) / 2
    span_a = (conv(i - 26) + base(i - 26)) / 2
    span_b = (_hh(h, 52, i - 26) + _ll(lo, 52, i - 26)) / 2
    cloud_top = max(span_a, span_b)
    ichi_above = bool(c[i] > cloud_top)
    ichi_tk = bool(conv(i - 1) <= base(i - 1) and conv(i) > base(i))

    # CCI(20)
    tp = (h + lo + c) / 3
    def cci_at(k):
        m = float(np.mean(tp[k - 19: k + 1]))
        md = float(np.mean(np.abs(tp[k - 19: k + 1] - m)))
        return (tp[k] - m) / (0.015 * (md or 1e-9))
    cci, cci_prev = cci_at(i), cci_at(i - 1)

    # 윌리엄스 %R(14)
    wr = (_hh(h, 14, i) - c[i]) / ((_hh(h, 14, i) - _ll(lo, 14, i)) or 1e-9) * -100

    # ROC(12)
    roc = (c[i] / c[i - 12] - 1) * 100

    # VR(20)
    up_v = dn_v = eq_v = 0.0
    for k in range(i - 19, i + 1):
        if c[k] > c[k - 1]: up_v += v[k]
        elif c[k] < c[k - 1]: dn_v += v[k]
        else: eq_v += v[k]
    vr = (up_v + eq_v / 2) / max(dn_v + eq_v / 2, 1e-9) * 100

    # MFI(14)
    pmf = nmf = 0.0
    for k in range(i - 13, i + 1):
        mf = tp[k] * v[k]
        if tp[k] > tp[k - 1]: pmf += mf
        elif tp[k] < tp[k - 1]: nmf += mf
    mfi = 100 - 100 / (1 + pmf / max(nmf, 1e-9))

    # 엔벨로프(20, ±6%)
    env_up_line, env_lo_line = ma20[i] * 1.06, ma20[i] * 0.94

    # 캔들 패턴
    body = c[i] - op[i]
    rng2 = max(h[i] - lo[i], 1e-9)
    body_abs = abs(body)
    big_bull = bool(op[i] > 0 and body / op[i] >= 0.03 and body / rng2 >= 0.6)
    low_sh = min(op[i], c[i]) - lo[i]
    up_sh = h[i] - max(op[i], c[i])
    hammer = bool(low_sh >= 2 * body_abs and up_sh <= body_abs and c[i] < ma20[i])
    three_white = bool(c[i] > op[i] and c[i-1] > op[i-1] and c[i-2] > op[i-2] and c[i] > c[i-1] > c[i-2])

    # 저점 높이기
    rising_lows = bool(_ll(lo, 5, i) > _ll(lo, 5, i - 5) > _ll(lo, 5, i - 10))

    v_avg20 = np.mean(v[i - 20:i]) if i >= 20 else float(np.mean(v[:i] if i else [v[i]]))
    chg = (c[i] - c[i - 1]) / c[i - 1] * 100
    chg5 = (c[i] - c[i - 5]) / c[i - 5] * 100 if i >= 5 else 0.0
    hi120 = _hh(h, 120, i)
    hi20prev = float(np.max(h[max(0, i - 20): i]))
    lo120 = _ll(lo, 120, i)
    disp20 = c[i] / ma20[i] * 100

    def f(x, d=2):
        return None if x is None or (isinstance(x, float) and not np.isfinite(x)) else round(float(x), d)

    return {
        "code": code, "name": name, "market": market,
        "price": f(c[i], 0), "chg": f(chg), "chg5": f(chg5),
        "amt": f(amt[i], 0), "volRatio": f(v[i] / (v_avg20 or 1e-9)),
        "ma5": f(ma5[i], 0), "ma20": f(ma20[i], 0), "ma60": f(ma60[i], 0),
        "aligned": bool(ma5[i] > ma20[i] > ma60[i]),
        "reversed": bool(ma5[i] < ma20[i] < ma60[i]),
        "gcross": bool(ma5[i - 1] <= ma20[i - 1] and ma5[i] > ma20[i]),
        "dcross": bool(ma5[i - 1] >= ma20[i - 1] and ma5[i] < ma20[i]),
        "above20": bool(c[i] > ma20[i] and c[i - 1] <= ma20[i - 1]),
        "on20": bool(c[i] > ma20[i]),
        "macdGC": bool(macd[i - 1] <= sig[i - 1] and macd[i] > sig[i]),
        "histTurn": bool(hist[i - 1] <= 0 and hist[i] > 0),
        "rsi": f(rsi[i]), "stochK": f(stk[i]),
        "stochRebound": bool(np.isfinite(stk[i - 1]) and stk[i - 1] < 25 and stk[i] > stk[i - 1]),
        "bbUp": f(bb_up[i], 0), "bbLo": f(bb_lo[i], 0),
        "bbNearLo": bool(c[i] <= bb_lo[i] * 1.02),
        "bbBreakUp": bool(c[i] > bb_up[i]),
        "squeeze": squeeze,
        "obvUp": bool(obv[i] > obv[i - 20]) if i >= 20 else False,
        "near52": bool(c[i] >= hi120 * 0.95),
        "newHigh20": bool(c[i] > hi20prev),
        "near52Low": bool(c[i] <= lo120 * 1.05),
        "hi20prev": f(hi20prev, 0), "lo120": f(lo120, 0), "hi120": f(hi120, 0),
        "disp20": f(disp20), "adx": f(adx),
        "adxUpDir": bool(di_pos > di_neg),
        "adxStrong": bool(adx >= 25 and di_pos > di_neg),
        "psarUp": bool(ps_up),
        "ichiAbove": ichi_above, "ichiTK": ichi_tk, "cloudTop": f(cloud_top, 0),
        "cci": f(cci), "cciBreak": bool(cci_prev <= 100 and cci > 100), "cciLow": bool(cci <= -100),
        "wr": f(wr), "wrLow": bool(wr <= -80),
        "roc": f(roc), "vr": f(vr), "vrLow": bool(vr <= 70),
        "mfi": f(mfi), "mfiLow": bool(mfi <= 20), "mfiHigh": bool(mfi >= 80),
        "atrPct": f(atr_pct),
        "envUpLine": f(env_up_line, 0), "envLoLine": f(env_lo_line, 0),
        "envLoNear": bool(c[i] <= env_lo_line * 1.01),
        "envUpBreak": bool(c[i] > env_up_line),
        "bigBull": big_bull, "hammer": hammer, "threeWhite": three_white,
        "risingLows": rising_lows,
        "spark": [int(x) for x in c[-SPARK_LEN:]],
    }


def main():
    print("📡 스톡캐쳐 파이프라인 시작", flush=True)
    dates = business_days(LOOKBACK_DAYS)
    if not dates:
        print("❌ 거래일 목록을 하나도 확보하지 못했어요. "
              "KRX 서버 접속 자체가 막혔을 가능성이 있어요 "
              "(네트워크 차단, 방화벽, VPN 등을 확인해 주세요).", flush=True)
        sys.exit(1)
    base_date = dates[-1]
    print(f"  기간: {dates[0]} ~ {base_date} ({len(dates)}거래일)", flush=True)

    raw = collect(dates)
    print(f"  원시 행수: {len(raw):,}", flush=True)

    # 종목명 매핑
    names = {}
    for mkt in ("KOSPI", "KOSDAQ"):
        for t in stock.get_market_ticker_list(base_date, market=mkt):
            names[t] = stock.get_market_ticker_name(t)
        time.sleep(SLEEP)

    print(f"  수급(외국인/기관) {SUPPLY_DAYS}거래일 누적 수집 중…", flush=True)
    frg_sum, inst_sum, frg_days, inst_days = investor_flags(dates)

    results = []
    charts = {}
    grouped = raw.sort_values("date").groupby("티커")
    for code, g in grouped:
        name = names.get(code)
        if not name:
            continue
        if COMMON_ONLY and not code.endswith("0"):
            continue
        if any(k in name for k in EXCLUDE_KEYWORDS):
            continue
        c = g["종가"].to_numpy(dtype=float)
        if len(c) < 70 or c[-1] <= 0:
            continue
        op = g["시가"].to_numpy(dtype=float)
        hi = g["고가"].to_numpy(dtype=float)
        lw = g["저가"].to_numpy(dtype=float)
        vv = g["거래량"].to_numpy(dtype=float)
        r = analyze_ticker(code, name, g["market"].iloc[-1],
                           op, hi, lw, c, vv, g["거래대금"].to_numpy(dtype=float))
        if r:
            fnet = round(frg_sum.get(code, 0))
            inet = round(inst_sum.get(code, 0))
            fdays = frg_days.get(code, 0)
            idays = inst_days.get(code, 0)
            # 120일 누적 순매수(양수) = 기간 전체 순매수 우위
            r["frgBuy120"] = bool(fnet > 0)
            r["instBuy120"] = bool(inet > 0)
            r["frgNet120"] = fnet
            r["instNet120"] = inet
            r["frgDays120"] = fdays      # 120일 중 외국인 순매수 일수
            r["instDays120"] = idays     # 120일 중 기관 순매수 일수
            # 하위호환(VOL6 프론트가 참조하는 3일 필드) — 120일 지표로 매핑
            r["frgBuy3"] = r["frgBuy120"]
            r["instBuy3"] = r["instBuy120"]
            r["frgNet3"] = fnet
            r["instNet3"] = inet
            results.append(r)
            charts[code] = {
                "o": [int(x) for x in op[-CHART_LEN:]],
                "h": [int(x) for x in hi[-CHART_LEN:]],
                "l": [int(x) for x in lw[-CHART_LEN:]],
                "c": [int(x) for x in c[-CHART_LEN:]],
                "v": [int(x) for x in vv[-CHART_LEN:]],
            }

    out = {
        "baseDate": base_date,
        "generatedAt": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "count": len(results),
        "stocks": results,
    }
    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    with open(OUT_PATH, "w", encoding="utf-8") as fp:
        json.dump(out, fp, ensure_ascii=False, separators=(",", ":"))
    charts_dir = os.path.join(os.path.dirname(OUT_PATH), "charts")
    if os.path.isdir(charts_dir):
        shutil.rmtree(charts_dir)
    os.makedirs(charts_dir, exist_ok=True)
    for code, ch in charts.items():
        with open(os.path.join(charts_dir, f"{code}.json"), "w", encoding="utf-8") as fp:
            json.dump(ch, fp, ensure_ascii=False, separators=(",", ":"))

    size = os.path.getsize(OUT_PATH) / 1024 / 1024
    print(f"✅ 완료 — {len(results):,}종목 / 스크리너 {size:.1f}MB / 차트 파일 {len(charts):,}개 → data/", flush=True)


if __name__ == "__main__":
    main()
