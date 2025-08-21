import pandas as pd
from flask import Flask, request, jsonify, render_template

app = Flask(__name__)

EXCEL_FILE = "Commission_Structure_Jun25_Volume.xlsx"

# Cached DataFrames
wm_df = None                 # "WM Commission"
new_deposit_df = None        # "New Deposit Commission"


# ========= Utilities =========
def _normalize_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = (
        df.columns.astype(str)
        .str.replace(r"\s+", " ", regex=True)
        .str.replace("\n", " ", regex=False)
        .str.strip()
    )
    return df


def _find_like_col(df: pd.DataFrame, must_contain_all=None, exact_any=None):
    """
    Find first column whose normalized lowercase name contains all substrings in `must_contain_all`
    or equals any in `exact_any`.
    """
    cols = list(df.columns)
    low = {c: str(c).strip().lower() for c in cols}
    if exact_any:
        exact = [s.strip().lower() for s in exact_any]
        for c in cols:
            if low[c] in exact:
                return c
    if must_contain_all:
        parts = [s.strip().lower() for s in must_contain_all]
        for c in cols:
            name = low[c]
            if all(p in name for p in parts):
                return c
    return None


def _get_number(val):
    try:
        return float(val)
    except Exception:
        return 0.0


# ========= Loaders =========
def load_wm_sheet():
    """WM Commission (flags). Known to start headers on row 3 (index=2)."""
    global wm_df
    try:
        df = pd.read_excel(EXCEL_FILE, sheet_name="WM Commission", header=2)
        wm_df = _normalize_cols(df)
    except Exception:
        wm_df = None


def load_new_deposit_sheet():
    """
    New Deposit Commission (for Actual New Deposit sum by RM/tenure).
    Header row is not guaranteed; try a few possibilities, keep the first that has RM + Actual New Deposit.
    """
    global new_deposit_df
    for hdr in [0, 1, 2, 3, None]:
        try:
            df = pd.read_excel(EXCEL_FILE, sheet_name="New Deposit Commission", header=hdr)
            df = _normalize_cols(df)
            # Try to detect columns
            rm_col = (
                _find_like_col(df, exact_any=["rm"]) or
                _find_like_col(df, must_contain_all=["rm"])
            )
            actual_col = (
                _find_like_col(df, must_contain_all=["actual", "new", "deposit"]) or
                _find_like_col(df, exact_any=["actual new deposit"])
            )
            if rm_col and actual_col:
                new_deposit_df = df
                return
        except Exception:
            continue
    new_deposit_df = None


# ========= Lookups =========
def _find_employee_row(emp_id: str):
    if not emp_id:
        return None
    if wm_df is None:
        load_wm_sheet()
    if wm_df is None or wm_df.empty:
        return None

    df = wm_df
    emp_col = None
    for c in df.columns:
        if str(c).strip().lower().startswith("employee id"):
            emp_col = c
            break
    if emp_col is None:
        return None

    rows = df[df[emp_col].astype(str).str.strip().str.upper() == emp_id.upper()]
    if rows.empty:
        return None
    return rows.iloc[0]


def get_flags_from_excel(emp_id: str):
    """
    From 'WM Commission' sheet:
      - ytm_growth: normalized 'YTM Portfolio Growth Achiev (%)'
      - ytd_ok -> (Eligibility) > 0
      - encash_ok -> (Eligibility.1) > 0
      - cif_ok -> (Eligibility.2) > 0
    """
    row = _find_employee_row(emp_id)
    if row is None:
        return {
            "found": False,
            "ytm_growth": 0.0,
            "ytd_ok": False,
            "encash_ok": False,
            "cif_ok": False,
            "p_value": 0.0,
            "r_value": 0.0,
        }

    ytm_raw = _get_number(row.get("YTM Portfolio Growth Achiev (%)", 0.0))
    ytm_growth = ytm_raw * 100.0 if 0 < ytm_raw < 10 else ytm_raw

    ytd_ok = _get_number(row.get("Eligibility", 0.0)) > 0.0
    p_value = _get_number(row.get("Eligibility.1", 0.0))
    encash_ok = p_value > 0.0
    r_value = _get_number(row.get("Eligibility.2", 0.0))
    cif_ok = r_value > 0.0

    ret = {
        "found": True,
        "ytm_growth": ytm_growth,
        "ytd_ok": bool(ytd_ok),
        "encash_ok": bool(encash_ok),
        "cif_ok": bool(cif_ok),
        "p_value": p_value,
        "r_value": r_value,
    }
    print(f"[DEBUG] Flags for {emp_id}: {ret}")
    return ret


def get_actual_new_deposit_sum(emp_id: str, tenure: int | None = None) -> float:
    """
    From 'New Deposit Commission' sheet:
    Sum 'Actual New Deposit' for rows where RM == employee code.
    If `tenure` is provided (>0) and a 'Weight Term' column exists, filter by that term as well.
    """
    if not emp_id:
        return 0.0
    if new_deposit_df is None:
        load_new_deposit_sheet()
    if new_deposit_df is None or new_deposit_df.empty:
        return 0.0

    df = new_deposit_df
    rm_col = (
        _find_like_col(df, exact_any=["rm"]) or
        _find_like_col(df, must_contain_all=["rm"])
    )
    actual_col = (
        _find_like_col(df, must_contain_all=["actual", "new", "deposit"]) or
        _find_like_col(df, exact_any=["actual new deposit"])
    )
    weight_term_col = (
        _find_like_col(df, exact_any=["weight term"]) or
        _find_like_col(df, must_contain_all=["weight", "term"])
    )

    if not rm_col or not actual_col:
        return 0.0

    sub = df[df[rm_col].astype(str).str.strip().str.upper() == emp_id.upper()]
    if sub.empty:
        return 0.0

    if tenure and tenure > 0 and weight_term_col in sub.columns:
        # Convert both to numeric for a robust comparison
        wt = pd.to_numeric(sub[weight_term_col], errors="coerce").fillna(-1).astype(int)
        sub = sub.loc[wt == int(tenure)]

    if sub.empty:
        return 0.0

    total = pd.to_numeric(sub[actual_col], errors="coerce").fillna(0).sum()
    return float(total)


# ========= Incentive Matrices / Slabs =========
MATRIX_PERMANENT = [
    [30, 60, 90, 120, 135, 150, 180, 240, 252],
    [36, 72, 108, 144, 162, 180, 216, 288, 300],
    [42, 84, 126, 168, 189, 210, 252, 336, 360],
    [48, 96, 144, 192, 216, 240, 288, 384, 405],
    [54, 108, 162, 216, 243, 270, 324, 432, 450],
    [60, 120, 180, 240, 270, 300, 360, 480, 510],
    [66, 132, 198, 264, 297, 330, 396, 528, 540],
    [72, 144, 216, 288, 324, 360, 432, 576, 600],
]

MATRIX_CONTRACTUAL = [
    [50, 100, 150, 200, 225, 250, 300, 400, 420],
    [60, 120, 180, 240, 270, 300, 360, 480, 500],
    [70, 140, 210, 280, 315, 350, 420, 560, 600],
    [80, 160, 240, 320, 360, 400, 480, 640, 675],
    [90, 180, 270, 360, 405, 450, 540, 720, 750],
    [100, 200, 300, 400, 450, 500, 600, 800, 850],
    [110, 220, 330, 440, 495, 550, 660, 880, 900],
    [120, 240, 360, 480, 540, 600, 720, 960, 1000],
]


BUILDUP_PERMANENT = {
    12: 0.0030, 36: 0.0033, 60: 0.0036, 96: 0.0039,
    120: 0.0042, 144: 0.0045, 180: 0.0048, 216: 0.0054
}
BUILDUP_CONTRACTUAL = {
    12: 0.0050, 36: 0.0055, 60: 0.0060, 96: 0.0065,
    120: 0.0070, 144: 0.0075, 180: 0.0080, 216: 0.0090
}


def amount_index(amount: float):
    # 0..8 banding identical to UI logic
    if 0 <= amount <= 2_499_999: return 0
    if amount <= 4_999_999: return 1
    if amount <= 7_499_999: return 2
    if amount <= 9_999_999: return 3
    if amount <= 29_999_999: return 4
    if amount <= 49_999_999: return 5
    if amount <= 99_999_999: return 6
    if amount <= 200_000_000: return 7
    return 8  # top slab (flat 0.03%)


def tenure_index(tenure: int):
    # identical to UI
    if 3 <= tenure <= 5: return 0
    if 6 <= tenure <= 8: return 1
    if 9 <= tenure <= 11: return 2
    if 12 <= tenure <= 14: return 3
    if 15 <= tenure <= 17: return 4
    if 18 <= tenure <= 20: return 5
    if 21 <= tenure <= 23: return 6
    if tenure == 24: return 7
    if tenure > 24: return 8
    return -1


def closest_tenure_key(tenure: int, table: dict):
    keys = sorted(map(int, table.keys()))
    last = None
    for k in keys:
        if tenure >= k:
            last = k
    return last


# ========= Calculators =========
def calc_fixed_new(amount, tenure, employee_type, ytd_ok, encash_ok, cif_ok):
    employee_type = (employee_type or "").lower().strip()
    matrix = MATRIX_PERMANENT if employee_type == "permanent" else MATRIX_CONTRACTUAL
    ai = amount_index(amount)
    ti = tenure_index(tenure)

    if ai == 8:
        base = amount * 0.0003  # 0.03% flat for top slab
    else:
        if ti == -1:
            return 0.0, 0.0
        per_lac = matrix[ai][ti]  # BDT per lakh
        base = (amount / 100000.0) * per_lac

    incentive = 0.5 * base
    if ytd_ok:
        incentive += 0.4 * base
    if encash_ok:
        incentive += 0.05 * base
    if cif_ok:
        incentive += 0.05 * base
    return round(incentive, 2), round(base, 2)


def calc_fixed_renewal(renewal_amount: float, renewal_due: float):
    if renewal_amount <= 0:
        return 0.0, 0.0

    retention = (renewal_due / renewal_amount) * 100.0
    if retention >= 100:
        rate, cap = 0.0015, 15000
    elif retention >= 85:
        rate, cap = 0.0010, 10000
    elif retention >= 70:
        rate, cap = 0.0005, 5000
    else:
        rate, cap = 0.0, 0.0

    calculated = renewal_due * rate
    eligible = min(calculated, cap)
    return round(eligible, 2), round(retention, 2)


def calc_buildup(amounts: list, tenure_list: list, employee_type: str, repeat: bool):
    rate_table = BUILDUP_PERMANENT if (employee_type or "").lower().strip() == "permanent" else BUILDUP_CONTRACTUAL
    if not repeat:
        amount = float(amounts[0]) if amounts else 0.0
        tenure = int(tenure_list[0]) if tenure_list else 12
        k = closest_tenure_key(tenure, rate_table)
        if k is None:
            return 0.0
        base = amount * rate_table[k]
        return round(base, 2)
    else:
        total = 0.0
        for amt, ten in zip(amounts, tenure_list):
            try:
                amt_f = float(amt)
                ten_i = int(ten)
            except Exception:
                continue
            k = closest_tenure_key(ten_i, rate_table)
            if k is None:
                continue
            total += amt_f * rate_table[k]
        return round(total, 2)


# ========= Routes =========
@app.route("/")
def home():
    return render_template("index.html")


@app.route("/get_flags", methods=["POST"])
def get_flags():
    data = request.get_json(force=True)
    emp_id = (data or {}).get("employee_id", "").strip()
    info = get_flags_from_excel(emp_id)
    return jsonify(info)


@app.route("/get_new_deposit_total", methods=["POST"])
def get_new_deposit_total():
    """
    NEW: accepts tenure and filters Actual New Deposit by Weight Term if present.
    """
    data = request.get_json(force=True) or {}
    emp_id = (data.get("employee_id") or "").strip()
    tenure = int(float(data.get("tenure", 0) or 0))
    total = get_actual_new_deposit_sum(emp_id, tenure=tenure if tenure > 0 else None)
    return jsonify({
        "ok": True,
        "employee_id": emp_id,
        "tenure": tenure,
        "total_actual_new_deposit": round(total, 2)
    })


@app.route("/calculate", methods=["POST"])
def calculate():
    payload = request.get_json(force=True) or {}
    product = (payload.get("productType") or "").strip().lower()
    employee_type = (payload.get("employeeType") or "permanent").strip().lower()
    emp_id = (payload.get("employee_id", "") or "").strip()

    # Always fetch flags
    flags = get_flags_from_excel(emp_id)

    # Back-compat + split handling
    fixed_type = (payload.get("fixedType") or "").strip().lower()
    if product == "fixed":
        if fixed_type == "new":
            product = "fixed_new"
        elif fixed_type == "renewal":
            product = "fixed_renewal"

    if product == "fixed_new":
        # Amount is fetched from Excel (sum of Actual New Deposit by RM *and* Weight Term)
        tenure = int(float(payload.get("tenure", 0) or 0))
        used_amount = get_actual_new_deposit_sum(emp_id, tenure=tenure if tenure > 0 else None)
        incentive, base = calc_fixed_new(
            used_amount, tenure, employee_type,
            flags["ytd_ok"], flags["encash_ok"], flags["cif_ok"]
        )
        return jsonify({
            "ok": True,
            "type": "fixed_new",
            "used_amount": round(used_amount, 2),
            "base": base,
            "incentive": incentive,
            "ytd_ok": flags["ytd_ok"],
            "encash_ok": flags["encash_ok"],
            "cif_ok": flags["cif_ok"],
            "ytm_growth": flags["ytm_growth"],
            "p_value": flags["p_value"],
            "r_value": flags["r_value"],
        })

    elif product == "fixed_renewal":
        renewal_amount = float(payload.get("renewal_amount", 0) or 0)
        renewal_due = float(payload.get("renewal_due", 0) or 0)
        eligible, retention = calc_fixed_renewal(renewal_amount, renewal_due)
        return jsonify({
            "ok": True,
            "type": "fixed_renewal",
            "eligible": eligible,
            "retention": retention,
        })

    elif product == "buildup":
        repeat = bool(payload.get("repeat", False))
        amounts = payload.get("amounts", []) or []
        tenures = payload.get("tenures", []) or []
        incentive = calc_buildup(amounts, tenures, employee_type, repeat)
        return jsonify({
            "ok": True,
            "type": "buildup",
            "incentive": incentive,
        })

    else:
        return jsonify({"ok": False, "error": "Invalid productType"}), 400


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)
