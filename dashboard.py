"""dashboard.py — Local web dashboard for the EMS Trade Manager
=============================================================
Talks to the GitHub Actions API to display run history, logs,
and control the daily rebalance workflow.

Setup:
  1. Create a GitHub Personal Access Token (PAT):
       https://github.com/settings/tokens/new
     Required scopes: repo  (or at minimum: actions:read + actions:write)
  2. Add to your .env file:
       GITHUB_PAT=ghp_your_token_here
       GITHUB_REPO=Loudlarry/trade-manager
       DASHBOARD_PASSWORD=choose_a_strong_password
  3. Install deps:  pip install -r requirements-dashboard.txt
  4. Run:           python dashboard.py
  5. Open:          http://<your-ip>:5000

Security: Set DASHBOARD_PASSWORD in .env before exposing this to a network.
Your PAT and API keys are never sent to the browser — all API calls are
made server-side here in Python.
"""

from datetime import datetime, timedelta
import base64
import io
import json
import os
import re
import secrets
import zipfile

import yfinance as yf

import requests
from dotenv import load_dotenv, set_key
from flask import Flask, jsonify, redirect, render_template, request, session, url_for

load_dotenv()

GITHUB_PAT: str = os.getenv("GITHUB_PAT", "")
GITHUB_REPO: str = os.getenv("GITHUB_REPO", "Loudlarry/trade-manager")
WORKFLOW_FILE: str = "daily-rebalance.yml"
GITHUB_BRANCH: str = "master"
DOTENV_PATH: str   = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
HISTORY_PATH: str  = os.path.join(os.path.dirname(os.path.abspath(__file__)), "portfolio_history.json")
TICKER_RE          = re.compile(r"^[A-Z0-9.\-]{1,10}$")
DASHBOARD_PASSWORD: str = os.getenv("DASHBOARD_PASSWORD", "")

app = Flask(__name__)


def _get_or_create_secret_key() -> str:
    """Return FLASK_SECRET_KEY from env, generating one if absent.
    On hosted platforms (Render etc.) set FLASK_SECRET_KEY as an env var
    so sessions survive restarts.
    """
    key = os.getenv("FLASK_SECRET_KEY", "")
    if not key:
        key = secrets.token_hex(32)
        try:
            set_key(DOTENV_PATH, "FLASK_SECRET_KEY", key)
        except Exception:
            pass  # .env may not exist on hosted platforms — key works in-memory only
        os.environ["FLASK_SECRET_KEY"] = key
    return key


app.secret_key = _get_or_create_secret_key()


@app.before_request
def _require_auth():
    """Redirect to /login when DASHBOARD_PASSWORD is set and the session is not authenticated."""
    if not DASHBOARD_PASSWORD:
        return  # no password configured — open access (local use)
    if request.endpoint in ("login", "logout", "static"):
        return
    if not session.get("authenticated"):
        if request.path.startswith("/api/"):
            return jsonify({"error": "Unauthorized"}), 401
        return redirect(url_for("login", next=request.path))


# ── GitHub API helpers ────────────────────────────────────────────────────────

def _headers() -> dict:
    return {
        "Authorization": f"Bearer {GITHUB_PAT}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }


def _gh_get(path: str, **kwargs) -> dict:
    url = f"https://api.github.com/repos/{GITHUB_REPO}/{path}"
    resp = requests.get(url, headers=_headers(), timeout=20, **kwargs)
    resp.raise_for_status()
    return resp.json()


def _gh_put(path: str) -> None:
    url = f"https://api.github.com/repos/{GITHUB_REPO}/{path}"
    requests.put(url, headers=_headers(), timeout=15).raise_for_status()


def _gh_post(path: str, payload: dict) -> None:
    url = f"https://api.github.com/repos/{GITHUB_REPO}/{path}"
    requests.post(url, headers=_headers(), json=payload, timeout=15).raise_for_status()


def _gh_read_repo_file(repo_path: str) -> tuple:
    """Read a file from the GitHub repo. Returns (content_str, sha) or (None, None)."""
    if not GITHUB_PAT:
        return None, None
    try:
        url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{repo_path}"
        resp = requests.get(url, headers=_headers(), timeout=15,
                            params={"ref": GITHUB_BRANCH})
        if resp.status_code == 404:
            return None, None
        resp.raise_for_status()
        data = resp.json()
        content = base64.b64decode(data["content"]).decode("utf-8")
        return content, data["sha"]
    except Exception:
        return None, None


def _gh_write_repo_file(repo_path: str, content: str, sha, message: str) -> bool:
    """Commit a file to the GitHub repo. Returns True on success."""
    if not GITHUB_PAT:
        return False
    payload = {
        "message": message,
        "content": base64.b64encode(content.encode("utf-8")).decode("ascii"),
        "branch": GITHUB_BRANCH,
    }
    if sha:
        payload["sha"] = sha
    try:
        url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{repo_path}"
        resp = requests.put(url, headers=_headers(), json=payload, timeout=20)
        resp.raise_for_status()
        return True
    except Exception:
        return False


# ── Portfolio history helpers ─────────────────────────────────────────────────

def _load_history_and_sha() -> tuple:
    """Load portfolio_history.json. GitHub API is authoritative (persists on hosted
    platforms); falls back to local file for pure-local usage."""
    raw, sha = _gh_read_repo_file("portfolio_history.json")
    if raw is not None:
        try:
            return json.loads(raw), sha
        except json.JSONDecodeError:
            pass
    # Local fallback
    try:
        with open(HISTORY_PATH, encoding="utf-8") as f:
            return json.load(f), None
    except (FileNotFoundError, json.JSONDecodeError):
        return {}, None


def _load_history() -> dict:
    return _load_history_and_sha()[0]


def _append_history(date_str: str, total_value: float) -> None:
    history, sha = _load_history_and_sha()
    if date_str in history:
        return  # Already recorded today — skip to avoid unnecessary GitHub commits
    history[date_str] = round(total_value, 2)
    content = json.dumps(dict(sorted(history.items())), indent=2)
    # Try GitHub first (persists across restarts on hosted platforms)
    if _gh_write_repo_file("portfolio_history.json", content, sha,
                           f"[dashboard] portfolio snapshot {date_str}"):
        return
    # Fall back to local file
    tmp = HISTORY_PATH + ".tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            f.write(content)
        os.replace(tmp, HISTORY_PATH)
    except OSError:
        pass  # Best-effort — silently skip if filesystem is read-only


def _yahoo_adj_close(ticker: str, start_dt: datetime, end_dt: datetime) -> list:
    """Fetch daily adjusted-close prices via yfinance (handles Yahoo auth automatically).

    Adjusted close is equivalent to a total-return / DRIP series: dividends are
    folded into historical prices so the return between any two adjusted-close
    values equals buy-and-hold with all dividends reinvested.
    """
    df = yf.download(
        ticker,
        start=start_dt.strftime("%Y-%m-%d"),
        end=(end_dt + timedelta(days=1)).strftime("%Y-%m-%d"),
        auto_adjust=True,
        progress=False,
        multi_level_index=False,
    )
    if df.empty:
        return []
    points = []
    for idx, row in df.iterrows():
        date  = idx.strftime("%Y-%m-%d") if hasattr(idx, "strftime") else str(idx)[:10]
        price = float(row["Close"])
        if price and price == price:  # skip NaN
            points.append({"date": date, "price": price})
    return points


# ── Routes ────────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template("dashboard.html", repo=GITHUB_REPO)


@app.route("/login", methods=["GET", "POST"])
def login():
    error = None
    if request.method == "POST":
        pw = request.form.get("password", "")
        if DASHBOARD_PASSWORD and secrets.compare_digest(pw, DASHBOARD_PASSWORD):
            session["authenticated"] = True
            session.permanent = True
            dest = request.args.get("next") or url_for("index")
            # Guard against open-redirect: only allow relative paths
            if not dest.startswith("/") or dest.startswith("//"):
                dest = url_for("index")
            return redirect(dest)
        error = "Incorrect password."
    return render_template("login.html", error=error)


@app.route("/logout", methods=["POST"])
def logout():
    session.clear()
    return redirect(url_for("login"))


@app.route("/api/runs")
def api_runs():
    data = _gh_get(
        f"actions/workflows/{WORKFLOW_FILE}/runs",
        params={"per_page": 15},
    )
    runs = [
        {
            "id": r["id"],
            "run_number": r["run_number"],
            "status": r["status"],
            "conclusion": r["conclusion"],
            "created_at": r["created_at"],
            "updated_at": r["updated_at"],
            "html_url": r["html_url"],
            "head_commit_message": (r.get("head_commit") or {}).get("message", ""),
        }
        for r in data.get("workflow_runs", [])
    ]
    return jsonify(runs)


@app.route("/api/workflow_state")
def api_workflow_state():
    data = _gh_get(f"actions/workflows/{WORKFLOW_FILE}")
    return jsonify({"state": data.get("state", "unknown")})


@app.route("/api/logs/<int:run_id>")
def api_logs(run_id: int):
    artifacts = _gh_get(f"actions/runs/{run_id}/artifacts")
    for artifact in artifacts.get("artifacts", []):
        if "ems-log" in artifact["name"]:
            url = (
                f"https://api.github.com/repos/{GITHUB_REPO}"
                f"/actions/artifacts/{artifact['id']}/zip"
            )
            resp = requests.get(
                url, headers=_headers(), timeout=30, allow_redirects=True
            )
            resp.raise_for_status()
            with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
                for name in zf.namelist():
                    if name.endswith(".log"):
                        text = zf.read(name).decode("utf-8", errors="replace")
                        return jsonify({"log": text})
    return jsonify({"log": "No log artifact found for this run."})


@app.route("/api/disable", methods=["POST"])
def api_disable():
    _gh_put(f"actions/workflows/{WORKFLOW_FILE}/disable")
    return jsonify({"ok": True})


@app.route("/api/enable", methods=["POST"])
def api_enable():
    _gh_put(f"actions/workflows/{WORKFLOW_FILE}/enable")
    return jsonify({"ok": True})


@app.route("/api/trigger", methods=["POST"])
def api_trigger():
    _gh_post(
        f"actions/workflows/{WORKFLOW_FILE}/dispatches",
        {"ref": GITHUB_BRANCH},
    )
    return jsonify({"ok": True})


@app.route("/api/config")
def api_config_get():
    """Return which credentials are currently configured (values are never exposed)."""
    return jsonify({
        "PUBLIC_SECRET_KEY_set": bool(os.getenv("PUBLIC_SECRET_KEY", "")),
        "GITHUB_PAT_set":        bool(os.getenv("GITHUB_PAT", "")),
        "GITHUB_REPO":           os.getenv("GITHUB_REPO", GITHUB_REPO),
    })


@app.route("/api/config", methods=["POST"])
def api_config_post():
    """Persist credentials to .env and reload them into the running process."""
    global GITHUB_PAT, GITHUB_REPO
    data = request.get_json(force=True) or {}
    updated = []
    for key in ("PUBLIC_SECRET_KEY", "GITHUB_PAT", "GITHUB_REPO"):
        val = str(data.get(key, "")).strip()
        if val:
            set_key(DOTENV_PATH, key, val)   # writes/updates .env
            os.environ[key] = val            # hot-reload into this process
            updated.append(key)
    if "GITHUB_PAT" in updated:
        GITHUB_PAT = os.environ["GITHUB_PAT"]
    if "GITHUB_REPO" in updated:
        GITHUB_REPO = os.environ["GITHUB_REPO"]
    return jsonify({"ok": True, "updated": updated})


@app.route("/api/targets")
def api_targets_get():
    """Return current targets.json — reads from GitHub repo (authoritative source)."""
    raw, _ = _gh_read_repo_file("targets.json")
    if raw is not None:
        try:
            data = json.loads(raw)
            return jsonify({k: v for k, v in data.items() if not k.startswith("_")})
        except json.JSONDecodeError:
            pass
    # Local fallback (pure-local usage without GitHub API)
    targets_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "targets.json")
    try:
        with open(targets_path, encoding="utf-8") as f:
            data = json.load(f)
            return jsonify({k: v for k, v in data.items() if not k.startswith("_")})
    except FileNotFoundError:
        return jsonify({}), 200


@app.route("/api/targets", methods=["POST"])
def api_targets_post():
    """Validate and atomically save a new targets.json."""
    targets_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "targets.json")
    data = request.get_json(force=True) or {}
    raw: dict = data.get("targets", {})

    # Normalise and validate every entry
    clean: dict = {}
    for raw_ticker, weight in raw.items():
        ticker = str(raw_ticker).upper().strip()
        if not TICKER_RE.match(ticker):
            return jsonify({"error": f"Invalid ticker \u2018{raw_ticker}\u2019 (max 10 chars, A-Z 0-9 . -)"}), 400
        if not isinstance(weight, (int, float)) or weight < 0:
            return jsonify({"error": f"{ticker}: weight must be a non-negative number."}), 400
        if float(weight) > 0:
            clean[ticker] = round(float(weight), 6)

    total = sum(clean.values())
    if total > 0.99 + 1e-9:
        return jsonify({
            "error": f"Weights sum to {total * 100:.2f}% \u2014 must be \u226499% (1% cash buffer)."
        }), 400

    if not clean:
        return jsonify({"error": "No non-zero targets provided."}), 400

    content = json.dumps(clean, indent=2)
    # Commit to GitHub repo (persists on hosted platforms + keeps version history)
    _, sha = _gh_read_repo_file("targets.json")
    if _gh_write_repo_file("targets.json", content, sha, "[dashboard] update target weights"):
        return jsonify({"ok": True, "saved": clean})
    # Fall back to local file
    targets_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "targets.json")
    tmp_path = targets_path + ".tmp"
    try:
        with open(tmp_path, "w", encoding="utf-8") as f:
            f.write(content)
        os.replace(tmp_path, targets_path)
    except OSError as e:
        return jsonify({"error": f"Failed to save: {e}"}), 500

    return jsonify({"ok": True, "saved": clean})


@app.route("/api/portfolio")
def api_portfolio():
    """Fetch live portfolio positions from Public.com and compare against targets.json."""
    secret = os.getenv("PUBLIC_SECRET_KEY", "")
    if not secret:
        return jsonify({"error": "PUBLIC_SECRET_KEY not set in .env"}), 503

    try:
        # Step 1: Authenticate
        auth_resp = requests.post(
            "https://api.public.com/userapiauthservice/personal/access-tokens",
            json={"validityInMinutes": 60, "secret": secret},
            timeout=15,
        )
        auth_resp.raise_for_status()
        token = auth_resp.json()["accessToken"]
        api_hdrs = {"Authorization": f"Bearer {token}"}

        # Step 2: Discover brokerage account
        acc_resp = requests.get(
            "https://api.public.com/userapigateway/trading/account",
            headers=api_hdrs,
            timeout=15,
        )
        acc_resp.raise_for_status()
        accounts = acc_resp.json().get("accounts", [])
        if not accounts:
            return jsonify({"error": "No accounts found"}), 502
        account_id = next(
            (a["accountId"] for a in accounts if a.get("accountType") == "BROKERAGE"),
            accounts[0]["accountId"],
        )

        # Step 3: Fetch portfolio snapshot
        port_resp = requests.get(
            f"https://api.public.com/userapigateway/trading/{account_id}/portfolio/v2",
            headers=api_hdrs,
            timeout=20,
        )
        port_resp.raise_for_status()
        portfolio = port_resp.json()

        cash = float(portfolio.get("buyingPower", {}).get("cashOnlyBuyingPower", 0))
        holdings: dict = {}
        for pos in portfolio.get("positions", []):
            ticker = pos["instrument"]["symbol"].upper()
            holdings[ticker] = float(pos["currentValue"])

        total = sum(holdings.values()) + cash

        # Step 4: Load targets from GitHub repo (authoritative source)
        raw_targets, _ = _gh_read_repo_file("targets.json")
        if raw_targets is not None:
            try:
                all_targets = json.loads(raw_targets)
                targets = {k: v for k, v in all_targets.items() if not k.startswith("_")}
            except json.JSONDecodeError:
                targets = {}
        else:
            targets_path = os.path.join(
                os.path.dirname(os.path.abspath(__file__)), "targets.json"
            )
            with open(targets_path, encoding="utf-8") as f:
                all_targets = json.load(f)
                targets = {k: v for k, v in all_targets.items() if not k.startswith("_")}

        # Step 5: Build comparison rows (union of held and targeted tickers)
        rows = []
        for ticker in sorted(set(holdings) | set(targets)):
            cur_val = holdings.get(ticker, 0.0)
            cur_wt  = (cur_val / total * 100) if total else 0.0
            tgt_wt  = targets.get(ticker, 0.0) * 100
            rows.append({
                "ticker":         ticker,
                "value":          round(cur_val, 2),
                "current_weight": round(cur_wt, 2),
                "target_weight":  round(tgt_wt, 2),
                "drift":          round(cur_wt - tgt_wt, 2),
            })

        rows.sort(key=lambda x: (-x["target_weight"], x["ticker"]))

        # Record daily portfolio value snapshot for performance tracking
        _append_history(datetime.now().strftime("%Y-%m-%d"), total)

        return jsonify({
            "total_value": round(total, 2),
            "cash":        round(cash, 2),
            "cash_weight": round(cash / total * 100, 2) if total else 0.0,
            "rows":        rows,
        })

    except requests.HTTPError as e:
        code = e.response.status_code if e.response is not None else 0
        return jsonify({"error": f"Public API HTTP {code}: {e}"}), 502
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/performance")
def api_performance():
    """Return normalized portfolio vs SPY/QQQ performance series for charting."""
    history      = _load_history()
    sorted_dates = sorted(history.keys())
    if len(sorted_dates) < 2:
        return jsonify({"status": "insufficient_data", "days": len(sorted_dates)})

    start_date = sorted_dates[0]
    start_dt   = datetime.strptime(start_date, "%Y-%m-%d")
    # Pad back 2 days so yfinance is sure to include data on start_date
    fetch_from = start_dt - timedelta(days=2)
    fetch_to   = datetime.now()

    try:
        spy_raw = _yahoo_adj_close("SPY", fetch_from, fetch_to)
        qqq_raw = _yahoo_adj_close("QQQ", fetch_from, fetch_to)
    except Exception as e:
        return jsonify({"error": f"Benchmark fetch failed: {e}"}), 502

    port_start       = history[sorted_dates[0]]
    portfolio_series = [
        {"date": d, "value": round(history[d] / port_start * 100, 4)}
        for d in sorted_dates
    ]

    def normalize_series(raw, anchor_date):
        anchor = next((pt["price"] for pt in raw if pt["date"] >= anchor_date), None)
        if not anchor:
            return []
        return [
            {"date": pt["date"], "value": round(pt["price"] / anchor * 100, 4)}
            for pt in raw if pt["date"] >= anchor_date
        ]

    return jsonify({
        "start_date": start_date,
        "portfolio":  portfolio_series,
        "spy":        normalize_series(spy_raw, start_date),
        "qqq":        normalize_series(qqq_raw, start_date),
    })


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    if not GITHUB_PAT:
        print(
            "\nERROR: GITHUB_PAT not found in environment.\n"
            "Add it to your .env file:\n"
            "  GITHUB_PAT=ghp_your_token_here\n"
            "  GITHUB_REPO=Loudlarry/trade-manager\n\n"
            "Create a PAT at: https://github.com/settings/tokens/new\n"
            "Required scopes: repo  (or: actions:read + actions:write)\n"
        )
        raise SystemExit(1)
    port = int(os.getenv("PORT", 5000))
    print(f"\nEMS Dashboard \u2192 http://0.0.0.0:{port}   (repo: {GITHUB_REPO})")
    if DASHBOARD_PASSWORD:
        print("Auth: DASHBOARD_PASSWORD is set \u2014 login required.")
    else:
        print("WARNING: DASHBOARD_PASSWORD not set \u2014 add it to .env before exposing to a network.")
    print()
    app.run(debug=False, host="0.0.0.0", port=port)
