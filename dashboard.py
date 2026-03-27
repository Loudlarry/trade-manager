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
  3. Install deps:  pip install -r requirements-dashboard.txt
  4. Run:           python dashboard.py
  5. Open:          http://localhost:5000

Security: The server binds to 127.0.0.1 (localhost only).
Your PAT is never sent to the browser — all GitHub API calls are
made server-side here in Python.
"""

import io
import json
import os
import re
import zipfile

import requests
from dotenv import load_dotenv, set_key
from flask import Flask, jsonify, render_template, request

load_dotenv()

GITHUB_PAT: str = os.getenv("GITHUB_PAT", "")
GITHUB_REPO: str = os.getenv("GITHUB_REPO", "Loudlarry/trade-manager")
WORKFLOW_FILE: str = "daily-rebalance.yml"
GITHUB_BRANCH: str = "master"
DOTENV_PATH: str   = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
TICKER_RE          = re.compile(r"^[A-Z0-9.\-]{1,10}$")

app = Flask(__name__)


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


# ── Routes ────────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template("dashboard.html", repo=GITHUB_REPO)


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
    """Return current targets.json as a plain dict."""
    targets_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "targets.json")
    try:
        with open(targets_path, encoding="utf-8") as f:
            return jsonify(json.load(f))
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

    # Atomic write: .tmp then replace
    tmp_path = targets_path + ".tmp"
    try:
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(clean, f, indent=2)
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

        # Step 4: Load targets
        targets_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "targets.json"
        )
        with open(targets_path, encoding="utf-8") as f:
            targets = json.load(f)

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

        return jsonify({
            "total_value": round(total, 2),
            "cash":        round(cash, 2),
            "cash_weight": round(cash / total * 100, 2) if total else 0.0,
            "rows":        rows,
        })

    except requests.HTTPError as e:
        code = e.response.status_code if e.response is not None else 0
        return jsonify({"error": f"Public API HTTP {code}: {e}"}), 502
    except FileNotFoundError:
        return jsonify({"error": "targets.json not found"}), 500
    except Exception as e:
        return jsonify({"error": str(e)}), 500


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
    print(f"\nEMS Dashboard → http://localhost:5000   (repo: {GITHUB_REPO})\n")
    app.run(debug=False, host="127.0.0.1", port=5000)
