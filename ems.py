"""
ems.py — Custom Execution Management System for Public.com
=============================================================
Reads target portfolio weights from targets.json, compares them to live
holdings via the Public.com Trading API, and executes fractional
rebalancing trades based on drift and cash-buffer rules.

Public.com API reference: https://public.com/api/docs

Authentication flow (two-step):
  1. POST secret key to /userapiauthservice → receive short-lived access token
  2. Use access token as Bearer on all gateway requests

Run modes:
  • DRY_RUN = True  → logs calculated orders, skips real API calls
  • DRY_RUN = False → executes live market orders

Cron example (daily at 09:35 ET after market open):
  35 9 * * 1-5 /usr/bin/python3 /path/to/ems.py >> /var/log/ems.log 2>&1
"""

import json
import logging
import os
import sys
import uuid
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional

import requests
from dotenv import load_dotenv

# Load .env early so EMS_DRY_RUN and other vars are available at module level.
load_dotenv()

# ─────────────────────────── Configuration ───────────────────────────────────

# Toggle to True to simulate without sending real orders.
# Set via env var so you never need to edit code between test and live runs.
# In GitHub Actions: set the EMS_DRY_RUN secret/variable to "false" to go live.
# Locally: add EMS_DRY_RUN=false to your .env file when ready.
DRY_RUN: bool = os.getenv("EMS_DRY_RUN", "true").lower() != "false"

# Percentage of total account value kept uninvested at all times (covers
# fees / margin buffer). 0.05 = 5% of total account value.
CASH_BUFFER_PCT: float = 0.05

# Minimum absolute weight deviation (as a decimal) that triggers a rebalance.
# 0.05 = 5 percentage points.
DRIFT_THRESHOLD: float = 0.05

# Minimum absolute dollar value per order (avoids noise trades).
MIN_ORDER_DOLLARS: float = 1.00

# How long the access token remains valid (minutes). Max varies by plan.
TOKEN_VALIDITY_MINUTES: int = 60

# Path to the targets file (relative to this script).
TARGETS_FILE: Path = Path(__file__).parent / "targets.json"

# ─────────────────────────── Logging Setup ───────────────────────────────────

LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(message)s"
logging.basicConfig(
    level=logging.INFO,
    format=LOG_FORMAT,
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(
            Path(__file__).parent / "ems.log",
            mode="a",
            encoding="utf-8",
        ),
    ],
)
log = logging.getLogger("EMS")

# ─────────────────────────── Data Classes ────────────────────────────────────


@dataclass
class AccountState:
    """Snapshot of the account at the time the script runs."""
    account_id: str             # Public.com internal account UUID
    total_value: float          # Full portfolio value (cash + positions)
    cash_balance: float         # Uninvested cash (cashOnlyBuyingPower)
    holdings: dict[str, float]  # {ticker: current_dollar_value}


@dataclass
class Order:
    """A single pending trade instruction."""
    ticker: str
    side: str           # "BUY" or "SELL"  (matches API enum)
    dollar_amount: float
    target_weight: float
    current_weight: float
    drift: float        # current_weight − target_weight (signed)


# ─────────────────────────── Public.com API Client ───────────────────────────

class PublicAPIClient:
    """
    Wrapper around the Public.com REST Trading API.

    Auth service:    https://api.public.com/userapiauthservice
    Trading gateway: https://api.public.com/userapigateway/trading

    Authentication is a two-step process:
      Step 1 – POST secret → short-lived access token
      Step 2 – Bearer token on every gateway request

    Full docs: https://public.com/api/docs
    """

    AUTH_URL = "https://api.public.com/userapiauthservice/personal/access-tokens"
    GATEWAY_BASE = "https://api.public.com/userapigateway/trading"

    def __init__(self, secret_key: str) -> None:
        self._secret_key = secret_key
        self._access_token: Optional[str] = None
        self._session = requests.Session()
        self._session.headers.update(
            {
                "Content-Type": "application/json",
                "Accept": "application/json",
                # Required by the API to identify the consumer.
                "User-Agent": "public-ems-bot/1.0",
            }
        )

    # ── Authentication ────────────────────────────────────────────────────

    def authenticate(self) -> None:
        """
        POST /userapiauthservice/personal/access-tokens
        Exchange the secret key for a short-lived Bearer access token.

        Request body:
          { "validityInMinutes": 60, "secret": "YOUR_SECRET_KEY" }

        Response:
          { "accessToken": "YOUR_ACCESS_TOKEN" }
        """
        log.info("Authenticating with Public.com API …")
        payload = {
            "validityInMinutes": TOKEN_VALIDITY_MINUTES,
            "secret": self._secret_key,
        }
        resp = self._session.post(self.AUTH_URL, json=payload, timeout=15)
        resp.raise_for_status()
        self._access_token = resp.json()["accessToken"]
        self._session.headers["Authorization"] = f"Bearer {self._access_token}"
        log.info("Authentication successful.")

    def _require_auth(self) -> None:
        if not self._access_token:
            raise RuntimeError(
                "Client is not authenticated. Call authenticate() first."
            )

    # ── Low-level helpers ─────────────────────────────────────────────────

    def _get(self, path: str, params: Optional[dict] = None) -> dict:
        self._require_auth()
        url = f"{self.GATEWAY_BASE}/{path.lstrip('/')}"
        resp = self._session.get(url, params=params, timeout=15)
        resp.raise_for_status()
        return resp.json()

    def _post(self, path: str, payload: dict) -> dict:
        self._require_auth()
        url = f"{self.GATEWAY_BASE}/{path.lstrip('/')}"
        resp = self._session.post(url, json=payload, timeout=15)
        resp.raise_for_status()
        return resp.json()

    # ── Public API endpoints ──────────────────────────────────────────────

    def get_accounts(self) -> dict:
        """
        GET /userapigateway/trading/account
        Returns the list of accounts for the authenticated user.

        Response:
          {
            "accounts": [
              {
                "accountId": "string",
                "accountType": "BROKERAGE",
                "optionsLevel": "NONE",
                "brokerageAccountType": "CASH",
                "tradePermissions": "BUY_AND_SELL"
              }
            ]
          }
        """
        return self._get("/account")

    def get_portfolio(self, account_id: str) -> dict:
        """
        GET /userapigateway/trading/{accountId}/portfolio/v2
        Returns positions, equity breakdown, buying power, and open orders.

        Response (abbreviated):
          {
            "accountId": "string",
            "buyingPower": {
              "cashOnlyBuyingPower": "234.56",
              "buyingPower": "234.56",
              "optionsBuyingPower": "234.56"
            },
            "equity": [
              { "type": "CASH", "value": "234.56", "percentageOfPortfolio": "1.9" }
            ],
            "positions": [
              {
                "instrument": { "symbol": "NVDA", "type": "EQUITY" },
                "quantity": "10.5",
                "currentValue": "1500.00",
                ...
              }
            ],
            "orders": [ ... ]
          }
        """
        return self._get(f"/{account_id}/portfolio/v2")

    def place_order(
        self,
        account_id: str,
        ticker: str,
        side: str,
        dollar_amount: float,
    ) -> dict:
        """
        POST /userapigateway/trading/{accountId}/order
        Places a fractional, dollar-denominated market order.

        Key request fields:
          - orderId:    Client-generated UUID (deduplication key)
          - instrument: { "symbol": "NVDA", "type": "EQUITY" }
          - orderSide:  "BUY" | "SELL"
          - orderType:  "MARKET"
          - expiration: { "timeInForce": "DAY" }
          - amount:     "150.00"  ← dollar-based fractional field (NOT quantity)

        Response:
          { "orderId": "fceeb48e-5d9a-4151-9d06-5347bd820ee3" }

        Note: Placement is asynchronous. The returned orderId confirms
        submission only — poll GET /{accountId}/order/{orderId} for fill status.
        """
        payload = {
            # RFC 4122 UUID — serves as idempotency / deduplication key.
            "orderId": str(uuid.uuid4()),
            "instrument": {
                "symbol": ticker.upper(),
                "type": "EQUITY",
            },
            "orderSide": side.upper(),   # "BUY" or "SELL"
            "orderType": "MARKET",
            "expiration": {
                "timeInForce": "DAY",
            },
            # "amount" is the fractional dollar field for notional orders.
            # "quantity" would be used for whole-share orders instead.
            "amount": str(round(dollar_amount, 2)),
        }
        return self._post(f"/{account_id}/order", payload)


# ─────────────────────────── Core Functions ──────────────────────────────────


def load_targets() -> dict[str, float]:
    """
    Load desired portfolio weights from targets.json.

    File format:
        { "NVDA": 0.15, "AAPL": 0.10, "MSFT": 0.20, ... }

    Weights must sum to ≤ 1.0 (remaining fraction stays as cash).
    Raises on missing file, bad JSON, or weights that exceed 1.0.
    """
    log.info("Loading targets from %s", TARGETS_FILE)

    if not TARGETS_FILE.exists():
        raise FileNotFoundError(f"targets.json not found at {TARGETS_FILE}")

    with TARGETS_FILE.open("r", encoding="utf-8") as fh:
        raw = json.load(fh)

    # Strip internal comment keys (keys starting with "_")
    data = {k: v for k, v in raw.items() if not k.startswith("_")}

    if not isinstance(data, dict):
        raise ValueError("targets.json must be a JSON object {ticker: weight, ...}")

    # Normalise keys to uppercase
    targets: dict[str, float] = {k.upper(): float(v) for k, v in data.items()}

    total = sum(targets.values())
    if total > 1.0 + 1e-9:
        raise ValueError(
            f"Target weights sum to {total:.4f}, which exceeds 1.0. "
            "Please review targets.json."
        )

    log.info(
        "Loaded %d target(s) | total allocation: %.2f%%",
        len(targets),
        total * 100,
    )
    for ticker, w in targets.items():
        log.info("  %-8s → target weight %.2f%%", ticker, w * 100)

    return targets


def get_account_state(client: PublicAPIClient) -> AccountState:
    """
    Discover the brokerage account ID, then fetch live portfolio data.

    Two API calls:
      1. GET /account            → find the BROKERAGE accountId
      2. GET /{id}/portfolio/v2  → positions + buying power

    Total portfolio value is calculated as:
        sum(position["currentValue"]) + cashOnlyBuyingPower
    """
    log.info("Fetching account state from Public.com API …")

    # ── Step 1: Discover accountId ────────────────────────────────────────
    accounts_resp = client.get_accounts()
    accounts: list[dict] = accounts_resp.get("accounts", [])

    if not accounts:
        raise RuntimeError("No accounts found for this API key.")

    # Prefer the first BROKERAGE account; fall back to the first account.
    account = next(
        (a for a in accounts if a.get("accountType") == "BROKERAGE"),
        accounts[0],
    )
    account_id: str = account["accountId"]
    log.info("Using account: %s (type=%s)", account_id, account.get("accountType"))

    # ── Step 2: Fetch portfolio snapshot ─────────────────────────────────
    portfolio = client.get_portfolio(account_id)

    # Cash balance → use cashOnlyBuyingPower from buyingPower object
    buying_power: dict = portfolio.get("buyingPower", {})
    cash_balance = float(buying_power.get("cashOnlyBuyingPower", 0))
    log.info("  Cash (buyingPower): $%.2f", cash_balance)

    # Positions → build holdings dict
    positions: list[dict] = portfolio.get("positions", [])
    holdings: dict[str, float] = {}
    invested_value = 0.0

    for pos in positions:
        ticker = pos["instrument"]["symbol"].upper()
        current_value = float(pos["currentValue"])
        holdings[ticker] = current_value
        invested_value += current_value
        log.info("  Holding %-8s : $%.2f", ticker, current_value)

    if not holdings:
        log.warning("No open positions found in portfolio.")

    # Total AUM = all invested positions + uninvested cash
    total_value = invested_value + cash_balance
    log.info(
        "  Total value: $%.2f  (invested $%.2f + cash $%.2f)",
        total_value,
        invested_value,
        cash_balance,
    )

    return AccountState(
        account_id=account_id,
        total_value=total_value,
        cash_balance=cash_balance,
        holdings=holdings,
    )


def calculate_orders(
    targets: dict[str, float],
    state: AccountState,
) -> list[Order]:
    """
    Core rebalancing logic.

    Steps:
      1. Reserve CASH_BUFFER_PCT % of total value as uninvested buffer.
      2. Compute current weight for every position.
      3. Flag positions whose |drift| > DRIFT_THRESHOLD.
         Exception: positions removed from targets.json (target = 0%) are
         always fully sold regardless of size — treated as intentional exits.
      4. Generate sell orders first (to free cash), then buy orders.
      5. Skip any order whose absolute dollar amount < MIN_ORDER_DOLLARS.

    Returns a list of Order objects sorted: sells before buys.
    """
    cash_buffer = state.total_value * CASH_BUFFER_PCT
    investable_value = state.total_value - cash_buffer

    if investable_value <= 0:
        log.error(
            "Investable value ($%.2f) is ≤ $0 after applying %.0f%% cash buffer ($%.2f). "
            "No orders generated.",
            investable_value,
            CASH_BUFFER_PCT * 100,
            cash_buffer,
        )
        return []

    log.info(
        "Investable value: $%.2f  (account $%.2f − %.0f%% buffer $%.2f)",
        investable_value,
        state.total_value,
        CASH_BUFFER_PCT * 100,
        cash_buffer,
    )

    # All tickers that appear in either targets or current holdings
    all_tickers = set(targets.keys()) | set(state.holdings.keys())

    sells: list[Order] = []
    buys: list[Order] = []

    for ticker in sorted(all_tickers):
        target_weight = targets.get(ticker, 0.0)
        current_value = state.holdings.get(ticker, 0.0)
        current_weight = current_value / state.total_value  # weight on full AUM

        drift = current_weight - target_weight  # positive = overweight

        log.debug(
            "%-8s  target=%.2f%%  current=%.2f%%  drift=%+.2f%%",
            ticker,
            target_weight * 100,
            current_weight * 100,
            drift * 100,
        )

        # ── Drift gate ────────────────────────────────────────────────────
        # Exception: if a ticker has been removed from targets.json entirely
        # (target_weight == 0) and we still hold it, always sell regardless of
        # position size — this is an intentional full exit, not drift noise.
        is_full_exit = target_weight == 0.0 and current_value > 0.0

        if not is_full_exit and abs(drift) <= DRIFT_THRESHOLD:
            log.info(
                "%-8s  SKIP  |drift| %.2f%% ≤ threshold %.2f%%",
                ticker,
                abs(drift) * 100,
                DRIFT_THRESHOLD * 100,
            )
            continue

        # ── Dollar delta ─────────────────────────────────────────────────
        target_value = target_weight * investable_value
        delta = target_value - current_value  # negative = must sell

        if abs(delta) < MIN_ORDER_DOLLARS:
            log.info(
                "%-8s  SKIP  |$delta| $%.2f < min order $%.2f",
                ticker,
                abs(delta),
                MIN_ORDER_DOLLARS,
            )
            continue

        order = Order(
            ticker=ticker,
            side="SELL" if delta < 0 else "BUY",
            dollar_amount=abs(delta),
            target_weight=target_weight,
            current_weight=current_weight,
            drift=drift,
        )

        if order.side == "SELL":
            sells.append(order)
        else:
            buys.append(order)

        log.info(
            "%-8s  %-4s  $%.2f  |  target %.2f%%  current %.2f%%  drift %+.2f%%",
            ticker,
            order.side,
            order.dollar_amount,
            target_weight * 100,
            current_weight * 100,
            drift * 100,
        )

    # Sells before buys → ensure cash is available before purchasing
    orders = sells + buys

    log.info(
        "Order summary: %d sell(s) totalling $%.2f | %d buy(s) totalling $%.2f",
        len(sells),
        sum(o.dollar_amount for o in sells),
        len(buys),
        sum(o.dollar_amount for o in buys),
    )

    return orders


def execute_trades(
    orders: list[Order],
    client: PublicAPIClient,
    account_id: str,
) -> None:
    """
    Loop through the order list and POST each one to the Public.com API.

    Sells are processed before buys (guaranteed by calculate_orders).
    Each response is logged; failures are logged as errors and skipped
    (non-fatal) so the remaining orders can still be attempted.

    Note: The API returns submission confirmation only (async execution).
    The returned orderId can be polled via GET /{accountId}/order/{orderId}
    to check fill status.
    """
    if not orders:
        log.info("No orders to execute.")
        return

    log.info("Executing %d order(s) …", len(orders))

    for i, order in enumerate(orders, start=1):
        log.info(
            "[%d/%d] %s %s $%.2f",
            i,
            len(orders),
            order.side.upper(),
            order.ticker,
            order.dollar_amount,
        )
        try:
            response = client.place_order(
                account_id=account_id,
                ticker=order.ticker,
                side=order.side,
                dollar_amount=order.dollar_amount,
            )
            # Response: { "orderId": "fceeb48e-..." }  (submission confirmed only)
            returned_order_id = response.get("orderId", "N/A")
            log.info("  ✓ submitted orderId=%s", returned_order_id)
        except requests.HTTPError as exc:
            log.error(
                "  ✗ HTTP %s placing %s %s: %s",
                exc.response.status_code if exc.response is not None else "?",
                order.side,
                order.ticker,
                exc,
            )
        except requests.RequestException as exc:
            log.error(
                "  ✗ Network error placing %s %s: %s",
                order.side,
                order.ticker,
                exc,
            )


# ─────────────────────────── Orchestrator ────────────────────────────────────


def run() -> None:
    """
    Main orchestration function.

    Pipeline:
      load .env
      → authenticate (secret → access token)
      → load targets.json
      → fetch account state (accountId, positions, cash)
      → calculate drift-gated rebalance orders
      → execute orders (or log them if DRY_RUN)
    """
    start_time = datetime.now()
    log.info("=" * 60)
    log.info("EMS run started at %s", start_time.strftime("%Y-%m-%d %H:%M:%S"))
    log.info("DRY_RUN=%s | CASH_BUFFER=%.0f%% | DRIFT_THRESHOLD=%.0f%%",
             DRY_RUN, CASH_BUFFER_PCT * 100, DRIFT_THRESHOLD * 100)
    log.info("=" * 60)

    # ── Environment ───────────────────────────────────────────────────────
    secret_key = os.getenv("PUBLIC_SECRET_KEY")

    if not secret_key:
        log.critical(
            "PUBLIC_SECRET_KEY not found in environment. "
            "Create a .env file or set the variable and retry."
        )
        sys.exit(1)

    client = PublicAPIClient(secret_key=secret_key)

    # ── Pipeline ──────────────────────────────────────────────────────────
    try:
        # Step 1: Exchange secret for access token
        client.authenticate()

        # Step 2: Load target weights
        targets = load_targets()

        # Step 3: Fetch live account state (discovers accountId internally)
        state = get_account_state(client)

        # Step 4: Calculate drift-gated orders
        orders = calculate_orders(targets, state)

        # Step 5: Execute or dry-run
        if DRY_RUN:
            log.info("─── DRY RUN — no real orders will be sent ───")
            for order in orders:
                log.info(
                    "  [DRY] %s %s $%.2f",
                    order.side,
                    order.ticker,
                    order.dollar_amount,
                )
        else:
            execute_trades(orders, client, account_id=state.account_id)

    except FileNotFoundError as exc:
        log.critical("Configuration error: %s", exc)
        sys.exit(1)
    except ValueError as exc:
        log.critical("Validation error: %s", exc)
        sys.exit(1)
    except requests.RequestException as exc:
        log.critical("API connectivity error: %s", exc)
        sys.exit(1)

    elapsed = (datetime.now() - start_time).total_seconds()
    log.info("EMS run completed in %.2fs", elapsed)
    log.info("=" * 60)


# ─────────────────────────── Entry Point ─────────────────────────────────────

if __name__ == "__main__":
    run()
