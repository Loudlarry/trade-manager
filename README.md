# Trade Manager — EMS (Execution Management System)

An automated Python rebalancing engine for Public.com portfolios.

---

## Project Structure

```
Trade Manager/
├── ems.py            ← Main script
├── targets.json      ← Your desired portfolio weights
├── .env              ← API credentials (never commit this)
├── .env.example      ← Template for .env
├── requirements.txt  ← Python dependencies
└── ems.log           ← Auto-created on first run
```

---

## Quick Start

### 1. Install dependencies
```bash
pip install -r requirements.txt
```

### 2. Configure credentials
```bash
copy .env.example .env
# Open .env and paste your PUBLIC_SECRET_KEY
# Get it from: https://public.com/settings/security/api
```

### 3. Set your target weights
Edit `targets.json`. Weights must sum to ≤ 1.0.
```json
{
  "NVDA": 0.15,
  "AAPL": 0.10
}
```

### 4. Run in Dry-Run mode (safe, no real orders)
In `ems.py`, ensure `DRY_RUN = True`, then:
```bash
python ems.py
```

### 5. Go live
Set `DRY_RUN = False` in `ems.py` and re-run.

---

## Key Configuration (top of ems.py)

| Variable | Default | Description |
|---|---|---|
| `DRY_RUN` | `True` | Simulate orders without executing |
| `CASH_BUFFER_PCT` | `5%` (0.05) | % of total account value kept uninvested |
| `DRIFT_THRESHOLD` | `5%` | Min drift to trigger rebalance |
| `MIN_ORDER_DOLLARS` | `$1.00` | Min trade size |
| `TOKEN_VALIDITY_MINUTES` | `60` | Access token lifetime in minutes |

---

## Business Logic

1. **Cash Buffer**: `investable_value = total_account_value - $25.00`
2. **Drift Gate**: Only trade if `|current_weight - target_weight| > 5%`
3. **Order Sequencing**: Sells are executed before buys to free cash first

---

## Cron Job (Daily at 9:35 AM ET, weekdays)
```cron
35 9 * * 1-5 /usr/bin/python3 /path/to/ems.py >> /var/log/ems.log 2>&1
```

---

## Authentication

Public.com uses a **two-step auth flow**:

1. POST your **Secret Key** to get a short-lived **Access Token**
   ```
   POST https://api.public.com/userapiauthservice/personal/access-tokens
   { "validityInMinutes": 60, "secret": "YOUR_SECRET_KEY" }
   → { "accessToken": "..." }
   ```
2. Every subsequent request uses `Authorization: Bearer <accessToken>`

Generate your Secret Key at: **Account Settings → Security → API**

---

## Real API Endpoints Used

| Purpose | Method | Endpoint |
|---|---|---|
| Auth | POST | `https://api.public.com/userapiauthservice/personal/access-tokens` |
| Get accounts | GET | `https://api.public.com/userapigateway/trading/account` |
| Get portfolio | GET | `https://api.public.com/userapigateway/trading/{accountId}/portfolio/v2` |
| Place order | POST | `https://api.public.com/userapigateway/trading/{accountId}/order` |

---

