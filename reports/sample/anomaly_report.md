# Revenue Anomaly Report

_Method: robust z-score (median/MAD), 14-day trailing window, weekday-adjusted=True, |z| > 3.5. LLM provider: `mock`_

### 2026-07-16 - revenue spike (+274%) - severity: high
Revenue was **341,931** vs an expected **91,412** (robust z-score +30.7).
The change is spread evenly across all categories, which points to a platform-wide event rather than a single product.
**Likely causes:** a sale/campaign, a bulk B2B order, or duplicate ingestion. **Check next:** campaign calendar, top customers that day, duplicate order ids in bronze.

### 2026-08-02 - revenue spike (+24%) - severity: medium
Revenue was **131,041** vs an expected **106,081** (robust z-score +4.2).
The change is spread evenly across all categories, which points to a platform-wide event rather than a single product.
**Likely causes:** a sale/campaign, a bulk B2B order, or duplicate ingestion. **Check next:** campaign calendar, top customers that day, duplicate order ids in bronze.
_Borderline signal: likely normal variance from high-value orders; monitor rather than escalate._

### 2026-08-28 - revenue drop (-87%) - severity: high
Revenue was **12,942** vs an expected **101,726** (robust z-score -7.8).
The change is spread evenly across all categories, which points to a platform-wide event rather than a single product.
**Likely causes:** checkout or payment outage, a missing source file, or a late batch. **Check next:** file count in the landing zone for that day, payment gateway status, and quarantine volume for the day.

### 2026-09-13 - revenue spike (+39%) - severity: medium
Revenue was **164,143** vs an expected **117,753** (robust z-score +4.7).
The change is spread evenly across all categories, which points to a platform-wide event rather than a single product.
**Likely causes:** a sale/campaign, a bulk B2B order, or duplicate ingestion. **Check next:** campaign calendar, top customers that day, duplicate order ids in bronze.
_Borderline signal: likely normal variance from high-value orders; monitor rather than escalate._
