# Schema Drift Report

_LLM provider: `mock`_

### orders: new column `promo_code`
- **Files (7):** `orders_2026-09-29.csv`, `orders_2026-09-30.csv`, `orders_2026-10-01.csv` ... - **ingested**; the column is kept in bronze (schema evolution) but not used downstream yet.
- **Assessment:** It does not match any known column, so it is probably a new attribute.
- **Action:** if it is needed, add `promo_code` to the `orders` contract (optional) and select it in the silver cleaner; otherwise acknowledge and ignore.

### orders: missing required column(s) unit_price
- **Files (1):** `orders_2026-10-06_partner.csv` - **rejected**, nothing from them was loaded.
- **Likely cause:** a different export (partner feed or a changed extract job).
- **Action:** ask the source team to send the column, or agree a default and add a mapping before re-landing the file. The rest of the batch was unaffected.
