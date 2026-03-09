import pandas as pd
import duckdb

HOLIDAYS = ['2026-02-16']

conn = duckdb.connect()

df = conn.execute("""
    SELECT ts_event, price, size, side
    FROM '/Volumes/Disk-ito/Ari/Saved Projects/Coding/mnq-analyzer/NQ_Trades_Data_Conv/02_26.parquet'
    WHERE EXTRACT(hour FROM ts_event) >= 13
      AND EXTRACT(hour FROM ts_event) <= 16
      AND symbol NOT LIKE '%-%%'
""").df()

df['ts_event'] = pd.to_datetime(df['ts_event'], utc=True)
df['ts_ny'] = df['ts_event'].dt.tz_convert('America/New_York')
df = df.set_index('ts_ny')
df['date'] = df.index.date

results = []

for date, day_df in df.groupby('date'):
    if str(date) in HOLIDAYS:
        print(f"⚠️ Skipping {date} — holiday")
        continue

    try:
        or_ticks = day_df.between_time('09:30', '09:44').copy()
        if len(or_ticks) < 100:
            print(f"Skipping {date} — not enough ticks")
            continue

        or_high = or_ticks['price'].max()
        or_low = or_ticks['price'].min()

        or_ticks['price_rounded'] = (or_ticks['price'] / 0.25).round() * 0.25

        vp_total = or_ticks.groupby('price_rounded')['size'].sum()
        vp_buy = or_ticks[or_ticks['side'] == 'A'].groupby('price_rounded')['size'].sum()
        vp_sell = or_ticks[or_ticks['side'] == 'B'].groupby('price_rounded')['size'].sum()
        vp_delta = vp_buy.subtract(vp_sell, fill_value=0)

        total_buy = int(or_ticks[or_ticks['side'] == 'A']['size'].sum())
        total_sell = int(or_ticks[or_ticks['side'] == 'B']['size'].sum())

        results.append({
            'date': str(date),
            'or_high': or_high,
            'or_low': or_low,
            'or_range': or_high - or_low,
            'total_volume': int(or_ticks['size'].sum()),
            'total_buy': total_buy,
            'total_sell': total_sell,
            'total_delta': total_buy - total_sell,
            'vp_total': vp_total.to_json(),
            'vp_delta': vp_delta.to_json(),
        })

        print(f"✅ {date} | OR: {or_low}-{or_high} | Volume: {int(or_ticks['size'].sum())}")

    except Exception as e:
        print(f"❌ {date} error: {e}")
        continue

output = pd.DataFrame(results)
output.to_parquet('/Volumes/Disk-ito/Ari/Saved Projects/Coding/ORB_Backtester/or_volume_02_26.parquet', index=False)
print(f"\nSaved {len(output)} days to or_volume_02_26.parquet ✅")