from datetime import datetime, timezone

ms_now = round(datetime.now(timezone.utc).timestamp() * 1e3)
print(ms_now)