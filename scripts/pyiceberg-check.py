"""Read a table written by the pipeline's iceberg output with PyIceberg."""
import sys
from pyiceberg.catalog.sql import SqlCatalog

wh = sys.argv[1]
cat = SqlCatalog("telemetry", uri=f"sqlite:///{wh}/cat.db", warehouse=f"file://{wh}/wh")
t = cat.load_table("net.raw.telemetry")
print("PyIceberg loaded:", ".".join(t.name()), "| format v", t.metadata.format_version)
print("partition spec:", t.spec())
print("schema:", ", ".join(f"{f.name}:{f.field_type}" for f in t.schema().fields))
snap = t.current_snapshot()
print("snapshot:", snap.summary.operation.value, "| batch-id:", snap.summary.additional_properties.get("telemetry.batch-id", "")[:16], "...")
tbl = t.scan().to_arrow()
rows = tbl.to_pylist()
print("rows:", tbl.num_rows, "| sources:", sorted({r["source"] for r in rows}))
huge = next(r for r in rows if r["field"] == "state/counters/huge")
print("uint64 > MaxInt64 -> value_int:", huge["value_int"], "value_string:", huge["value_string"])
print("unicode tag:", dict(huge["tags"])["site"])
hr = t.scan(row_filter="timestamp >= '2026-11-24T10:00:00+00:00'").to_arrow()
print("filtered scan (>= 10:00):", hr.num_rows, "rows from", sorted(set(hr.column("source").to_pylist())))
# golden batch: core1 at 09:59:59, edge-2 at 10:00:00, dr02 at 10:00:01
assert tbl.num_rows == 19, tbl.num_rows
assert hr.num_rows == 11 and set(hr.column("source").to_pylist()) == {"edge-2", "dr02"}, hr.num_rows
print("OK")
