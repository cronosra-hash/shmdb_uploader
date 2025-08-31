import time
from datetime import datetime
import traceback


def wrap_query(name: str, query_fn):
    start = time.time()
    diagnostics = {
        "label": name,
        "last_fetched": datetime.utcnow().isoformat(),
    }

    try:
        result = query_fn()
        duration = round((time.time() - start) * 1000, 2)  # ms

        diagnostics.update(
            {
                "data": result,
                "record_count": len(result),
                "duration_ms": duration,
                "status": "ok",
            }
        )

        # Optional freshness check
        if result and isinstance(result[0], dict) and "updated_at" in result[0]:
            updated_times = [r["updated_at"] for r in result if r.get("updated_at")]
            if updated_times:
                diagnostics["freshness"] = max(updated_times)

        # Optional volatility check
        if result and isinstance(result[0], dict):
            diagnostics["volatility"] = compute_volatility(result)

        # Optional SQL string if query_fn exposes it
        if hasattr(query_fn, "__name__"):
            diagnostics["query_name"] = query_fn.__name__

    except Exception as e:
        diagnostics.update(
            {
                "data": [],
                "record_count": 0,
                "duration_ms": None,
                "status": "error",
                "error": str(e),
                "traceback": traceback.format_exc(),
            }
        )

    return diagnostics


def compute_volatility(rows):
    from statistics import stdev, mean

    volatility = {}
    numeric_fields = {
        k for row in rows for k, v in row.items() if isinstance(v, (int, float))
    }

    for field in numeric_fields:
        values = [
            row[field] for row in rows if isinstance(row.get(field), (int, float))
        ]
        if len(values) > 1:
            volatility[field] = round(stdev(values) / (mean(values) or 1), 3)

    return volatility
