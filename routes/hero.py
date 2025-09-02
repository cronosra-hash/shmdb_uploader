# routes/hero.py
from fastapi import APIRouter
from db import fetch_hero_stats

router = APIRouter()

@router.get("/api/hero-stats")
async def get_hero_stats():
    stats = await fetch_hero_stats()
    return {
        "total_titles": stats.total_titles,
        "recent_updates": stats.recent_updates,
        "freshness_pct": stats.freshness_pct,
        "orphaned_records": stats.orphaned_records,
        "avg_field_volatility": stats.avg_field_volatility
    }
