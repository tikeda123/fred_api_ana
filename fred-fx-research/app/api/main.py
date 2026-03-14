import traceback
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from app.core.config import settings
from app.core.logging import get_logger, setup_logging
from app.storage.duckdb import get_connection, close_connection
from app.api.routers import health, series, observations, panels, factors, regimes, releases, memo, ohlc, data_quality, market_uploads, market_bars, cross_asset, events, filters
import os
from pathlib import Path

logger = get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    setup_logging()
    # データディレクトリ作成
    for d in [settings.raw_data_root, settings.normalized_data_root, settings.derived_data_root]:
        Path(d).mkdir(parents=True, exist_ok=True)
    # DuckDB 初期化
    get_connection()
    logger.info("app_started", env=settings.app_env)
    yield
    close_connection()
    logger.info("app_stopped")


app = FastAPI(
    title="FRED FX Research API",
    version="0.1.0",
    description="FRED/ALFRED を使った FX マーケット分析基盤",
    lifespan=lifespan,
)

@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception):
    tb = traceback.format_exception(type(exc), exc, exc.__traceback__)
    logger.error("unhandled_exception", path=str(request.url), error=str(exc), traceback="".join(tb))
    return JSONResponse(
        status_code=500,
        content={"status": "error", "detail": str(exc), "traceback": "".join(tb)},
    )

app.include_router(health.router, tags=["health"])
app.include_router(series.router, prefix="/api/v1", tags=["series"])
app.include_router(observations.router, prefix="/api/v1", tags=["observations"])
app.include_router(panels.router, prefix="/api/v1", tags=["panels"])
app.include_router(factors.router, prefix="/api/v1", tags=["factors"])
app.include_router(regimes.router, prefix="/api/v1", tags=["regimes"])
app.include_router(releases.router, prefix="/api/v1", tags=["releases"])
app.include_router(memo.router, prefix="/api/v1", tags=["memo"])
app.include_router(ohlc.router, prefix="/api/v1", tags=["ohlc"])
app.include_router(data_quality.router, prefix="/api/v1", tags=["data-quality"])
app.include_router(market_uploads.router, prefix="/api/v1", tags=["market-uploads"])
app.include_router(market_bars.router, prefix="/api/v1", tags=["market-bars"])
app.include_router(cross_asset.router, prefix="/api/v1", tags=["cross-asset"])
app.include_router(events.router, prefix="/api/v1", tags=["events"])
app.include_router(filters.router, prefix="/api/v1", tags=["filters"])
