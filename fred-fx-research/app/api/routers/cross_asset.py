"""
Cross Asset API
- POST /cross-asset/features/rebuild  : 特徴量再計算
- GET  /cross-asset/features          : 特徴量取得 (long-form)
- GET  /panels/fx-crossasset          : FX クロスアセットパネル (wide-form)
"""

from datetime import date, datetime
from typing import Optional

from fastapi import APIRouter, Query

from app.models.api_models import ApiResponse, CrossAssetFeatureBuildRequest, RollingStatsRequest
from app.services import cross_asset_feature_service, join_panel_service, market_bar_service, normalize_service, rolling_stats_service
from app.storage.repositories import cross_asset_repo

router = APIRouter(tags=["cross-asset"])


@router.post("/cross-asset/features/rebuild")
async def rebuild_features(req: CrossAssetFeatureBuildRequest) -> ApiResponse:
    """
    USATECH の instrument-level 特徴量と pair-level 特徴量を再計算して保存する。
    """
    inst_n = cross_asset_feature_service.rebuild_instrument_features(
        instrument_id=req.source_instrument_id,
        start=req.start,
        end=req.end,
    )
    pair_n = cross_asset_feature_service.rebuild_pair_features(
        pairs=req.pairs,
        source_instrument_id=req.source_instrument_id,
        start=req.start,
        end=req.end,
    )
    return ApiResponse(
        data={
            "source_instrument_id": req.source_instrument_id,
            "instrument_features_rows": inst_n,
            "pair_features_rows": pair_n,
        },
        as_of=datetime.utcnow(),
    )


@router.get("/cross-asset/features")
async def get_features(
    feature_scope: str = Query(..., description="'instrument' | 'pair' | 'global'"),
    scope_id: str = Query(..., description="'usatechidxusd_h4' | 'USDJPY' 等"),
    feature_names: Optional[str] = Query(None, description="カンマ区切り特徴量名"),
    start: Optional[date] = Query(None),
    end: Optional[date] = Query(None),
    pivot: bool = Query(False, description="True で wide-form (pivot) を返す"),
) -> ApiResponse:
    """
    クロスアセット特徴量を返す。
    pivot=False → long-form, pivot=True → wide-form
    """
    names = [n.strip() for n in feature_names.split(",")] if feature_names else None

    if pivot:
        records = cross_asset_repo.get_features_pivot(feature_scope, scope_id, names, start, end)
    else:
        records = cross_asset_repo.get_features(feature_scope, scope_id, names, start, end)

    return ApiResponse(data=records, as_of=datetime.utcnow())


@router.get("/panels/fx-crossasset")
async def get_fx_crossasset_panel(
    pair: str = Query(..., description="USDJPY | EURUSD | AUDUSD"),
    start: Optional[date] = Query(None),
    end: Optional[date] = Query(None),
    include_usatech: bool = Query(True),
    include_fred: bool = Query(True),
    rebuild: bool = Query(False, description="True でパネルを再構築してから返す"),
) -> ApiResponse:
    """
    FX クロスアセット日次パネルを返す。
    rebuild=True の場合は mart を再構築してから返す。
    """
    if rebuild:
        join_panel_service.build_panel(pair, start, end)

    records = cross_asset_repo.get_panel(pair, start, end)

    # フィルタリング
    if not include_usatech:
        usatech_cols = {
            "usatech_close", "usatech_ret_1d", "usatech_mom_5d", "usatech_mom_20d",
            "usatech_rv_5d", "usatech_rv_20d", "usatech_drawdown_20d", "usatech_range_pct_1d",
        }
        records = [{k: v for k, v in r.items() if k not in usatech_cols} for r in records]

    if not include_fred:
        fred_cols = {"vix_close", "usd_broad_close", "rate_spread_3m", "yield_spread_10y"}
        records = [{k: v for k, v in r.items() if k not in fred_cols} for r in records]

    return ApiResponse(data=records, as_of=datetime.utcnow())


@router.post("/panels/fx-crossasset/rebuild")
async def rebuild_fx_crossasset_panel(
    pair: str = Query(..., description="USDJPY | EURUSD | AUDUSD"),
    start: Optional[date] = Query(None),
    end: Optional[date] = Query(None),
    full: bool = Query(True, description="True で日次集約+特徴量計算も実行"),
) -> ApiResponse:
    """
    FX クロスアセットパネルを明示的に再構築する。
    full=True の場合: H4→日次集約 → instrument特徴量 → pair特徴量 → パネル構築
    """
    instrument_id = "usatechidxusd_h4"
    daily_rows = 0
    inst_rows = 0
    pair_rows = 0

    norm_rows = 0

    if full:
        # 0. FX spot を raw → normalized に正規化
        fx_series = cross_asset_feature_service.PAIR_TO_SERIES.get(pair)
        if fx_series:
            norm_rows += normalize_service.normalize_series(fx_series, start, end)
        # VIX / USD Broad も正規化（エラー時はスキップ）
        for sid in ["VIXCLS", "DTWEXBGS"]:
            try:
                norm_rows += normalize_service.normalize_series(sid, start, end)
            except Exception:
                pass

        # 1. H4 → 日次集約
        daily_rows = market_bar_service.rebuild_daily(
            instrument_id=instrument_id,
            timeframe_source="240",
            start=start,
            end=end,
        )
        # 2. instrument-level 特徴量
        inst_rows = cross_asset_feature_service.rebuild_instrument_features(
            instrument_id=instrument_id,
            start=start,
            end=end,
        )
        # 3. pair-level 特徴量
        pair_rows = cross_asset_feature_service.rebuild_pair_features(
            pairs=[pair],
            source_instrument_id=instrument_id,
            start=start,
            end=end,
        )

    # 4. パネル構築
    n = join_panel_service.build_panel(pair, start, end)
    return ApiResponse(
        data={
            "pair": pair,
            "rows_built": n,
            "daily_rows": daily_rows,
            "instrument_features": inst_rows,
            "pair_features": pair_rows,
            "normalized_rows": norm_rows,
        },
        as_of=datetime.utcnow(),
    )


@router.post("/cross-asset/rolling-stats")
async def rolling_stats(req: RollingStatsRequest) -> ApiResponse:
    result = rolling_stats_service.compute_rolling_stats(
        pair=req.pair,
        start=req.start,
        end=req.end,
        corr_window=req.corr_window,
        beta_window=req.beta_window,
    )
    return ApiResponse(data=result, as_of=datetime.utcnow())
