"""
FRED API を使って各国の国債利回り情報を取得するプログラム
"""

import requests
import json
from datetime import datetime, timedelta


FRED_API_KEY = "295c8ed11b9219fba6a09bcdeb21a20d"
FRED_BASE_URL = "https://api.stlouisfed.org/fred/series/observations"

# 主要国の国債利回りシリーズID (FRED)
BOND_SERIES = {
    "米国 2年債":   "DGS2",
    "米国 5年債":   "DGS5",
    "米国 10年債":  "DGS10",
    "米国 30年債":  "DGS30",
    "日本 10年債":  "IRLTLT01JPM156N",
    "ドイツ 10年債": "IRLTLT01DEM156N",
    "英国 10年債":  "IRLTLT01GBM156N",
    "フランス 10年債": "IRLTLT01FRM156N",
    "イタリア 10年債": "IRLTLT01ITM156N",
    "カナダ 10年債": "IRLTLT01CAM156N",
}


def fetch_series(series_id: str, start_date: str, end_date: str) -> list[dict]:
    """FREDから指定シリーズのデータを取得する"""
    params = {
        "series_id": series_id,
        "api_key": FRED_API_KEY,
        "file_type": "json",
        "observation_start": start_date,
        "observation_end": end_date,
        "sort_order": "desc",
        "limit": 10,
    }
    response = requests.get(FRED_BASE_URL, params=params, timeout=10)
    response.raise_for_status()
    data = response.json()
    return data.get("observations", [])


def get_latest_value(observations: list[dict]) -> tuple[str, str]:
    """有効な最新値を返す (日付, 値)"""
    for obs in observations:
        if obs["value"] != ".":
            return obs["date"], obs["value"]
    return "N/A", "N/A"


def main():
    end_date = datetime.today().strftime("%Y-%m-%d")
    start_date = (datetime.today() - timedelta(days=60)).strftime("%Y-%m-%d")

    print("=" * 55)
    print("  各国国債利回り情報 (FRED API)")
    print(f"  取得日: {end_date}")
    print("=" * 55)
    print(f"{'国債':<20} {'日付':<14} {'利回り (%)':>10}")
    print("-" * 55)

    results = []
    for label, series_id in BOND_SERIES.items():
        try:
            observations = fetch_series(series_id, start_date, end_date)
            date, value = get_latest_value(observations)
            results.append({"label": label, "series_id": series_id, "date": date, "value": value})
            print(f"{label:<20} {date:<14} {value:>10}")
        except requests.RequestException as e:
            print(f"{label:<20} {'ERROR':<14} {str(e)[:20]:>10}")

    # JSON保存
    output_file = f"bonds_{end_date}.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    print("-" * 55)
    print(f"\n結果を {output_file} に保存しました。")


if __name__ == "__main__":
    main()
