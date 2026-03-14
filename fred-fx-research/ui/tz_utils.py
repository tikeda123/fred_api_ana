"""
タイムゾーン表示ユーティリティ
- UTC で保存されたデータを JST (Asia/Tokyo, UTC+9) で表示するためのヘルパー
"""

from datetime import datetime, timezone, timedelta

JST = timezone(timedelta(hours=9))


def now_jst() -> datetime:
    """現在の JST 日時を返す"""
    return datetime.now(JST)


def now_jst_str(fmt: str = "%Y-%m-%d %H:%M JST") -> str:
    """現在の JST 日時を文字列で返す"""
    return now_jst().strftime(fmt)


def to_jst(dt: datetime, fmt: str = "%Y-%m-%d %H:%M JST") -> str:
    """UTC datetime を JST 文字列に変換する。naive datetime は UTC として扱う。"""
    if dt is None:
        return ""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(JST).strftime(fmt)


def to_jst_short(dt: datetime) -> str:
    """短い JST 表示 (日付+時刻)"""
    return to_jst(dt, "%m/%d %H:%M")
