"""User-agent parsing with Spark SQL expressions only (no UDF, no library).

Order of the rules matters: many UAs contain several tokens
("Chrome ... Safari", "Edg ... Chrome ... Safari", "iPhone ... like Mac OS X").
The most specific token is tested first.
"""
from __future__ import annotations
from pyspark.sql import Column, functions as F


def _l(ua: Column) -> Column:
    return F.lower(F.coalesce(ua, F.lit("")))


def device_type(ua: Column) -> Column:
    u = _l(ua)
    return (
        F.when(u == "", "unknown")
        .when(u.rlike(r"bot|crawler|spider|headless"), "bot")
        .when(u.rlike(r"^app/") & u.rlike(r"platform: (ios|android)"), "mobile")
        .when(u.rlike(r"ipad|tablet"), "tablet")
        # Android tablets omit the word "Mobile" in the UA
        .when(u.rlike(r"android") & ~u.rlike(r"mobile"), "tablet")
        .when(u.rlike(r"iphone|ipod|android|mobile"), "mobile")
        .when(u.rlike(r"windows|macintosh|x11|cros|linux"), "desktop")
        .otherwise("unknown")
    )


def os_name(ua: Column) -> Column:
    u = _l(ua)
    return (
        F.when(u == "", "unknown")
        .when(u.rlike(r"^app/") & u.rlike(r"platform: ios"), "iOS")
        .when(u.rlike(r"^app/") & u.rlike(r"platform: android"), "Android")
        .when(u.rlike(r"iphone|ipad|ipod|cpu os"), "iOS")
        .when(u.rlike(r"android"), "Android")
        .when(u.rlike(r"windows"), "Windows")
        .when(u.rlike(r"cros"), "ChromeOS")
        .when(u.rlike(r"mac os x|macintosh"), "macOS")
        .when(u.rlike(r"linux|x11"), "Linux")
        .otherwise("other")
    )


def browser_name(ua: Column) -> Column:
    u = _l(ua)
    return (
        F.when(u == "", "unknown")
        .when(u.rlike(r"^app/"), "app")
        .when(u.rlike(r"fban|fbav|fbios"), "Facebook in-app")
        .when(u.rlike(r"instagram"), "Instagram in-app")
        .when(u.rlike(r"edg(e|a|ios)?/"), "Edge")
        .when(u.rlike(r"opr/|opera"), "Opera")
        .when(u.rlike(r"samsungbrowser"), "Samsung Internet")
        .when(u.rlike(r"firefox|fxios"), "Firefox")
        .when(u.rlike(r"gsa/"), "Google app")
        .when(u.rlike(r"; wv\)"), "Android WebView")
        .when(u.rlike(r"crios|chrome/|chromium"), "Chrome")
        .when(u.rlike(r"safari"), "Safari")
        # iOS WebView: AppleWebKit + Mobile/xxx but no Safari token
        .when(u.rlike(r"applewebkit") & u.rlike(r"mobile/"), "iOS WebView")
        .otherwise("other")
    )
