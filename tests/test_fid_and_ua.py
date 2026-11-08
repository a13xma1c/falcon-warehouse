"""fid decoding and user-agent parsing."""
import pytest
from pyspark.sql import functions as F

from falcon import ua
from falcon.common import encode_fid, fid_position, fid_request_id


def test_fid_roundtrip(spark):
    fids = [encode_fid("e6130887b008ab9e", 0), encode_fid("e6130887b008ab9e", 10), encode_fid("0000000000000001", 3)]
    df = spark.createDataFrame([(f,) for f in fids], "fid string")
    rows = df.select(fid_request_id("fid").alias("r"), fid_position("fid").alias("p")).collect()
    assert [(r.r, r.p) for r in rows] == [("e6130887b008ab9e", 0), ("e6130887b008ab9e", 10), ("0000000000000001", 3)]


def test_fid_known_value_from_data(spark):
    # taken from the dataset: base64url("00a3b4c5d6e7f801:2") with padding stripped
    fid = "MDBhM2I0YzVkNmU3ZjgwMToy"
    r = spark.createDataFrame([(fid,)], "fid string").select(fid_request_id("fid").alias("r"), fid_position("fid").alias("p")).first()
    assert (r.r, r.p) == ("00a3b4c5d6e7f801", 2)


@pytest.mark.parametrize("bad", ["!!bad", "", "AAAA", "bm9jb2xvbg"])    # last one decodes to "nocolon"
def test_fid_bad_values_give_null(spark, bad):
    r = spark.createDataFrame([(bad,)], "fid string").select(fid_request_id("fid").alias("r"), fid_position("fid").alias("p")).first()
    assert r.r is None and r.p is None


def test_fid_null_input(spark):
    r = spark.createDataFrame([(None,)], "fid string").select(fid_request_id("fid").alias("r")).first()
    assert r.r is None


UA_CASES = [
    # (user agent, device, os, browser)
    ("Mozilla/5.0 (iPhone; CPU iPhone OS 18_7 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/26.6.1 Mobile/15E148 Safari/604.1",
     "mobile", "iOS", "Safari"),
    ("Mozilla/5.0 (iPhone; CPU iPhone OS 18_7 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) CriOS/150.0 Mobile/15E148 Safari/604.1",
     "mobile", "iOS", "Chrome"),
    ("Mozilla/5.0 (iPhone; CPU iPhone OS 18_7 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148 [FBAN/FBIOS;FBAV/500.0]",
     "mobile", "iOS", "Facebook in-app"),
    ("Mozilla/5.0 (iPhone; CPU iPhone OS 18_7 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148 Instagram 300.0 (iPhone14,2)",
     "mobile", "iOS", "Instagram in-app"),
    ("Mozilla/5.0 (iPhone; CPU iPhone OS 18_7 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148",
     "mobile", "iOS", "iOS WebView"),
    ("Mozilla/5.0 (iPad; CPU OS 18_7 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.0 Mobile/15E148 Safari/604.1",
     "tablet", "iOS", "Safari"),
    ("Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/155.0.0.0 Mobile Safari/537.36",
     "mobile", "Android", "Chrome"),
    ("Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/153.0.0.0 Safari/537.36",
     "tablet", "Android", "Chrome"),      # Android without "Mobile" is a tablet
    ("Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) SamsungBrowser/30.0 Chrome/143.0.0.0 Mobile Safari/537.36",
     "mobile", "Android", "Samsung Internet"),
    ("Mozilla/5.0 (Linux; Android 14; SM-S911B Build/UP1A; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/151.0 Mobile Safari/537.36",
     "mobile", "Android", "Android WebView"),
    ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/153.0.0.0 Safari/537.36",
     "desktop", "Windows", "Chrome"),
    ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/153.0.0.0 Safari/537.36 Edg/153.0.0.0",
     "desktop", "Windows", "Edge"),
    ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/153.0.0.0 Safari/537.36 OPR/110.0.0.0",
     "desktop", "Windows", "Opera"),
    ("Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:156.0) Gecko/20100101 Firefox/156.0", "desktop", "Windows", "Firefox"),
    ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/26.6.1 Safari/605.1.15",
     "desktop", "macOS", "Safari"),
    ("Mozilla/5.0 (X11; CrOS x86_64 14541.0.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0 Safari/537.36",
     "desktop", "ChromeOS", "Chrome"),
    ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0 Safari/537.36",
     "desktop", "Linux", "Chrome"),
    ("app/1.0 app/12.24.30-196819 (env: prod; platform: ios; osVersion: 26.6.2; brand: apple; model: iPhone)",
     "mobile", "iOS", "app"),
    ("app/1.0 app/12.24.30-196819 (env: prod; platform: android; osVersion: 14; brand: samsung; model: SM-S911B)",
     "mobile", "Android", "app"),
    ("Mozilla/5.0 (compatible; OrderEditing/1.0)", "unknown", "other", "other"),
    ("", "unknown", "unknown", "unknown"),
    (None, "unknown", "unknown", "unknown"),
]


def test_user_agent_parsing(spark):
    df = spark.createDataFrame([(u,) for u, *_ in UA_CASES], "ua string")
    got = df.select("ua", ua.device_type(F.col("ua")).alias("d"), ua.os_name(F.col("ua")).alias("o"),
                    ua.browser_name(F.col("ua")).alias("b")).collect()
    for row, (u, d, o, b) in zip(got, UA_CASES):
        assert (row.d, row.o, row.b) == (d, o, b), f"UA: {u}"
