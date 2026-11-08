"""History features: no future leakage, arrival-aware."""
from falcon.mart import history_features

FI_SCHEMA = ("fid string, publisher_id string, dt string, slot_arrival_dt string, request_arrival_dt string, "
             "request_found boolean, has_preview boolean, is_click boolean, first_click_arrival_dt string, "
             "is_conversion boolean, first_conv_arrival_dt string")


def _write(spark, wh, rows):
    spark.createDataFrame(rows, FI_SCHEMA).write.partitionBy("dt").parquet(f"{wh}/dw/fact_impression")


def slot(fid, dt, conv_arrival=None, click_arrival=None, pub="P1", arr=None, preview=False, req_found=True):
    return (fid, pub, dt, arr or dt, arr or dt, req_found, preview, click_arrival is not None, click_arrival,
            conv_arrival is not None, conv_arrival)


def _rows(spark, wh, targets, windows=(7, 28), group="publisher_id"):
    rows = spark.createDataFrame(targets, "publisher_id string, dt string")
    out = history_features(spark, wh, rows, list(windows), group, "pub")
    return {(r.publisher_id, r.dt): r.asDict() for r in out.collect()}


def test_window_excludes_the_request_day_and_the_future(spark, wh):
    _write(spark, wh, [
        slot("a", "2026-09-01", conv_arrival="2026-09-02"),       # 7d window of 09-08: dt 09-01..09-07 -> inside
        slot("b", "2026-09-07"),                                  # inside
        slot("c", "2026-09-08", conv_arrival="2026-09-08"),       # request day itself: excluded
        slot("d", "2026-09-09", conv_arrival="2026-09-09"),       # future: excluded
        slot("e", "2026-08-31", conv_arrival="2026-09-01"),       # 8 days back: outside 7d, inside 28d
    ])
    got = _rows(spark, wh, [("P1", "2026-09-08")])[("P1", "2026-09-08")]
    assert got["pub_slots_7d"] == 2 and got["pub_conv_rate_7d"] == 0.5
    assert got["pub_slots_28d"] == 3 and abs(got["pub_conv_rate_28d"] - 2 / 3) < 1e-9


def test_conversion_reported_on_or_after_request_day_is_not_known_yet(spark, wh):
    # the slot is old, but its conversion is only reported on 09-08 / 09-09: not visible for requests of 09-08
    _write(spark, wh, [slot("a", "2026-09-03", conv_arrival="2026-09-08"),
                       slot("b", "2026-09-03", conv_arrival="2026-09-05"),
                       slot("c", "2026-09-03")])
    got = _rows(spark, wh, [("P1", "2026-09-08"), ("P1", "2026-09-09")])
    assert got[("P1", "2026-09-08")]["pub_conv_rate_7d"] == 1 / 3      # only b
    assert got[("P1", "2026-09-09")]["pub_conv_rate_7d"] == 2 / 3      # a became known on 09-08


def test_clicks_follow_the_same_arrival_rule(spark, wh):
    _write(spark, wh, [slot("a", "2026-09-03", click_arrival="2026-09-08"), slot("b", "2026-09-03", click_arrival="2026-09-04")])
    got = _rows(spark, wh, [("P1", "2026-09-08")])[("P1", "2026-09-08")]
    assert got["pub_click_rate_7d"] == 0.5


def test_slot_that_arrived_late_is_unknown_until_it_arrived(spark, wh):
    _write(spark, wh, [slot("a", "2026-09-03", arr="2026-09-08"), slot("b", "2026-09-03")])
    got = _rows(spark, wh, [("P1", "2026-09-08"), ("P1", "2026-09-09")])
    assert got[("P1", "2026-09-08")]["pub_slots_7d"] == 1
    assert got[("P1", "2026-09-09")]["pub_slots_7d"] == 2


def test_no_history_gives_null_rate_not_zero(spark, wh):
    _write(spark, wh, [slot("a", "2026-09-20")])
    got = _rows(spark, wh, [("P1", "2026-09-08"), ("P2", "2026-09-08")])
    for k in [("P1", "2026-09-08"), ("P2", "2026-09-08")]:
        assert got[k]["pub_slots_7d"] == 0 and got[k]["pub_conv_rate_7d"] is None and got[k]["pub_click_rate_28d"] is None


def test_publishers_are_separate_and_previews_and_orphans_ignored(spark, wh):
    _write(spark, wh, [slot("a", "2026-09-05", conv_arrival="2026-09-06", pub="P1"),
                       slot("b", "2026-09-05", pub="P2"),
                       slot("c", "2026-09-05", conv_arrival="2026-09-06", pub="P1", preview=True),
                       slot("d", "2026-09-05", pub="P1", req_found=False)])
    got = _rows(spark, wh, [("P1", "2026-09-08"), ("P2", "2026-09-08")])
    assert got[("P1", "2026-09-08")]["pub_slots_7d"] == 1 and got[("P1", "2026-09-08")]["pub_conv_rate_7d"] == 1.0
    assert got[("P2", "2026-09-08")]["pub_slots_7d"] == 1 and got[("P2", "2026-09-08")]["pub_conv_rate_7d"] == 0.0


def test_history_is_stable_when_computed_later(spark, wh):
    """Whatever arrives on or after the request day must not change that day's features."""
    import shutil
    base = [slot("a", "2026-09-03", conv_arrival="2026-09-04"), slot("b", "2026-09-04")]
    _write(spark, wh, base)
    before = _rows(spark, wh, [("P1", "2026-09-08")])[("P1", "2026-09-08")]
    shutil.rmtree(f"{wh}/dw/fact_impression")
    later = [
        slot("b", "2026-09-04", conv_arrival="2026-09-12"),        # b converts after the request day (replaces b)
        slot("x", "2026-09-10", conv_arrival="2026-09-11"),        # future slot
        slot("z", "2026-09-05", arr="2026-09-09"),                 # slot of 09-05 that only arrived on 09-09
    ]
    _write(spark, wh, [slot("a", "2026-09-03", conv_arrival="2026-09-04")] + later)
    after = _rows(spark, wh, [("P1", "2026-09-08")])[("P1", "2026-09-08")]
    for k in ("pub_slots_7d", "pub_conv_rate_7d", "pub_click_rate_7d", "pub_slots_28d", "pub_conv_rate_28d"):
        assert before[k] == after[k], k
