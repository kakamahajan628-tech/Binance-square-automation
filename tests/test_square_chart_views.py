from dataclasses import replace
from pathlib import Path
from time import perf_counter

import pytest
from PIL import Image

from test_square_desk import snapshot
from square_desk.analysis import studies
from square_desk.chart_views import indicators,choose_view,VIEWS,derivatives_context
from square_desk.images import chart
from square_desk.media import validate_png


def test_indicator_final_readings_match_existing_analysis():
    snap=snapshot()
    result=indicators(snap)
    metrics=studies(snap)
    for key in ('ema20','ema50','rsi','macd'):
        assert result[key][-1]==pytest.approx(metrics[key],abs=1e-10)
    assert result['signal'][-1]==pytest.approx(metrics['macd_signal'],abs=1e-10)
    assert result['vwap'][-1]==pytest.approx(metrics['vwap_24h'],abs=1e-10)
    assert result['lower'][-1]<=result['middle'][-1]<=result['upper'][-1]


def test_view_rotation_is_deterministic_and_covers_five_closed_bar_slots():
    snap=snapshot(end=1791936000)
    views=[choose_view(snapshot(end=snap.as_of+i*900)) for i in range(5)]
    assert set(views)==set(VIEWS)
    assert choose_view(snap)==choose_view(snap)


@pytest.mark.parametrize('view',VIEWS)
def test_each_view_is_valid_durable_png_under_existing_media_bounds(tmp_path,view):
    snap=snapshot(end=1791936000)
    snap.fetched_at=snap.as_of+1
    metrics=studies(snap)
    before=snap.dict()
    started=perf_counter()
    filename=chart(snap,metrics,tmp_path,context={'chart_view':view})
    data=(tmp_path/filename).read_bytes()
    assert validate_png(data)==Path(filename).stem
    with Image.open(tmp_path/filename) as image:
        assert image.size==(1600,1100)
    assert perf_counter()-started<15
    assert snap.dict()==before
    assert chart(snap,metrics,tmp_path,context={'chart_view':view})==filename


def test_styles_produce_distinct_bytes_for_identical_evidence(tmp_path):
    snap=snapshot()
    assert len({chart(snap,studies(snap),tmp_path,context={'chart_view':view}) for view in VIEWS})==5


def test_short_history_and_zero_volume_do_not_invent_vwap(tmp_path):
    snap=snapshot(length=55)
    snap.candles=[replace(c,volume=0) for c in snap.candles]
    assert all(value is None for value in indicators(snap)['vwap'])
    chart(snap,studies(snap),tmp_path,context={'chart_view':'value'})


def test_flat_price_renders_finite_rsi_and_overlapping_levels(tmp_path):
    snap=snapshot()
    snap.candles=[replace(c,open=1,high=1,low=1,close=1) for c in snap.candles]
    assert indicators(snap)['rsi'][-1]==50
    chart(snap,studies(snap),tmp_path,context={'chart_view':'structure'})


def test_open_interest_requires_real_matching_fresh_derivative_metadata():
    snap=snapshot()
    value={'source':'binance_usdt_perpetual','symbol':snap.symbol,
           'as_of':snap.fetched_at,'open_interest_base_units':1234.5}
    assert derivatives_context(snap,{}, {'derivatives':value})==value
    assert derivatives_context(snap,{}) is None
    for changed in ({'symbol':'OTHER'},{'source':'spot_volume'},{'as_of':snap.fetched_at-301},
                    {'open_interest_base_units':-1},{'open_interest_base_units':float('nan')}):
        assert derivatives_context(snap,{}, {'derivatives':{**value,**changed}}) is None
