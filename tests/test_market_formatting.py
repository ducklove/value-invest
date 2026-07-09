from services.market.formatting import format_indicator_change


def test_format_indicator_change_normalizes_direction_and_sign():
    assert format_indicator_change({"change_pct": "+1.25%", "direction": "up"}) == "▲1.25%"
    assert format_indicator_change({"change_pct": "down -0.80%", "direction": "down"}) == "▼0.80%"
    assert format_indicator_change({"change": "0.10", "direction": ""}) == "0.10"
    assert format_indicator_change({}) == ""
