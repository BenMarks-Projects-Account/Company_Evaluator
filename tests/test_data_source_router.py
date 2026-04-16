"""Tests for Phase 2 — DataSourceRouter + config toggle + diff logic."""

import asyncio
import json
import os
import tempfile
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio

from data.data_source_router import (
    DataSourceRouter,
    compare_responses,
    _compare_scalar,
    _pct_delta,
    _write_diff_log,
    _DIFF_LOG_PATH,
)


# ── Helpers ──────────────────────────────────────────────────

def _make_settings_mock(overrides: dict | None = None):
    """Create a mock Settings that returns a data source from overrides."""
    import json as _json
    from unittest.mock import MagicMock

    mock = MagicMock()
    mock.data_source_overrides = _json.dumps(overrides or {})

    def _get_ds(key):
        o = overrides or {}
        val = o.get(key, o.get("default", "polygon"))
        return val if val in ("polygon", "fmp", "shadow") else "polygon"

    mock.get_data_source = _get_ds
    return mock


# ═══════════════════════════════════════════════════════════════
# 1. Config: per-call-site source selection
# ═══════════════════════════════════════════════════════════════

class TestConfigDataSource:
    """Verify Settings.get_data_source() resolves correctly."""

    def test_default_is_polygon(self):
        from config import Settings
        s = Settings(data_source_overrides="{}")
        assert s.get_data_source("anything") == "polygon"

    def test_explicit_default_override(self):
        from config import Settings
        s = Settings(data_source_overrides='{"default":"fmp"}')
        assert s.get_data_source("anything") == "fmp"

    def test_per_site_override(self):
        from config import Settings
        s = Settings(data_source_overrides='{"entry_point.get_rsi":"fmp","default":"polygon"}')
        assert s.get_data_source("entry_point.get_rsi") == "fmp"
        assert s.get_data_source("entry_point.get_sma") == "polygon"

    def test_shadow_mode(self):
        from config import Settings
        s = Settings(data_source_overrides='{"entry_point.get_snapshot":"shadow"}')
        assert s.get_data_source("entry_point.get_snapshot") == "shadow"

    def test_invalid_value_falls_back_to_polygon(self):
        from config import Settings
        s = Settings(data_source_overrides='{"default":"INVALID"}')
        assert s.get_data_source("anything") == "polygon"

    def test_malformed_json_falls_back_to_polygon(self):
        from config import Settings
        s = Settings(data_source_overrides="not-json")
        assert s.get_data_source("anything") == "polygon"

    def test_empty_overrides(self):
        from config import Settings
        s = Settings(data_source_overrides="{}")
        assert s.get_data_source("anything") == "polygon"


# ═══════════════════════════════════════════════════════════════
# 2. Router: source selection
# ═══════════════════════════════════════════════════════════════

class TestRouterSourceSelection:
    """Router.route() dispatches to correct source based on config."""

    @pytest.mark.asyncio
    async def test_polygon_mode_calls_polygon_only(self):
        router = DataSourceRouter()
        poly_fn = AsyncMock(return_value={"price": 100})
        fmp_fn = AsyncMock(return_value={"price": 101})

        with patch("data.data_source_router.get_settings", return_value=_make_settings_mock({"default": "polygon"})):
            result = await router.route("test.key", poly_fn, fmp_fn, "AAPL")

        assert result == {"price": 100}
        poly_fn.assert_awaited_once_with("AAPL")
        fmp_fn.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_fmp_mode_calls_fmp_only(self):
        router = DataSourceRouter()
        poly_fn = AsyncMock(return_value={"price": 100})
        fmp_fn = AsyncMock(return_value={"price": 101})

        with patch("data.data_source_router.get_settings", return_value=_make_settings_mock({"test.key": "fmp"})):
            result = await router.route("test.key", poly_fn, fmp_fn, "AAPL")

        assert result == {"price": 101}
        fmp_fn.assert_awaited_once_with("AAPL")
        poly_fn.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_fmp_mode_falls_back_when_fmp_fn_is_none(self):
        router = DataSourceRouter()
        poly_fn = AsyncMock(return_value={"data": 1})

        with patch("data.data_source_router.get_settings", return_value=_make_settings_mock({"test.key": "fmp"})):
            result = await router.route("test.key", poly_fn, None, "AAPL")

        assert result == {"data": 1}
        poly_fn.assert_awaited_once_with("AAPL")

    @pytest.mark.asyncio
    async def test_per_call_site_override(self):
        router = DataSourceRouter()
        poly_fn = AsyncMock(return_value="polygon_result")
        fmp_fn = AsyncMock(return_value="fmp_result")

        overrides = {"default": "polygon", "site_a": "fmp"}
        with patch("data.data_source_router.get_settings", return_value=_make_settings_mock(overrides)):
            # site_a → fmp
            res_a = await router.route("site_a", poly_fn, fmp_fn, "X")
            assert res_a == "fmp_result"

            poly_fn.reset_mock()
            fmp_fn.reset_mock()

            # site_b → polygon (default)
            res_b = await router.route("site_b", poly_fn, fmp_fn, "X")
            assert res_b == "polygon_result"

    @pytest.mark.asyncio
    async def test_kwargs_forwarded(self):
        router = DataSourceRouter()
        poly_fn = AsyncMock(return_value=42)

        with patch("data.data_source_router.get_settings", return_value=_make_settings_mock()):
            await router.route("k", poly_fn, None, "AAPL", days=365, limit=10)

        poly_fn.assert_awaited_once_with("AAPL", days=365, limit=10)


# ═══════════════════════════════════════════════════════════════
# 3. Shadow mode
# ═══════════════════════════════════════════════════════════════

class TestShadowMode:
    """Shadow mode: call both, return Polygon, log diff."""

    @pytest.mark.asyncio
    async def test_shadow_returns_polygon_result(self):
        router = DataSourceRouter()
        poly_fn = AsyncMock(return_value={"source": "polygon"})
        fmp_fn = AsyncMock(return_value={"source": "fmp"})

        with patch("data.data_source_router.get_settings", return_value=_make_settings_mock({"x": "shadow"})):
            with patch("data.data_source_router._write_diff_log"):
                result = await router.route("x", poly_fn, fmp_fn, "AAPL")

        assert result == {"source": "polygon"}
        poly_fn.assert_awaited_once()
        fmp_fn.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_shadow_calls_both_sources(self):
        router = DataSourceRouter()
        poly_fn = AsyncMock(return_value=10)
        fmp_fn = AsyncMock(return_value=11)

        with patch("data.data_source_router.get_settings", return_value=_make_settings_mock({"x": "shadow"})):
            with patch("data.data_source_router._write_diff_log"):
                await router.route("x", poly_fn, fmp_fn, "MSFT")

        poly_fn.assert_awaited_once_with("MSFT")
        fmp_fn.assert_awaited_once_with("MSFT")

    @pytest.mark.asyncio
    async def test_shadow_polygon_exception_does_not_crash(self):
        router = DataSourceRouter()
        poly_fn = AsyncMock(side_effect=RuntimeError("polygon down"))
        fmp_fn = AsyncMock(return_value={"ok": True})

        with patch("data.data_source_router.get_settings", return_value=_make_settings_mock({"x": "shadow"})):
            with patch("data.data_source_router._write_diff_log"):
                result = await router.route("x", poly_fn, fmp_fn, "AAPL")

        # Polygon raised → result is None, but no crash
        assert result is None

    @pytest.mark.asyncio
    async def test_shadow_fmp_exception_does_not_crash(self):
        router = DataSourceRouter()
        poly_fn = AsyncMock(return_value={"ok": True})
        fmp_fn = AsyncMock(side_effect=RuntimeError("fmp down"))

        with patch("data.data_source_router.get_settings", return_value=_make_settings_mock({"x": "shadow"})):
            with patch("data.data_source_router._write_diff_log"):
                result = await router.route("x", poly_fn, fmp_fn, "AAPL")

        assert result == {"ok": True}

    @pytest.mark.asyncio
    async def test_shadow_writes_diff_log(self):
        router = DataSourceRouter()
        poly_fn = AsyncMock(return_value={"price": 100.0})
        fmp_fn = AsyncMock(return_value={"price": 100.5})

        with patch("data.data_source_router.get_settings", return_value=_make_settings_mock({"x": "shadow"})):
            with patch("data.data_source_router._write_diff_log") as mock_log:
                await router.route("x", poly_fn, fmp_fn, "AAPL")

        mock_log.assert_called_once()
        entry = mock_log.call_args[0][0]
        assert entry["call_site"] == "x"
        assert entry["symbol"] == "AAPL"
        assert entry["source_returned"] == "polygon"
        assert "diff" in entry

    @pytest.mark.asyncio
    async def test_shadow_macd_no_sma_note(self):
        """MACD note was removed after SMA→EMA fix; diff should have no note."""
        router = DataSourceRouter()
        poly_fn = AsyncMock(return_value={"value": 1.0, "signal": 0.5, "histogram": 0.5})
        fmp_fn = AsyncMock(return_value={"value": 1.0, "signal": 0.6, "histogram": 0.4})

        with patch("data.data_source_router.get_settings", return_value=_make_settings_mock({"entry_point.get_macd": "shadow"})):
            with patch("data.data_source_router._write_diff_log") as mock_log:
                await router.route("entry_point.get_macd", poly_fn, fmp_fn, "AAPL")

        entry = mock_log.call_args[0][0]
        assert "note" not in entry["diff"]


# ═══════════════════════════════════════════════════════════════
# 4. Numeric diff calculation
# ═══════════════════════════════════════════════════════════════

class TestNumericDiff:
    """Field comparison logic for shadow mode diff."""

    def test_pct_delta_normal(self):
        assert _pct_delta(100, 101) == pytest.approx(1.0)

    def test_pct_delta_zero_base(self):
        assert _pct_delta(0, 5) is None

    def test_pct_delta_both_zero(self):
        assert _pct_delta(0, 0) == 0.0

    def test_compare_scalar_numeric(self):
        result = _compare_scalar("close", 150.0, 150.5, 0.5)
        assert result["field"] == "close"
        assert result["abs_delta"] == pytest.approx(0.5)
        assert result["pct_delta"] == pytest.approx(0.3333, abs=0.001)
        assert result["exceeds_threshold"] is False

    def test_compare_scalar_exceeds_threshold(self):
        result = _compare_scalar("close", 100.0, 102.0, 0.5)
        assert result["exceeds_threshold"] is True

    def test_compare_scalar_categorical_match(self):
        result = _compare_scalar("symbol", "AAPL", "AAPL", 0.0)
        assert result["match"] is True

    def test_compare_scalar_categorical_mismatch(self):
        result = _compare_scalar("symbol", "AAPL", "MSFT", 0.0)
        assert result["match"] is False

    def test_compare_scalar_missing_in_fmp(self):
        result = _compare_scalar("vwap", 150.0, None, 0.5)
        assert result["status"] == "missing_in_fmp"

    def test_compare_scalar_missing_in_polygon(self):
        result = _compare_scalar("vwap", None, 150.0, 0.5)
        assert result["status"] == "missing_in_polygon"

    def test_compare_scalar_both_none(self):
        result = _compare_scalar("vwap", None, None, 0.5)
        assert result["status"] == "both_none"


# ═══════════════════════════════════════════════════════════════
# 5. Response comparison (bars, dicts, scalars)
# ═══════════════════════════════════════════════════════════════

class TestCompareResponses:
    """Structured diff between Polygon and FMP responses."""

    def test_compare_dicts(self):
        poly = {"symbol": "AAPL", "last_price": 150.0, "day_volume": 1000}
        fmp = {"symbol": "AAPL", "last_price": 150.5, "day_volume": 1050}
        diff = compare_responses(poly, fmp, "test")
        assert diff["polygon_type"] == "dict"
        # Verify fields were compared
        fields = {f["field"]: f for f in diff["fields"]}
        assert "last_price" in fields
        assert fields["symbol"]["match"] is True

    def test_compare_scalars(self):
        diff = compare_responses(45.2, 45.8, "test.rsi")
        assert diff["fields"][0]["field"] == "value"
        assert diff["fields"][0]["abs_delta"] == pytest.approx(0.6, abs=0.01)

    def test_compare_bars_by_date(self):
        poly = [
            {"date": "2024-01-01", "open": 100, "close": 105, "high": 106, "low": 99, "volume": 1000},
            {"date": "2024-01-02", "open": 105, "close": 110, "high": 111, "low": 104, "volume": 1200},
        ]
        fmp = [
            {"date": "2024-01-01", "open": 100, "close": 105.1, "high": 106, "low": 99, "volume": 1000},
            {"date": "2024-01-02", "open": 105, "close": 110, "high": 111, "low": 104, "volume": 1200},
            {"date": "2024-01-03", "open": 110, "close": 112, "high": 113, "low": 109, "volume": 900},
        ]
        diff = compare_responses(poly, fmp, "test.bars")
        assert diff["polygon_bar_count"] == 2
        assert diff["fmp_bar_count"] == 3
        assert diff["dates_only_fmp"] == 1
        assert diff["dates_only_polygon"] == 0

    def test_compare_bars_mismatched_lengths(self):
        poly = [{"date": "2024-01-01", "close": 100, "open": 100, "high": 100, "low": 100, "volume": 100}]
        fmp = []
        diff = compare_responses(poly, fmp, "test")
        assert diff["polygon_bar_count"] == 1
        assert diff["fmp_bar_count"] == 0

    def test_compare_none_both(self):
        diff = compare_responses(None, None, "test")
        assert diff["status"] == "both_none"

    def test_compare_polygon_none(self):
        diff = compare_responses(None, {"price": 100}, "test")
        assert diff["status"] == "polygon_none"

    def test_compare_fmp_none(self):
        diff = compare_responses({"price": 100}, None, "test")
        assert diff["status"] == "fmp_none"


# ═══════════════════════════════════════════════════════════════
# 6. Diff log writing
# ═══════════════════════════════════════════════════════════════

class TestDiffLog:
    """Structured diff log writes JSON lines."""

    def test_diff_log_writes_jsonl(self, tmp_path):
        log_path = str(tmp_path / "test_diff.log")
        with patch("data.data_source_router._DIFF_LOG_PATH", log_path):
            with patch("data.data_source_router._DIFF_LOG_DIR", str(tmp_path)):
                _write_diff_log({"call_site": "test", "result": "ok"})
                _write_diff_log({"call_site": "test2", "result": "ok2"})

        with open(log_path, "r") as f:
            lines = f.readlines()
        assert len(lines) == 2
        entry = json.loads(lines[0])
        assert entry["call_site"] == "test"


# ═══════════════════════════════════════════════════════════════
# 7. Phase 1 tests still pass (quick import check)
# ═══════════════════════════════════════════════════════════════

class TestPhase1Compat:
    """Ensure Phase 1 FMP methods are untouched."""

    def test_fmp_client_has_phase1_methods(self):
        from data.fmp_client import FMPClient
        methods = [m for m in dir(FMPClient) if not m.startswith("_")]
        for m in ("get_historical_price_eod", "get_quote", "get_technical_indicator", "get_macd"):
            assert m in methods, f"Phase 1 method {m} missing from FMPClient"

    def test_polygon_client_unchanged(self):
        from data.polygon_client import PolygonClient
        methods = [m for m in dir(PolygonClient) if not m.startswith("_")]
        for m in ("get_raw_bars", "get_snapshot", "get_rsi", "get_sma", "get_macd",
                   "get_financials", "get_company_details", "get_price_history", "get_tickers"):
            assert m in methods, f"Polygon method {m} should still exist"
