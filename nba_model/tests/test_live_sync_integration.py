"""Integration tests for live_sync.py sync cycle and provider management."""
from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock
import sys
from datetime import datetime, timedelta, timezone

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT / "nba_model"))

from src import live_sync as ls


class LiveSyncRetryConfigurationTests(unittest.TestCase):
    """Test per-provider retry configuration functionality."""

    def setUp(self) -> None:
        """Reset module-level retry settings before each test."""
        ls.FETCH_RETRY_SETTINGS.clear()
        ls.FETCH_RETRY_SETTINGS.update({
            "attempts": 3,
            "base_delay_seconds": 0.5,
            "jitter_seconds": 0.2,
        })
        ls.PROVIDER_RETRY_OVERRIDES.clear()

    def test_update_fetch_retry_settings_global_config(self) -> None:
        """Test that global retry settings are loaded from config."""
        config = {
            "fetch_retry_attempts": 4,
            "fetch_retry_base_delay_seconds": 0.6,
            "fetch_retry_jitter_seconds": 0.25,
        }
        ls._update_fetch_retry_settings_from_config(config)
        
        self.assertEqual(ls.FETCH_RETRY_SETTINGS["attempts"], 4)
        self.assertAlmostEqual(ls.FETCH_RETRY_SETTINGS["base_delay_seconds"], 0.6, places=2)
        self.assertAlmostEqual(ls.FETCH_RETRY_SETTINGS["jitter_seconds"], 0.25, places=2)

    def test_update_fetch_retry_settings_provider_overrides(self) -> None:
        """Test that per-provider retry overrides are loaded correctly."""
        config = {
            "fetch_retry_attempts": 3,
            "fetch_retry_base_delay_seconds": 0.5,
            "fetch_retry_jitter_seconds": 0.2,
            "provider_retry_overrides": {
                "odds": {
                    "fetch_retry_attempts": 3,
                    "fetch_retry_base_delay_seconds": 0.5,
                    "fetch_retry_jitter_seconds": 0.2,
                },
                "injuries": {
                    "fetch_retry_attempts": 4,
                    "fetch_retry_base_delay_seconds": 0.8,
                    "fetch_retry_jitter_seconds": 0.3,
                },
                "lineups": {
                    "fetch_retry_attempts": 2,
                    "fetch_retry_base_delay_seconds": 0.3,
                    "fetch_retry_jitter_seconds": 0.1,
                },
            }
        }
        ls._update_fetch_retry_settings_from_config(config)
        
        # Check global settings
        self.assertEqual(ls.FETCH_RETRY_SETTINGS["attempts"], 3)
        
        # Check provider overrides
        self.assertIn("odds", ls.PROVIDER_RETRY_OVERRIDES)
        self.assertIn("injuries", ls.PROVIDER_RETRY_OVERRIDES)
        self.assertIn("lineups", ls.PROVIDER_RETRY_OVERRIDES)
        
        # Verify odds overrides
        self.assertEqual(ls.PROVIDER_RETRY_OVERRIDES["odds"].get("attempts"), 3)
        
        # Verify injuries has more retries
        self.assertEqual(ls.PROVIDER_RETRY_OVERRIDES["injuries"].get("attempts"), 4)
        self.assertAlmostEqual(
            ls.PROVIDER_RETRY_OVERRIDES["injuries"].get("base_delay_seconds", 0),
            0.8,
            places=2
        )

    def test_get_retry_settings_for_provider_with_override(self) -> None:
        """Test that provider-specific settings are returned when configured."""
        config = {
            "fetch_retry_attempts": 3,
            "fetch_retry_base_delay_seconds": 0.5,
            "fetch_retry_jitter_seconds": 0.2,
            "provider_retry_overrides": {
                "injuries": {
                    "fetch_retry_attempts": 4,
                    "fetch_retry_base_delay_seconds": 0.8,
                    "fetch_retry_jitter_seconds": 0.3,
                }
            }
        }
        ls._update_fetch_retry_settings_from_config(config)
        
        # Get settings for provider with override
        injury_settings = ls._get_retry_settings_for_provider("injuries")
        self.assertEqual(injury_settings["attempts"], 4)
        self.assertAlmostEqual(injury_settings["base_delay_seconds"], 0.8, places=2)
        self.assertAlmostEqual(injury_settings["jitter_seconds"], 0.3, places=2)

    def test_get_retry_settings_for_provider_fallback_to_global(self) -> None:
        """Test that global settings are used when no provider override exists."""
        config = {
            "fetch_retry_attempts": 3,
            "fetch_retry_base_delay_seconds": 0.5,
            "fetch_retry_jitter_seconds": 0.2,
            "provider_retry_overrides": {
                "odds": {
                    "fetch_retry_attempts": 3,
                    "fetch_retry_base_delay_seconds": 0.5,
                }
            }
        }
        ls._update_fetch_retry_settings_from_config(config)
        
        # Get settings for provider without override
        playstyle_settings = ls._get_retry_settings_for_provider("playstyle")
        self.assertEqual(playstyle_settings["attempts"], 3)
        self.assertAlmostEqual(playstyle_settings["base_delay_seconds"], 0.5, places=2)
        self.assertAlmostEqual(playstyle_settings["jitter_seconds"], 0.2, places=2)

    def test_retry_sleep_with_custom_parameters(self) -> None:
        """Test retry sleep calculation with custom base delay and jitter."""
        sleep_time = ls._retry_sleep_seconds(attempt=0, base_delay=1.0, jitter=0.0)
        # With jitter=0, should be exactly base_delay * (0 + 1) = 1.0
        self.assertAlmostEqual(sleep_time, 1.0, places=2)
        
        sleep_time = ls._retry_sleep_seconds(attempt=1, base_delay=1.0, jitter=0.0)
        # With jitter=0, should be exactly base_delay * (1 + 1) = 2.0
        self.assertAlmostEqual(sleep_time, 2.0, places=2)

    def test_retry_sleep_with_jitter_variance(self) -> None:
        """Test that jitter adds randomness to retry sleep."""
        # Run multiple times to check jitter variance
        sleep_times = [
            ls._retry_sleep_seconds(attempt=0, base_delay=1.0, jitter=0.1)
            for _ in range(10)
        ]
        # With base_delay=1.0, attempt=0: expect values ~1.0 ± 0.05 (with jitter up to 0.1)
        min_sleep = min(sleep_times)
        max_sleep = max(sleep_times)
        self.assertGreaterEqual(min_sleep, 0.9)  # At least close to 1.0
        self.assertLessEqual(max_sleep, 1.15)     # At most 1.0 + 0.1 jitter


class LiveSyncFetcherIntegrationTests(unittest.TestCase):
    """Test fetch function integration with retry settings."""

    def setUp(self) -> None:
        """Reset module-level retry settings before each test."""
        ls.FETCH_RETRY_SETTINGS.clear()
        ls.FETCH_RETRY_SETTINGS.update({
            "attempts": 3,
            "base_delay_seconds": 0.5,
            "jitter_seconds": 0.2,
        })
        ls.PROVIDER_RETRY_OVERRIDES.clear()

    def test_fetch_json_accepts_provider_name(self) -> None:
        """Test that fetch_json accepts provider_name parameter."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create a mock JSON response
            test_data = {"status": "ok", "data": [1, 2, 3]}
            
            with mock.patch("src.live_sync._fetch_bytes_with_retry") as mock_fetch:
                mock_fetch.return_value = json.dumps(test_data).encode("utf-8")
                
                result = ls.fetch_json(
                    url="https://example.com/api/data",
                    timeout=8,
                    provider_name="odds"
                )
                
                # Verify the mock was called with provider_name
                mock_fetch.assert_called_once()
                call_kwargs = mock_fetch.call_args.kwargs
                self.assertEqual(call_kwargs.get("provider_name"), "odds")
                self.assertEqual(result.get("status"), "ok")

    def test_fetch_text_accepts_provider_name(self) -> None:
        """Test that fetch_text accepts provider_name parameter."""
        test_data = "sample response text"
        
        with mock.patch("src.live_sync._fetch_bytes_with_retry") as mock_fetch:
            mock_fetch.return_value = test_data.encode("utf-8")
            
            result = ls.fetch_text(
                url="https://example.com/data",
                timeout=8,
                provider_name="rotowire_prizepicks"
            )
            
            # Verify the mock was called with provider_name
            mock_fetch.assert_called_once()
            call_kwargs = mock_fetch.call_args.kwargs
            self.assertEqual(call_kwargs.get("provider_name"), "rotowire_prizepicks")
            self.assertEqual(result, test_data)

    def test_fetch_binary_accepts_provider_name(self) -> None:
        """Test that fetch_binary accepts provider_name parameter."""
        test_data = b"\x89PNG\r\n\x1a\n"
        
        with mock.patch("src.live_sync._fetch_bytes_with_retry") as mock_fetch:
            mock_fetch.return_value = test_data
            
            result = ls.fetch_binary(
                url="https://example.com/image.png",
                timeout=8,
                provider_name="player_profiles"
            )
            
            # Verify the mock was called with provider_name
            mock_fetch.assert_called_once()
            call_kwargs = mock_fetch.call_args.kwargs
            self.assertEqual(call_kwargs.get("provider_name"), "player_profiles")
            self.assertEqual(result, test_data)


class LiveSyncProviderConfigurationTests(unittest.TestCase):
    """Test provider configuration and initialization."""

    def test_default_provider_configs_exist(self) -> None:
        """Test that default provider configurations are defined."""
        config = ls.DEFAULT_LIVE_CONFIG
        
        self.assertIn("providers", config)
        providers = config["providers"]
        
        # Check critical providers are configured
        self.assertIn("odds", providers)
        self.assertIn("player_props", providers)
        self.assertIn("injuries", providers)
        self.assertIn("lineups", providers)
        self.assertIn("playstyle", providers)

    def test_provider_enabled_status(self) -> None:
        """Test that provider enabled status is tracked correctly."""
        config = ls.DEFAULT_LIVE_CONFIG
        providers = config["providers"]
        
        # Critical providers should be enabled
        self.assertTrue(providers.get("odds", {}).get("enabled", False))
        self.assertTrue(providers.get("player_props", {}).get("enabled", False))
        self.assertTrue(providers.get("lineups", {}).get("enabled", False))
        
        # BETR should be disabled per BETR API analysis
        self.assertFalse(providers.get("betr", {}).get("enabled", True))

    def test_provider_timeout_configuration(self) -> None:
        """Test that request timeouts are configured for providers."""
        config = ls.DEFAULT_LIVE_CONFIG
        providers = config["providers"]
        
        # Check odds provider timeout
        odds_timeout = providers.get("odds", {}).get("request_timeout_seconds")
        self.assertIsNotNone(odds_timeout)
        self.assertGreater(odds_timeout, 0)
        self.assertLess(odds_timeout, 30)  # Should be reasonable
        
        # Check injuries provider timeout
        injuries_timeout = providers.get("injuries", {}).get("request_timeout_seconds")
        self.assertIsNotNone(injuries_timeout)
        self.assertGreater(injuries_timeout, 0)


class LiveSyncContextUpdateTests(unittest.TestCase):
    """Test live sync context and provider integration."""

    def test_context_key_columns_defined(self) -> None:
        """Test that required context key columns are defined."""
        # CONTEXT_KEY_COLUMNS should include critical provenance columns
        self.assertIsNotNone(ls.CONTEXT_KEY_COLUMNS)
        self.assertIsInstance(ls.CONTEXT_KEY_COLUMNS, list)
        self.assertGreater(len(ls.CONTEXT_KEY_COLUMNS), 0)
        
        # Should include basic player information
        required_cols = {"player_name", "team", "game_date"}
        context_set = set(ls.CONTEXT_KEY_COLUMNS)
        self.assertTrue(required_cols.issubset(context_set))

    def test_prop_line_context_columns_exist(self) -> None:
        """Test that prop line context columns are properly defined."""
        self.assertIsNotNone(ls.PROP_LINE_CONTEXT_COLUMNS)
        self.assertIsInstance(ls.PROP_LINE_CONTEXT_COLUMNS, list)
        
        # Should have market columns like line_points, line_assists, etc.
        prop_cols = set(ls.PROP_LINE_CONTEXT_COLUMNS)
        self.assertGreater(len(prop_cols), 0)


class LiveSyncInitializationTests(unittest.TestCase):
    """Test live_sync module initialization and startup."""

    def test_retry_settings_initialized(self) -> None:
        """Test that retry settings are properly initialized."""
        self.assertIsNotNone(ls.FETCH_RETRY_SETTINGS)
        self.assertIn("attempts", ls.FETCH_RETRY_SETTINGS)
        self.assertIn("base_delay_seconds", ls.FETCH_RETRY_SETTINGS)
        self.assertIn("jitter_seconds", ls.FETCH_RETRY_SETTINGS)
        
        # Should have reasonable defaults
        self.assertGreater(ls.FETCH_RETRY_SETTINGS["attempts"], 0)
        self.assertGreater(ls.FETCH_RETRY_SETTINGS["base_delay_seconds"], 0)
        self.assertGreaterEqual(ls.FETCH_RETRY_SETTINGS["jitter_seconds"], 0)

    def test_provider_retry_overrides_initialized(self) -> None:
        """Test that provider retry overrides structure exists."""
        self.assertIsNotNone(ls.PROVIDER_RETRY_OVERRIDES)
        # Should be an empty dict initially or populated after config load
        self.assertIsInstance(ls.PROVIDER_RETRY_OVERRIDES, dict)


class LiveSyncConfigurationLoadTests(unittest.TestCase):
    """Test loading and parsing of live_sync.json configuration."""

    def test_config_file_exists(self) -> None:
        """Test that live_sync.json configuration file exists."""
        config_path = Path(__file__).parent.parent / "config" / "live_sync.json"
        self.assertTrue(config_path.exists(), f"Config file not found at {config_path}")

    def test_config_file_valid_json(self) -> None:
        """Test that live_sync.json is valid JSON."""
        config_path = Path(__file__).parent.parent / "config" / "live_sync.json"
        with open(config_path, "r") as f:
            config_data = json.load(f)
        
        self.assertIsInstance(config_data, dict)
        self.assertIn("enabled", config_data)
        self.assertIn("providers", config_data)

    def test_config_has_provider_retry_overrides(self) -> None:
        """Test that live_sync.json includes provider_retry_overrides."""
        config_path = Path(__file__).parent.parent / "config" / "live_sync.json"
        with open(config_path, "r") as f:
            config_data = json.load(f)
        
        self.assertIn("provider_retry_overrides", config_data)
        overrides = config_data["provider_retry_overrides"]
        
        # Should have configs for main providers
        self.assertIn("odds", overrides)
        self.assertIn("player_props", overrides)
        self.assertIn("injuries", overrides)
        self.assertIn("lineups", overrides)
        self.assertIn("playstyle", overrides)

    def test_provider_retry_overrides_have_valid_values(self) -> None:
        """Test that provider retry override values are valid numbers."""
        config_path = Path(__file__).parent.parent / "config" / "live_sync.json"
        with open(config_path, "r") as f:
            config_data = json.load(f)
        
        overrides = config_data.get("provider_retry_overrides", {})
        for provider_name, settings in overrides.items():
            if isinstance(settings, dict):
                # Check attempts is positive integer
                if "fetch_retry_attempts" in settings:
                    attempts = settings["fetch_retry_attempts"]
                    self.assertIsInstance(attempts, int)
                    self.assertGreater(attempts, 0)
                
                # Check delays are positive numbers
                if "fetch_retry_base_delay_seconds" in settings:
                    delay = settings["fetch_retry_base_delay_seconds"]
                    self.assertIsInstance(delay, (int, float))
                    self.assertGreater(delay, 0)
                
                # Check jitter is non-negative
                if "fetch_retry_jitter_seconds" in settings:
                    jitter = settings["fetch_retry_jitter_seconds"]
                    self.assertIsInstance(jitter, (int, float))
                    self.assertGreaterEqual(jitter, 0)


if __name__ == "__main__":
    unittest.main()
