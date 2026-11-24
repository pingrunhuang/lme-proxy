from __future__ import annotations

from pathlib import Path

import pytest

from config import ConfigError, check_config, is_placeholder_ip, load_feed_file, load_symbol_map


ROOT = Path(__file__).resolve().parents[1]


def test_load_default_yaml():
    feeds = load_feed_file(ROOT / "config" / "lme.yaml", symbol_map_path=ROOT / "config" / "symbols.yaml")
    assert [item.channel_id for item in feeds.feeds] == [112, 614]
    assert feeds.feeds[0].depth == 15
    assert feeds.feeds[0].endpoints[0].group == "224.0.240.13"
    assert feeds.feeds[0].endpoints[0].port == 61000
    assert is_placeholder_ip(feeds.interface_ip)


def test_check_config_live_requires_interface(tmp_path: Path):
    path = ROOT / "config" / "lme.yaml"
    check_config(path, live=False)
    with pytest.raises(ConfigError, match="interface_ip"):
        check_config(path, live=True)


def test_unsupported_channel(tmp_path: Path):
    path = tmp_path / "bad.yaml"
    path.write_text(
        "config_version: 1\nfeeds:\n  - {id: 115, protocol: lmesource_v4, revision: '4.13', channel_id: 115}\n"
    )
    with pytest.raises(ConfigError, match="115"):
        load_feed_file(path)


def test_symbol_map(tmp_path: Path):
    path = tmp_path / "symbols.yaml"
    path.write_text("42: LME:F:NI:3M\n")
    assert load_symbol_map(path) == {42: "LME:F:NI:3M"}
