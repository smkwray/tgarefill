from tgarefill.settings import get_settings


def test_settings_load() -> None:
    settings = get_settings()
    assert "fiscaldata" in settings.data_sources
    assert "series" in settings.fred_series
    assert "weekly" in settings.episode_rules
    assert "attribution" in settings.analysis
