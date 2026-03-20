from tgarefill.analytics.eras import assign_era


def test_assign_era_boundaries() -> None:
    assert assign_era(2008) == "2008-13"
    assert assign_era(2009) == "2008-13"
    assert assign_era(2013) == "2008-13"
    assert assign_era(2014) == "2014-19"
    assert assign_era(2022) == "2020-22"
    assert assign_era(2023) == "2023-26"
