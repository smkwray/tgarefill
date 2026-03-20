from __future__ import annotations

ERA_ORDER = ["2008-13", "2014-19", "2020-22", "2023-26"]


def assign_era(year: int) -> str:
    if year <= 2013:
        return "2008-13"
    if year <= 2019:
        return "2014-19"
    if year <= 2022:
        return "2020-22"
    return "2023-26"
