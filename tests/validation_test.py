from pollination.well_daylight.entry import WellDaylightEntryPoint
from queenbee.recipe.dag import DAG


def test_well_daylight():
    recipe = WellDaylightEntryPoint().queenbee
    assert recipe.name == 'well-daylight-entry-point'
    assert isinstance(recipe, DAG)
