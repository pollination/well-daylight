from pollination_dsl.dag import Inputs, GroupedDAG, task, Outputs
from dataclasses import dataclass
from pollination.honeybee_radiance.schedule import EPWtoDaylightHours
from pollination.ladybug.translate import EpwToWea


@dataclass
class WellDaylightProcessEPW(GroupedDAG):
    """DAG to process the EPW file."""

    # inputs
    epw = Inputs.file(
        description='EPW file.',
        extensions=['epw']
    )

    @task(template=EPWtoDaylightHours)
    def create_daylight_hours(
        self, epw=epw
    ):
        return [
            {
                'from': EPWtoDaylightHours()._outputs.daylight_hours,
                'to': 'daylight_hours.csv'
            }
        ]

    @task(template=EpwToWea)
    def create_wea(
        self, epw=epw
    ):
        return [
            {
                'from': EpwToWea()._outputs.wea,
                'to': 'wea.wea'
            }
        ]

    wea = Outputs.file(
        source='wea.wea', description='A wea file generated from the input epw.'
    )

    daylight_hours = Outputs.file(
        source='daylight_hours.csv', description='Path to daylight hours '
        'schedule.'
    )
