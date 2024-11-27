from pollination_dsl.dag import Inputs, DAG, task, Outputs
from dataclasses import dataclass
from pollination.honeybee_radiance.multiphase import AddApertureGroupBlinds
from pollination.two_phase_daylight_coefficient import TwoPhaseDaylightCoefficientEntryPoint
from pollination.honeybee_radiance_postprocess.well import WellAnnualDaylight

# input/output alias
from pollination.alias.inputs.model import hbjson_model_grid_input
from pollination.alias.inputs.north import north_input
from pollination.alias.inputs.radiancepar import rad_par_annual_input
from pollination.alias.inputs.grid import grid_filter_input, \
    min_sensor_count_input, cpu_count

from ._process_epw import WellDaylightProcessEPW


@dataclass
class WellDaylightEntryPoint(DAG):
    """WELL daylight entry point."""

    # inputs
    north = Inputs.float(
        default=0,
        description='A number between -360 and 360 for the counterclockwise '
        'difference between the North and the positive Y-axis in degrees. This '
        'can also be a Vector for the direction to North. (Default: 0).',
        spec={'type': 'number', 'minimum': -360, 'maximum': 360},
        alias=north_input
    )

    cpu_count = Inputs.int(
        default=50,
        description='The maximum number of CPUs for parallel execution. This will be '
        'used to determine the number of sensors run by each worker.',
        spec={'type': 'integer', 'minimum': 1},
        alias=cpu_count
    )

    min_sensor_count = Inputs.int(
        description='The minimum number of sensors in each sensor grid after '
        'redistributing the sensors based on cpu_count. This value takes '
        'precedence over the cpu_count and can be used to ensure that '
        'the parallelization does not result in generating unnecessarily small '
        'sensor grids. The default value is set to 1, which means that the '
        'cpu_count is always respected.', default=500,
        spec={'type': 'integer', 'minimum': 1},
        alias=min_sensor_count_input
    )

    radiance_parameters = Inputs.str(
        description='The radiance parameters for ray tracing.',
        default='-ab 2 -ad 5000 -lw 2e-05 -dr 0',
        alias=rad_par_annual_input
    )

    grid_filter = Inputs.str(
        description='Text for a grid identifier or a pattern to filter the sensor grids '
        'of the model that are simulated. For instance, first_floor_* will simulate '
        'only the sensor grids that have an identifier that starts with '
        'first_floor_. By default, all grids in the model will be simulated.',
        default='*',
        alias=grid_filter_input
    )

    model = Inputs.file(
        description='A Honeybee Model JSON file (HBJSON) or a Model pkl (HBpkl) file. '
        'This can also be a zipped version of a Radiance folder, in which case this '
        'recipe will simply unzip the file and simulate it as-is.',
        extensions=['json', 'hbjson', 'pkl', 'hbpkl', 'zip'],
        alias=hbjson_model_grid_input
    )

    epw = Inputs.file(
        description='EPW file.',
        extensions=['epw']
    )

    diffuse_transmission = Inputs.float(
        default=0.05,
        description='Diffuse transmission of the aperture group blinds. Default '
        'is 0.05 (5%).',
        spec={'type': 'number', 'minimum': 0.0001, 'maximum': 1}
    )

    specular_transmission = Inputs.float(
        default=0.0001,
        description='Specular transmission of the aperture group blinds. Default '
        'is 0 (0%).',
        spec={'type': 'number', 'minimum': 0.0001, 'maximum': 1}
    )

    @task(template=WellDaylightProcessEPW)
    def well_daylight_process_epw(
        self, epw=epw
    ):
        return [
            {
                'from': WellDaylightProcessEPW()._outputs.wea,
                'to': 'wea.wea'
            },
            {
                'from': WellDaylightProcessEPW()._outputs.daylight_hours,
                'to': 'daylight_hours.csv'
            }
        ]

    @task(template=AddApertureGroupBlinds)
    def add_aperture_group_blinds(
        self, model=model, diffuse_transmission=diffuse_transmission,
        specular_transmission=specular_transmission
    ):
        return [
            {
                'from': AddApertureGroupBlinds()._outputs.output_model,
                'to': 'model_blinds.hbjson'
            }
        ]

    @task(
        template=TwoPhaseDaylightCoefficientEntryPoint,
        needs=[well_daylight_process_epw, add_aperture_group_blinds]
    )
    def run_two_phase_daylight_coefficient(
            self, north=north, cpu_count=cpu_count, min_sensor_count=min_sensor_count,
            radiance_parameters=radiance_parameters, grid_filter=grid_filter,
            model=add_aperture_group_blinds._outputs.output_model,
            wea=well_daylight_process_epw._outputs.wea
    ):
        pass

    @task(
        template=WellAnnualDaylight,
        needs=[well_daylight_process_epw, run_two_phase_daylight_coefficient]
    )
    def well_annual_daylight(
        self, folder='results', model=model,
        daylight_hours=well_daylight_process_epw._outputs.daylight_hours,
    ):
        return [
            {
                'from': WellAnnualDaylight()._outputs.well_summary_folder,
                'to': 'well_summary'
            },
            {
                'from': WellAnnualDaylight()._outputs.l01_well_summary,
                'to': 'l01_well_summary.json'
            },
            {
                'from': WellAnnualDaylight()._outputs.l06_well_summary,
                'to': 'l06_well_summary.json'
            }
        ]

    results = Outputs.folder(
        source='results', description='Folder with raw result files (.ill) that '
        'contain illuminance matrices for each sensor at each timestep of the analysis.'
    )

    l01_summary = Outputs.file(
        description='JSON file containing the number of credits achieved.',
        source='l01_well_summary.json',
    )

    l06_summary = Outputs.file(
        description='JSON file containing the number of credits achieved.',
        source='l06_well_summary.json',
    )
