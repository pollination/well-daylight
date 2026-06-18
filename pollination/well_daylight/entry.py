from pollination_dsl.dag import Inputs, DAG, task, Outputs
from dataclasses import dataclass
from pollination.honeybee_radiance.multiphase import AddApertureGroupBlinds
from pollination.honeybee_radiance.schedule import EPWtoDaylightHours
from pollination.two_phase_daylight_coefficient import TwoPhaseDaylightCoefficientEntryPoint
from pollination.honeybee_radiance_postprocess.well import WellAnnualDaylight

# input/output alias
from pollination.alias.inputs.model import hbjson_model_room_input
from pollination.alias.inputs.wea import epw_input_timestep_annual_check
from pollination.alias.inputs.north import north_input
from pollination.alias.inputs.radiancepar import rad_par_annual_input
from pollination.alias.inputs.grid import grid_filter_input, cpu_count
from pollination.alias.outputs.daylight import well_l01_summary, well_l06_summary, leed_one_shade_transmittance_results

from ._visualization import WellDaylightVisualization


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
        'sensor grids.',
        default=1000, default_local=500,
        spec={'type': 'integer', 'minimum': 1}
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
        alias=hbjson_model_room_input
    )

    epw = Inputs.file(
        description='EPW or Wea file. This must be an hourly weather file with annual '
        'data.',
        extensions=['epw', 'wea'],
        alias=epw_input_timestep_annual_check
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

    @task(template=EPWtoDaylightHours)
    def create_daylight_hours(
        self, epw=epw
    ):
        return [
            {
                'from': EPWtoDaylightHours()._outputs.daylight_hours_csv,
                'to': 'daylight_hours.csv'
            },
            {
                'from': EPWtoDaylightHours()._outputs.daylight_hours_json,
                'to': 'daylight_hours.json'
            },
            {
                'from': EPWtoDaylightHours()._outputs.daylight_hours_wea,
                'to': 'wea.wea'
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
                'to': 'output_model.hbjson'
            }
        ]

    @task(
        template=TwoPhaseDaylightCoefficientEntryPoint,
        needs=[create_daylight_hours, add_aperture_group_blinds]
    )
    def run_two_phase_daylight_coefficient(
            self, north=north, cpu_count=cpu_count, min_sensor_count=min_sensor_count,
            radiance_parameters=radiance_parameters, grid_filter=grid_filter,
            model=add_aperture_group_blinds._outputs.output_model,
            wea=create_daylight_hours._outputs.daylight_hours_wea,
            dtype='float16'
    ):
        pass

    @task(
        template=WellAnnualDaylight,
        needs=[create_daylight_hours, run_two_phase_daylight_coefficient]
    )
    def well_annual_daylight(
        self, folder='results', model='output_model.hbjson',
        daylight_hours=create_daylight_hours._outputs.daylight_hours_csv,
    ):
        return [
            {
                'from': WellAnnualDaylight()._outputs.well_summary_folder,
                'to': 'well_summary'
            }
        ]

    @task(
        template=WellDaylightVisualization,
        needs=[run_two_phase_daylight_coefficient, well_annual_daylight],
        sub_paths={
            'l01_pass_fail': 'ies_lm/pass_fail/L01',
            'l06_pass_fail': 'ies_lm/pass_fail/L06'
        }
    )
    def create_visualization(
        self, model='output_model.hbjson', l01_pass_fail=well_annual_daylight._outputs.well_summary_folder,
        l06_pass_fail=well_annual_daylight._outputs.well_summary_folder
    ):
        return [
            {
                'from': WellDaylightVisualization()._outputs.visualization,
                'to': 'visualization.vsf'
            }
        ]

    output_model = Outputs.file(
        source='output_model.hbjson', description='Model with blinds.'
    )

    visualization = Outputs.file(
        source='visualization.vsf',
        description='Visualization in VisualizationSet format.'
    )

    results = Outputs.folder(
        source='results', description='Folder with raw result files (.ill) that '
        'contain illuminance matrices for each sensor at each timestep of the analysis.'
    )

    l01_summary = Outputs.file(
        description='JSON file containing the number of credits achieved.',
        source='well_summary/l01_well_summary.json', alias=well_l01_summary
    )

    l06_summary = Outputs.file(
        description='JSON file containing the number of credits achieved.',
        source='well_summary/l06_well_summary.json', alias=well_l06_summary
    )

    well_version = Outputs.file(
        description='JSON file containing the WELL version used for the analysis '
        'and the criteria for L01 and L06.',
        source='well_summary/well_version.json'
    )

    dynamic_schedule = Outputs.file(
        description='JSON file containing the dynamic schedules.',
        source='well_summary/ies_lm/l06_ies_lm_summary/states_schedule.json',
        alias=leed_one_shade_transmittance_results
    )
