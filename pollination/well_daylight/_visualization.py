from dataclasses import dataclass
from pollination_dsl.dag import Inputs, GroupedDAG, task, Outputs
from pollination.honeybee_display.translate import ModelToVis
from pollination.path.copy import CopyFolder
from pollination.honeybee_radiance_postprocess.well import WellDaylightVisMetadata


@dataclass
class WellDaylightVisualization(GroupedDAG):
    """Create visualization."""

    # inputs
    model = Inputs.file(
        description='Input Honeybee model.',
        extensions=['json', 'hbjson', 'pkl', 'hbpkl', 'zip']
    )

    l01_pass_fail = Inputs.folder(
        description='L01 pass/fail results.',
        path='results/L01'
    )

    l06_pass_fail = Inputs.folder(
        description='L06 pass/fail results.',
        path='results/L06'
    )

    @task(template=CopyFolder)
    def copy_l01_pass_fail(self, src=l01_pass_fail):
        return [
            {
                'from': CopyFolder()._outputs.dst,
                'to': 'visualization/L01'
            }
        ]

    @task(template=CopyFolder)
    def copy_l06_pass_fail(self, src=l06_pass_fail):
        return [
            {
                'from': CopyFolder()._outputs.dst,
                'to': 'visualization/L06'
            }
        ]

    @task(
        template=WellDaylightVisMetadata,
    )
    def create_vis_metadata(self):
        return [
            {
                'from': WellDaylightVisMetadata()._outputs.vis_metadata_folder,
                'to': 'visualization'
            }
        ]

    @task(
        template=ModelToVis,
        needs=[copy_l01_pass_fail, copy_l06_pass_fail, create_vis_metadata]
    )
    def create_vsf(
        self, model=model, grid_data='visualization',
        active_grid_data='L06', output_format='vsf'
    ):
        return [
            {
                'from': ModelToVis()._outputs.output_file,
                'to': 'visualization.vsf'
            }
        ]

    visualization = Outputs.file(
        source='visualization.vsf',
        description='Visualization in VisualizationSet format.'
    )
