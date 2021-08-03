from kfp import dsl, components
from tests import config
import kfp, os
from tests.integration_testing.utils import validate_container_execution, validate_container_no_execution
import time
from disdat_kfp.caching_wrapper import Caching


def iteration_down(x: int) -> int:
    return x - 1


iteration_down_op = components.create_component_from_func(iteration_down, base_image=config.BASE_IMAGE)

validate_execution = components.create_component_from_func(validate_container_execution,
                                                           base_image=config.BASE_IMAGE,
                                                           packages_to_install=['disdat'])
validate_no_execution = components.create_component_from_func(validate_container_no_execution,
                                                              base_image=config.BASE_IMAGE,
                                                              packages_to_install=['disdat'])

pipeline_name = __file__.split('/')[-1].replace('.py', '')

caching = Caching(disdat_context=pipeline_name,
                  disdat_repo_s3_url='s3://' + config.S3_BUCKET,
                  force_rerun_pipeline=False, use_verbose=True)


@dsl.graph_component
def for_loop_component_no_cache(number):
    itr = caching.enable_caching(iteration_down_op, x=number,
                                 _disdat_bundle='itr_bundle',
                                 _disdat_force_rerun=True)
    uuid = validate_execution('itr_bundle',
                              context_name=pipeline_name,
                              s3_url='s3://' + config.S3_BUCKET,
                              younger_than=time.time()).after(itr)

    itr = caching.enable_caching(iteration_down_op, x=number,
                                 _disdat_bundle='itr_bundle',
                                 _disdat_force_rerun=False,
                                 _after=[uuid])
    validate_no_execution('itr_bundle',
                          uuid=uuid.output,
                          context_name=pipeline_name,
                          s3_url='s3://' + config.S3_BUCKET
                          ).after(itr)

    with dsl.Condition(itr.output >= 0):
        uuid = for_loop_component_no_cache(itr.output)


@dsl.pipeline(
    name="loop pipeline"
)
def pipeline():
    loop_num = dsl.PipelineParam(name='loop', value="2", param_type="Integer")
    op = for_loop_component_no_cache(loop_num)
