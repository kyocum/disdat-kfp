import collections, kfp, os
from kfp import dsl, components
from disdat_kfp.caching_wrapper import Caching
from tests import config
from mlp_components.common.transformers import mlp_transformer
from tests.integration_testing.utils import validate_container_execution, validate_container_no_execution
import time


def scalar_container(int_param: int) -> bool:
    # make sure the parameters are passed in correctly
    assert int_param == 10
    return True


pipeline_name = __file__.split('/')[-1].replace('.py', '')


@dsl.pipeline(
    name=pipeline_name,
    description="test scalar output"
)
def pipeline():
    kfp.dsl.get_pipeline_conf().add_op_transformer(mlp_transformer(model="disdatnoop"))

    container_op = components.create_component_from_func(scalar_container,
                                                         base_image=config.BASE_IMAGE)

    validate_execution = components.create_component_from_func(validate_container_execution,
                                                               base_image=config.BASE_IMAGE,
                                                               packages_to_install=['disdat'])

    validate_no_execution = components.create_component_from_func(validate_container_no_execution,
                                                                  base_image=config.BASE_IMAGE,
                                                                  packages_to_install=['disdat'])

    caching = Caching(disdat_context=pipeline_name,
                      disdat_repo_s3_url='s3://' + config.S3_BUCKET,
                      force_rerun_pipeline=False, use_verbose=True)
    # run for the first time
    res_obj = caching.enable_caching(container_op,
                                     int_param=10,
                                     _disdat_force_rerun=True,
                                     _disdat_bundle='scalar_producer_bundle')

    producer_uuid = validate_execution('scalar_producer_bundle',
                                       context_name=pipeline_name,
                                       s3_url='s3://' + config.S3_BUCKET,
                                       younger_than=time.time()).after(res_obj)

    # run for the second time, use caching
    cached = caching.enable_caching(container_op,
                                    int_param=10,
                                    _after=[producer_uuid],
                                    _disdat_force_rerun=False,
                                    _disdat_bundle='scalar_producer_bundle')

    status = validate_no_execution('scalar_producer_bundle',
                                   uuid=producer_uuid.output,
                                   context_name=pipeline_name,
                                   s3_url='s3://' + config.S3_BUCKET).after(cached)


