import collections, kfp, os
from kfp import dsl, components
from caching_util.caching_wrapper import Caching
from tests import config
# from mlp_components.common.transformers import mlp_transformer
from tests.integration_testing.utils import validate_container_execution, validate_container_no_execution
import time


def user_container(timestamp: int) -> int:
    print(timestamp)
    return timestamp


def integrity_check(timestamp: int, expected: int) -> bool:
    assert timestamp == expected, 'returned value is wrong'
    return True


pipeline_name = __file__.split('/')[-1].replace('.py', '')


@dsl.pipeline(
    name=pipeline_name,
)
def pipeline():
    # kfp.dsl.get_pipeline_conf().add_op_transformer(mlp_transformer(model="disdatnoop"))

    container_op = components.create_component_from_func(user_container, base_image=config.BASE_IMAGE)
    integrity_check_op = components.create_component_from_func(integrity_check, base_image=config.BASE_IMAGE)

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
    run_1 = caching.enable_caching(container_op,
                                   timestamp=1,
                                   _disdat_force_rerun=True,
                                   _disdat_bundle='producer_bundle')

    validate_execution('producer_bundle',
                       context_name=pipeline_name,
                       s3_url='s3://' + config.S3_BUCKET,
                       younger_than=time.time()).after(run_1)

    run_2 = caching.enable_caching(container_op, timestamp=2,
                                   _disdat_force_rerun=True,
                                   _disdat_bundle='producer_bundle',
                                   _after=[run_1])

    uuid = validate_execution('producer_bundle',
                               context_name=pipeline_name,
                               s3_url='s3://' + config.S3_BUCKET,
                               younger_than=time.time()).after(run_2)

    run_3 = caching.enable_caching(container_op, timestamp=1,
                                   _disdat_force_rerun=False,
                                   _disdat_bundle='producer_bundle',
                                   _after=[uuid])

    validate_no_execution('producer_bundle',
                           uuid=uuid.output,
                           context_name=pipeline_name,
                           s3_url='s3://' + config.S3_BUCKET).after(run_3)

    integrity_check_op(run_3.outputs['Output'], 1)

