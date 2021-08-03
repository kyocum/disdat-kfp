import collections, kfp, os
from kfp import dsl, components
from tests import config
from disdat_kfp.caching_wrapper import Caching
from tests.integration_testing.utils import validate_container_execution, validate_container_no_execution
import time


pipeline_name = __file__.split('/')[-1].replace('.py', '')


def add(x: float, y: float) -> float:
    return int(x + y)


def multiply(x: float, y: float) -> float:
    return x * y


def divide(x: float, y: float) -> float:
    return x / y


@dsl.pipeline(
    name=pipeline_name,
    description="test named tuple output"
)
def pipeline():
    # kfp.dsl.get_pipeline_conf().add_op_transformer(mlp_transformer(model="disdatnoop"))

    add_op = components.create_component_from_func(add, base_image=config.BASE_IMAGE)

    multiply_op = components.create_component_from_func(multiply, base_image=config.BASE_IMAGE)

    divide_op = components.create_component_from_func(divide, base_image=config.BASE_IMAGE)

    validate_execution = components.create_component_from_func(validate_container_execution,
                                                               base_image=config.BASE_IMAGE,
                                                               packages_to_install=['disdat'])
    validate_no_execution = components.create_component_from_func(validate_container_no_execution,
                                                                  base_image=config.BASE_IMAGE,
                                                                  packages_to_install=['disdat'])

    caching = Caching(disdat_context=pipeline_name,
                      disdat_repo_s3_url='s3://' + config.S3_BUCKET,
                      force_rerun_pipeline=False, use_verbose=True)

    add_res = caching.enable_caching(add_op, x=10.0, y=5.0, _disdat_force_rerun=True,
                                     _disdat_bundle='add_bundle')

    add_uuid = validate_execution('add_bundle',
                                  context_name=pipeline_name,
                                  s3_url='s3://' + config.S3_BUCKET,
                                  younger_than=time.time()).after(add_res)

    divide_res = caching.enable_caching(divide_op, x=10.0, y=5.0, _disdat_force_rerun=True,
                                        _disdat_bundle='divide_bundle')
    divide_uuid = validate_execution('divide_bundle',
                                     context_name=pipeline_name,
                                     s3_url='s3://' + config.S3_BUCKET,
                                     younger_than=time.time()).after(divide_res)

    multiply_res = caching.enable_caching(multiply_op, x=add_res.output, y=divide_res.output,
                                          _disdat_force_rerun=True, _disdat_bundle='multiply_bundle')

    multiply_uuid = validate_execution('multiply_bundle',
                                       context_name=pipeline_name,
                                       s3_url='s3://' + config.S3_BUCKET,
                                       younger_than=time.time()).after(multiply_res)

    final_res = caching.enable_caching(divide_op, x=multiply_res.output, y=5.0, _disdat_force_rerun=True,
                                       _disdat_bundle='divide_bundle_2')

    final_uuid = validate_execution('divide_bundle_2',
                                    context_name=pipeline_name,
                                    s3_url='s3://' + config.S3_BUCKET,
                                    younger_than=time.time()).after(final_res)

    # re-execution to test caching

    add_res = caching.enable_caching(add_op, x=10.0, y=5.0, _disdat_force_rerun=False,
                                     _disdat_bundle='add_bundle',
                                     _after=[final_uuid])

    validate_no_execution('add_bundle',
                          uuid=add_uuid.output,
                          context_name=pipeline_name,
                          s3_url='s3://' + config.S3_BUCKET).after(add_res)

    divide_res = caching.enable_caching(divide_op, x=10.0, y=5.0, _disdat_force_rerun=False,
                                        _disdat_bundle='divide_bundle',
                                        _after=[final_uuid])

    validate_no_execution('divide_bundle',
                          uuid=divide_uuid.output,
                          context_name=pipeline_name,
                          s3_url='s3://' + config.S3_BUCKET).after(divide_res)

    multiply_res = caching.enable_caching(multiply_op, x=add_res.output, y=divide_res.output,
                                          _disdat_force_rerun=False,
                                          _disdat_bundle='multiply_bundle')

    validate_no_execution('multiply_bundle',
                          uuid=multiply_uuid.output,
                          context_name=pipeline_name,
                          s3_url='s3://' + config.S3_BUCKET).after(multiply_res)

    final_res = caching.enable_caching(divide_op, x=multiply_res.output, y=5.0,
                                       _disdat_force_rerun=False,
                                       _disdat_bundle='divide_bundle_2')

    validate_no_execution('divide_bundle_2',
                          uuid=final_uuid.output,
                          context_name=pipeline_name,
                          s3_url='s3://' + config.S3_BUCKET).after(final_res)
