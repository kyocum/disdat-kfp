import collections, kfp, os
from kfp import dsl, components
from typing import NamedTuple
from caching_util.caching_wrapper import Caching
from tests import config
# from mlp_components.common.transformers import mlp_transformer
from tests.integration_testing.utils import validate_container_execution, validate_container_no_execution
import time


def container(int_param: int = 10,
              float_param: float = 1.0,
              list_param: list = [1]) -> NamedTuple('Output', [('int_out', int),
                                                         ('float_out', float),
                                                         ('string_out', str),
                                                         ('bool_out', bool),
                                                         ('list_out', list)]):
    import collections
    Output = collections.namedtuple('Output', ['int_out', 'float_out', 'string_out', 'bool_out', 'list_out'])
    return Output(1, 2.0, '3', True, [4])


def integrity_check(int_param: int, float_param: float,
                    string_param: str, bool_param: bool,
                    list_param: list) -> bool:
    assert int_param == 1 and float_param == 2.0 and string_param == '3'
    assert bool_param == True and list_param == [4]
    return True


pipeline_name = __file__.split('/')[-1].replace('.py', '')


@dsl.pipeline(
    name=pipeline_name
)
def pipeline():

    container_op = components.create_component_from_func(container, base_image=config.BASE_IMAGE)

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
    res_obj = caching.enable_caching(container_op, int_param=1, float_param=1.0,
                                     list_param=[1], _disdat_force_rerun=True,
                                     _disdat_bundle='name_tuple_producer')

    producer_uuid = validate_execution('name_tuple_producer',
                                       context_name=pipeline_name,
                                       s3_url='s3://' + config.S3_BUCKET,
                                       younger_than=time.time()).after(res_obj)

    status = integrity_check_op(res_obj.outputs['int_out'],
                                res_obj.outputs['float_out'],
                                res_obj.outputs['string_out'],
                                res_obj.outputs['bool_out'],
                                res_obj.outputs['list_out']).after(producer_uuid)

    # run for the second time, use caching
    cached = caching.enable_caching(container_op, int_param=1, float_param=1.0,
                                     list_param=[1], _disdat_force_rerun=False,
                                     _disdat_bundle='name_tuple_producer',
                                    _after=[status])

    status = validate_no_execution('name_tuple_producer',
                                   uuid=producer_uuid.output,
                                   context_name=pipeline_name,
                                   s3_url='s3://' + config.S3_BUCKET).after(cached)

    output_1 = integrity_check_op(cached.outputs['int_out'],
                                  cached.outputs['float_out'],
                                  cached.outputs['string_out'],
                                  cached.outputs['bool_out'],
                                  cached.outputs['list_out']).after(status)
