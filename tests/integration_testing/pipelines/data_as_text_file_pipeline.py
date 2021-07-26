import kfp
from kfp import dsl, components
from caching_util.caching_wrapper import Caching
from tests import config
from tests.integration_testing.utils import validate_container_execution, validate_container_no_execution
import time


def file_producer(word_count: int, data_path: components.OutputTextFile(str)) -> int:
    data_path.write('*' * word_count)
    return word_count


def file_consumer(word_count: int,
                  data_handle: components.InputTextFile(str)) -> float:
    import time
    data = data_handle.read()
    assert len(data) == word_count, 'data length does not match'
    return time.time()


pipeline_name = __file__.split('/')[-1].replace('.py', '')


@dsl.pipeline(
    name=pipeline_name,
    description="input output path pipeline"
)
def pipeline():
    # kfp.dsl.get_pipeline_conf().add_op_transformer(mlp_transformer(model="disdatnoop"))

    caching = Caching(disdat_context=pipeline_name,
                      disdat_repo_s3_url='s3://' + config.S3_BUCKET,
                      force_rerun_pipeline=False, use_verbose=True)

    producer_op = components.create_component_from_func(file_producer,
                                                        base_image=config.BASE_IMAGE)
    consumer_op = components.create_component_from_func(file_consumer,
                                                        base_image=config.BASE_IMAGE)

    validate_execution = components.create_component_from_func(validate_container_execution,
                                                               base_image=config.BASE_IMAGE,
                                                               packages_to_install=['disdat'])
    validate_no_execution = components.create_component_from_func(validate_container_no_execution,
                                                                  base_image=config.BASE_IMAGE,
                                                                  packages_to_install=['disdat'])

    # force rerun, testing data flow
    result = caching.enable_caching(producer_op, word_count=10000,
                                    _disdat_force_rerun=True, _disdat_bundle='file_producer_bundle')

    producer_uuid = validate_execution('file_producer_bundle',
                                       context_name=pipeline_name,
                                       s3_url='s3://' + config.S3_BUCKET,
                                       younger_than=time.time()).after(result)

    result = caching.enable_caching(consumer_op, word_count=result.outputs['Output'],
                                    data_handle=result.outputs['data'],
                                    _disdat_force_rerun=True, _disdat_bundle='file_consumer_bundle',
                                    _after=[producer_uuid])

    consumer_uuid = validate_execution('file_consumer_bundle',
                                       context_name=pipeline_name,
                                       s3_url='s3://' + config.S3_BUCKET,
                                       younger_than=time.time()).after(result)

    # rerun again, testing caching mechanism
    result= caching.enable_caching(producer_op, word_count=10000,
                                   _disdat_force_rerun=False, _disdat_bundle='file_producer_bundle',
                                   _after=[consumer_uuid]
                                   )

    status = validate_no_execution('file_producer_bundle',
                                   uuid=producer_uuid.output,
                                   context_name=pipeline_name,
                                   s3_url='s3://' + config.S3_BUCKET).after(result)

    result = caching.enable_caching(consumer_op, word_count=result.outputs['Output'],
                                    data_handle=result.outputs['data'],
                                    _disdat_force_rerun=False,
                                    _disdat_bundle='file_consumer_bundle',
                                    _after=[status])

    validate_no_execution('file_consumer_bundle',
                          uuid=consumer_uuid.output,
                          context_name=pipeline_name,
                          s3_url='s3://' + config.S3_BUCKET).after(result)

