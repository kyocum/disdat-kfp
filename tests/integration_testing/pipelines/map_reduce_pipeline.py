import kfp
from kfp import components
from tests import config
from tests.integration_testing.utils import version_checker
from caching_util.caching_wrapper import Caching


def generator() -> list:
    return [1, 2, 3]


def mapper(num: int) -> int:
    return num * -1


pipeline_name = 'map_pipeline'


@kfp.dsl.pipeline(
    name=pipeline_name
)
def pipeline():
    generator_comp = components.create_component_from_func(generator, base_image=config.BASE_IMAGE)
    mapper_comp = components.create_component_from_func(mapper, base_image=config.BASE_IMAGE)

    checker = components.create_component_from_func(version_checker,
                                                    base_image=config.BASE_IMAGE,
                                                    packages_to_install=['disdat'])

    caching = Caching(disdat_context=pipeline_name,
                      disdat_repo_s3_url='s3://' + config.S3_BUCKET,
                      force_rerun_pipeline=False, use_verbose=True)

    data = generator_comp()
    uuid = checker(pipeline_name,
                   bundle_name='mapper_bundle',
                   s3_url='s3://' + config.S3_BUCKET).after(data)

    with kfp.dsl.ParallelFor(loop_args=data.output) as subproblem:
        map_results = caching.enable_caching(mapper_comp, num=subproblem,
                                             _disdat_force_rerun=True,
                                             _disdat_bundle='mapper_bundle').after(uuid)

    res = checker(pipeline_name,
                  bundle_name='mapper_bundle',
                  s3_url='s3://' + config.S3_BUCKET,
                  uuid=uuid.output,
                  gap=3,check=True).after(map_results)

    uuid = checker(pipeline_name,
                   bundle_name='mapper_bundle',
                   s3_url='s3://' + config.S3_BUCKET).after(res)

    with kfp.dsl.ParallelFor(loop_args=data.output) as subproblem:
        map_results = caching.enable_caching(mapper_comp, num=subproblem,
                                             _disdat_force_rerun=False,
                                             _disdat_bundle='mapper_bundle').after(uuid)
    checker(pipeline_name,
            bundle_name='mapper_bundle',
            s3_url='s3://' + config.S3_BUCKET,
            uuid=uuid.output,
            gap=0,
            check=True).after(map_results)

