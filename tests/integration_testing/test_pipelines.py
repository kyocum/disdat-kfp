from tests.integration_testing.utils import submit_and_validate
import kfp


def test_file_binary_stream():
    from tests.integration_testing.pipelines import data_as_text_file_pipeline
    zip_name = data_as_text_file_pipeline.__name__.replace('.', '_') + '.tar.gz'
    kfp.compiler.Compiler().compile(data_as_text_file_pipeline.pipeline, zip_name)
    submit_and_validate(zip_name)


def test_named_tuple():
    from tests.integration_testing.pipelines import named_tuple_pipeline
    zip_name = named_tuple_pipeline.__name__.replace('.', '_') + '.tar.gz'
    kfp.compiler.Compiler().compile(named_tuple_pipeline.pipeline, zip_name)
    submit_and_validate(zip_name)


def test_cascading_jobs():
    from tests.integration_testing.pipelines import cascading_pipeline
    zip_name = cascading_pipeline.__name__.replace('.', '_') + '.tar.gz'
    kfp.compiler.Compiler().compile(cascading_pipeline.pipeline, zip_name)
    submit_and_validate(zip_name)


def test_older_cache():
    from tests.integration_testing.pipelines import old_parameters_pipeline
    zip_name = old_parameters_pipeline.__name__.replace('.', '_') + '.tar.gz'
    kfp.compiler.Compiler().compile(old_parameters_pipeline.pipeline, zip_name)
    submit_and_validate(zip_name)


def test_loop_cache():
    from tests.integration_testing.pipelines import loop_pipeline
    zip_name = loop_pipeline.__name__.replace('.', '_') + '.tar.gz'
    kfp.compiler.Compiler().compile(loop_pipeline.pipeline, zip_name)
    submit_and_validate(zip_name, args='--parameter loop=2')


def test_mapper_job():
    from tests.integration_testing.pipelines import map_reduce_pipeline
    zip_name = map_reduce_pipeline.__name__.replace('.', '_') + '.tar.gz'
    kfp.compiler.Compiler().compile(map_reduce_pipeline.pipeline, zip_name)
    submit_and_validate(zip_name)


def test_data_as_file_job():
    from tests.integration_testing.pipelines import data_as_text_file_pipeline
    zip_name = data_as_text_file_pipeline.__name__.replace('.', '_') + '.tar.gz'
    kfp.compiler.Compiler().compile(data_as_text_file_pipeline.pipeline, zip_name)
    submit_and_validate(zip_name)