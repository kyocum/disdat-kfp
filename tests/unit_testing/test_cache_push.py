from disdat import api
import os
from caching_util.cache_push import caching_push
import pytest
from typing import NamedTuple, Callable, List
import inspect
import shutil
import random


s3_path = 's3://disdat-kubeflow-playground'
context_name = 'kfp-caching-plugin-v2-cache_push'
src_dir = 'test_artifacts/generated_data'
unzip_path = 'test_artifacts/saved_data'


@pytest.fixture
def prep_caching_push() -> Callable:
    def wrapper(user_params: dict,
                bundle_name: str) -> NamedTuple('Output', [('bundle_id', str)]):
        disdat_kwargs = {'bundle_name': bundle_name,
                         'context_name': context_name,
                         's3_url': s3_path,
                         'use_verbose': True,
                         'input_src_folder': src_dir + '/inputs',
                         'zip_file_name': src_dir + '/data_cache'}
        return caching_push(user_params, disdat_kwargs)
    return wrapper


def prep_fake_data(variable_list: List) -> dict:
    if os.path.isdir(src_dir):
        os.system('rm -r {}'.format(src_dir))
    fake_data = {}
    for var in variable_list:
        key = 'reserve_disdat_{}'.format(var)
        folder_path = os.path.join(src_dir, 'inputs', var)
        os.makedirs(folder_path, exist_ok=True)
        file_path = os.path.join(folder_path, 'data')
        os.system('touch {}'.format(file_path))
        value = file_path
        fake_data[key] = value
    return fake_data


def data_content_validator(bundle_name: str,
                           bundle_uuid: str,
                           expected_params: dict,
                           expected_fields: list) -> None:
    api.context(context_name)
    api.remote(context_name, remote_context=context_name, remote_url=s3_path)
    api.pull(context_name, uuid=bundle_uuid, localize=True)
    b = api.get(context_name, bundle_name=bundle_name, uuid=bundle_uuid)
    artifact_path = os.path.join(b.local_dir, 'data_cache.zip')
    if len(expected_fields) == 0:
        assert not os.path.isfile(artifact_path), 'bundle should not have ay zip file - ' + artifact_path
    else:
        assert os.path.isfile(artifact_path), 'bundle should not have ay zip file - ' + artifact_path
        shutil.unpack_archive(artifact_path, format='zip', extract_dir=unzip_path)
        for var in expected_fields:
            assert os.path.isfile(os.path.join(unzip_path, var, 'data')), 'output file missing'
    for key in expected_params:
        assert str(expected_params[key]) == b.params[key], 'parameter is corrupted'


def test_simple_artifacts(prep_caching_push):
    variables = ['first', 'second', 'third']
    bundle_name = inspect.currentframe().f_code.co_name
    input_artifact_kwargs = prep_fake_data(variables)
    user_params = {'x': 1, 'y': 2.0, 'z': True}
    user_kwargs = user_params.copy()
    user_kwargs.update(input_artifact_kwargs)
    push_result = prep_caching_push(user_kwargs, bundle_name)
    data_content_validator(bundle_name, push_result.bundle_id,
                           expected_params=user_params, expected_fields=variables)


def test_different_case_artifacts(prep_caching_push):
    variables = ['Output', 'output', 'OUTPUT']
    bundle_name = inspect.currentframe().f_code.co_name
    input_artifact_kwargs = prep_fake_data(variables)
    user_params = {'list': [1, '2', 3.0, [4, 5, 6], True, 'True', -123456789],
                   'dictionary': {'foo': 'bar'}}
    user_kwargs = user_params.copy()
    user_kwargs.update(input_artifact_kwargs)
    push_result = prep_caching_push(user_kwargs, bundle_name)
    data_content_validator(bundle_name, push_result.bundle_id,
                           expected_params=user_params, expected_fields=variables)


def test_no_artifact(prep_caching_push):
    variables = []
    bundle_name = inspect.currentframe().f_code.co_name
    input_artifact_kwargs = prep_fake_data(variables)
    user_params = {'val': random.randint(0, 100)}
    user_kwargs = user_params.copy()
    user_kwargs.update(input_artifact_kwargs)
    push_result = prep_caching_push(user_kwargs, bundle_name)
    data_content_validator(bundle_name, push_result.bundle_id,
                           expected_params=user_params, expected_fields=variables)
