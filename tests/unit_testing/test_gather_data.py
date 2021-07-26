import os.path
import shutil
import inspect
from disdat import api
from caching_util.gather_data import gather_data
import pytest
from typing import NamedTuple, Callable
from tests import config

s3_path = config.UNIT_TEST_S3_BUCKET
context_name = 'kfp-caching-plugin-v2-gather-data'
src_dir = config.ARTIFACT_DIR + '/generated_data'
unzip_path = config.ARTIFACT_DIR + '/saved_data'
output_path = config.ARTIFACT_DIR + '/final_output_data'
zip_file_name = config.ARTIFACT_DIR + '/data_cache'

bundle_name = 'test_gather_data'
var_list = ['Output', 'use_cache', 'bundle_id']

if os.path.isdir('test_artifacts'):
    os.system('rm -r test_artifacts')
os.makedirs('test_artifacts')


@pytest.fixture
def prep_gather_data() -> Callable:

    def wrapper(caching_check_uuid: str,
                caching_push_uuid: str) -> str:

        disdat_kwargs = {'bundle_name': bundle_name,
                         'context_name': context_name,
                         's3_url': s3_path,
                         'use_verbose': True,
                         'caching_push_uuid': caching_push_uuid,
                         'caching_check_uuid': caching_check_uuid,
                         'unzip_path': unzip_path,
                         'output_path': output_path}
        return gather_data({}, disdat_kwargs)
    return wrapper


def generate_fake_data(variable_list: list) -> str:
    if os.path.isdir(src_dir):
        os.system('rm -r {}'.format(src_dir))
    for var in variable_list:
        os.makedirs(src_dir, exist_ok=True)
        os.system('mkdir {}'.format(os.path.join(src_dir, var)))
        file_path = os.path.join(src_dir, var,'data')
        os.system('touch {}'.format(file_path))

    shutil.make_archive(base_name=zip_file_name, format='zip', root_dir=src_dir)
    api.context(context_name)
    api.remote(context_name, remote_context=context_name, remote_url=s3_path)
    with api.Bundle(context_name, name=bundle_name) as b:
        b.add_data(zip_file_name + '.zip')
    api.commit(context_name, bundle_name)
    api.push(context_name, bundle_name)
    return b.uuid


@pytest.fixture(scope='module', autouse=True)
def generate_cached_data():
    return generate_fake_data(var_list)


@pytest.fixture(scope='module', autouse=True)
def generate_latest_data():
    return generate_fake_data(var_list)


def integrity_check():
    """
    Make sure the unzipped folder has the same structure as the mock data
    :return:
    """
    unzipped = os.listdir(output_path)
    assert len(unzipped) == len(var_list), 'folder number not match'
    for var in var_list:
        assert var in unzipped, 'folder missing'


def test_use_cache_push_data(prep_gather_data, generate_cached_data, generate_latest_data):
    # uuid from cache push has higher priority
    caching_check_uuid_mock = generate_cached_data
    caching_push_uuid_mock = generate_latest_data
    uuid = prep_gather_data(caching_check_uuid=caching_check_uuid_mock,
                            caching_push_uuid=caching_push_uuid_mock)
    assert uuid == caching_push_uuid_mock
    integrity_check()


def test_use_cache_push_data_force_rerun(prep_gather_data, generate_cached_data, generate_latest_data):
    # uuid from cache push be used
    caching_check_uuid_mock = generate_cached_data
    caching_push_uuid_mock = generate_latest_data
    uuid = prep_gather_data(caching_check_uuid='',
                            caching_push_uuid=caching_push_uuid_mock)
    assert uuid == caching_push_uuid_mock
    integrity_check()


def test_use_cache_check_data(prep_gather_data, generate_cached_data, generate_latest_data):
    # uuid from cache should be used
    caching_check_uuid_mock = generate_cached_data
    caching_push_uuid_mock = generate_latest_data
    uuid = prep_gather_data(caching_check_uuid=caching_check_uuid_mock,
                            caching_push_uuid='{{tasks.condition-1.outputs.parameters.testing-nametuple-param4}}')
    assert uuid == caching_check_uuid_mock
    integrity_check()
