from disdat import api
from caching_util.cache_check import caching_check
import pytest
from typing import NamedTuple, Callable


s3_path = 's3://disdat-kubeflow-playground'
context_name = 'kfp-caching-plugin-v2-cache-check'


@pytest.fixture
def prep_caching_check() -> Callable:
    def wrapper(user_params: dict,
                bundle_name: str,
                force_rerun: bool = False) -> NamedTuple('Outputs', [('use_cache', bool), ('bundle_id', str)]):
        user_kwargs = user_params
        disdat_kwargs = {'bundle_name': bundle_name,
                         'context_name': context_name,
                         's3_url': s3_path,
                         'use_verbose': True,
                         'force_rerun': force_rerun}
        return caching_check(user_params, disdat_kwargs)
    return wrapper


def create_mock_data(bundle_name: str,
                     user_params: dict):
    api.context(context_name)
    api.remote(context_name, remote_context=context_name, remote_url=s3_path)
    component_signature = {k: str(v) for k, v in user_params.items()}
    proc_name = api.Bundle.calc_default_processing_name(
        bundle_name, component_signature, dep_proc_ids={})
    with api.Bundle(context_name, name=bundle_name, processing_name=proc_name) as b:
        b.add_params(component_signature)                       # local_path will be replaced by S3 by Disdat
    api.commit(context_name, bundle_name)
    api.push(context_name, bundle_name)                 # save the bundle to S3
    return b.uuid                                       # return the bundle uuid


def test_pull_forced_rerun(prep_caching_check):
    user_params = {'x': 1, 'y': 2.0, 'z': True}
    result = prep_caching_check(user_params, bundle_name='fake name', force_rerun=True)
    assert result.use_cache == False
    assert result.bundle_id == ''


def test_pull_simple_param_use_cache(prep_caching_check):
    user_params = {'x': 1, 'y': 2.0, 'z': True}
    bundle_name = test_pull_simple_param_use_cache.__name__
    uuid = create_mock_data(bundle_name, user_params=user_params)
    result = prep_caching_check(user_params, bundle_name=bundle_name, force_rerun=False)
    assert result.use_cache == True
    assert result.bundle_id == uuid


def test_pull_complex_data_use_cache(prep_caching_check):
    user_params = {'list': [1, '2', 3.0, [4, 5, 6], True, 'True', -123456789],
                   'dictionary': {'foo': 'bar'}}
    bundle_name = test_pull_complex_data_use_cache.__name__
    uuid = create_mock_data(bundle_name, user_params=user_params)
    result = prep_caching_check(user_params, bundle_name=bundle_name, force_rerun=False)
    assert result.use_cache == True
    assert result.bundle_id == uuid


def test_pull_no_param_use_cache(prep_caching_check):
    user_params = {}
    bundle_name = test_pull_no_param_use_cache.__name__
    uuid = create_mock_data(bundle_name, user_params=user_params)
    result = prep_caching_check(user_params, bundle_name=bundle_name, force_rerun=False)
    assert result.use_cache == True
    assert result.bundle_id == uuid


def test_pull_parameter_mismatch(prep_caching_check):
    user_params = {'list': [1, '2', 3.0, [4, 5, 6], True, 'True', -123456789],
                   'dictionary': {'foo': 'bar'}}
    bundle_name = test_pull_parameter_mismatch.__name__
    uuid = create_mock_data(bundle_name, user_params=user_params)

    user_params['dictionary']['new_data'] = 'new'
    result = prep_caching_check(user_params, bundle_name=bundle_name, force_rerun=False)
    assert result.use_cache == False
    assert result.bundle_id == ''


def test_pull_older_data(prep_caching_check):
    user_params_old = {'x': 1, 'y': 2.0, 'z': True}
    bundle_name = test_pull_older_data.__name__
    uuid_old = create_mock_data(bundle_name, user_params=user_params_old)
    user_params_new = {'x': 2, 'y': 2.0, 'z': True}
    uuid_new = create_mock_data(bundle_name, user_params=user_params_new)

    result = prep_caching_check(user_params_old, bundle_name=bundle_name, force_rerun=False)
    assert result.use_cache == True
    assert result.bundle_id == uuid_old
    assert result.bundle_id != uuid_new


def test_pull_no_bundle(prep_caching_check):
    user_params = {'x': 1, 'y': 2.0, 'z': True}
    bundle_name = 'no such bundle exists'
    result = prep_caching_check(user_params, bundle_name=bundle_name, force_rerun=False)
    assert result.use_cache == False
    assert result.bundle_id == ''