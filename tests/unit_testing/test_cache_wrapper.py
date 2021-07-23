import json
import inspect
from tests import config
from caching_util.caching_wrapper import Caching
from typing import NamedTuple
from kfp import components

disdat_kwargs = {'bundle_name': 'str', 'context_name': 'str',
                 's3_url': 'str','force_rerun': 'bool', 'use_verbose': 'bool'}

disdat_params = {'bundle_name': 'test_bundle', 'context_name': 'text_context',
                 's3_url': 'test_url', 'force_rerun': True, 'use_verbose': False}


def core_code_str(user_kwargs: dict, disdat_kwargs: dict) -> str:
    import json
    return json.dumps({'user_kwargs': user_kwargs,
                       'disdat_kwargs': disdat_kwargs})


def core_core_named_tuple(user_kwargs: dict, disdat_kwargs: dict) -> NamedTuple('Output', [('user_kwargs', dict),
                                                                                           ('disdat_kwargs', dict)]):
    import collections
    Output = collections.namedtuple('Output', ['user_kwargs', 'disdat_kwargs'])
    return Output(user_kwargs, disdat_kwargs)


def check_signature(signature, container, push_mode=True):
    for param in container.component_spec.inputs:
        if push_mode:
            if isinstance(signature.parameters[param.name].annotation, components.InputPath):
                assert signature.parameters[param.name].annotation.type is None
            else:
                assert signature.parameters[param.name].annotation == param.type
        else:
            assert param.name not in signature.parameters
    for param in container.component_spec.outputs:
        if push_mode:
            assert (signature.parameters['reserve_disdat_' + param.name].annotation.type == param.type)
        if not push_mode:
            assert (signature.parameters[param.name].annotation.type == param.type)
    for param in disdat_kwargs:
        assert signature.parameters[param].annotation == disdat_kwargs[param]


def test_simple_container_signature_return_str_check_mode():
    def simple_container(param1: str, param2: float, param3: bool) -> int:
        return 1
    func_name = inspect.currentframe().f_code.co_name
    container = components.create_component_from_func(simple_container, base_image=config.BASE_IMAGE)
    gen_code = Caching._code_wrapper_generator(user_kwargs=container.component_spec.inputs,
                                               disdat_kwargs=disdat_kwargs,
                                               core_code=core_code_str,
                                               return_signature='''str''',
                                               generated_func_name='generated_' + func_name)
    user_params = {'param1': '1', 'param2': 2.0, 'param3': True}
    # check by calling
    result = json.loads(gen_code(**user_params, **disdat_params))
    assert result['user_kwargs'] == user_params
    assert result['disdat_kwargs'] == disdat_params


def test_simple_container_signature_return_str_push_mode():
    def simple_container(param1: str, param2: float, param3: bool) -> int:
        return 1
    func_name = inspect.currentframe().f_code.co_name
    container = components.create_component_from_func(simple_container, base_image=config.BASE_IMAGE)
    gen_code = Caching._code_wrapper_generator(user_kwargs=container.component_spec.inputs,
                                               disdat_kwargs=disdat_kwargs,
                                               input_artifact_list=container.component_spec.outputs,
                                               core_code=core_code_str,
                                               return_signature='''str''',
                                               generated_func_name='generated_' + func_name)
    user_params = {'param1': '1', 'param2': 2.0, 'param3': True}
    artifact = {'reserve_disdat_Output': 1}
    # # check by calling
    user_params.update(artifact)
    result = json.loads(gen_code(**user_params, **disdat_params))
    assert result['user_kwargs'] == user_params
    assert result['disdat_kwargs'] == disdat_params


def test_simple_container_signature_return_str_gather_mode():
    def simple_container(param1: str, param2: float, param3: bool) -> int:
        return 1
    func_name = inspect.currentframe().f_code.co_name
    container = components.create_component_from_func(simple_container, base_image=config.BASE_IMAGE)
    gen_code = Caching._code_wrapper_generator(user_kwargs=[],
                                               disdat_kwargs=disdat_kwargs,
                                               output_artifact_list=container.component_spec.outputs,
                                               core_code=core_code_str,
                                               return_signature='''str''',
                                               generated_func_name='generated_' + func_name)
    artifact = {'Output': 1}
    check_signature(inspect.signature(gen_code), container, push_mode=False)
    # check by calling
    result = json.loads(gen_code(**artifact, **disdat_params))
    assert result['user_kwargs'] == artifact
    assert result['disdat_kwargs'] == disdat_params


def test_simple_container_signature_return_tuple_push_mode():
    def simple_container(param1: str, param2: float, param3: bool) -> int:
        return 1
    func_name = inspect.currentframe().f_code.co_name
    container = components.create_component_from_func(simple_container, base_image=config.BASE_IMAGE)
    gen_code = Caching._code_wrapper_generator(user_kwargs=container.component_spec.inputs,
                                               disdat_kwargs=disdat_kwargs,
                                               core_code=core_core_named_tuple,
                                               input_artifact_list=container.component_spec.outputs,
                                               return_signature='''NamedTuple('Output', [('user_kwargs', dict),
                                               ('disdat_kwargs', dict)])''',
                                               generated_func_name='generated_' + func_name)
    user_params = {'param1': '5', 'param2': 10.0, 'param3': False}

    # check signature
    signature = inspect.signature(gen_code)
    check_signature(signature, container, push_mode=True)
    # check return values
    artifacts = {'reserve_disdat_{}'.format(key): '' for key in ['Output']}
    user_params.update(artifacts)
    result = gen_code(**user_params, **disdat_params)
    assert result.user_kwargs == user_params
    assert result.disdat_kwargs == disdat_params


def test_named_tuple_return_str_push_mode():
    def namedtuple_container(param1: str, param2: float, param3: bool) \
        -> NamedTuple('Output', [('val1', str), ('val2', int), ('val3', bool)]):
        import collections
        Output = collections.namedtuple('Output', ['val1', 'val2', 'val3'])
        return Output('1', 2, True)
    func_name = inspect.currentframe().f_code.co_name
    container = components.create_component_from_func(namedtuple_container, base_image=config.BASE_IMAGE)
    gen_code = Caching._code_wrapper_generator(user_kwargs=container.component_spec.inputs,
                                               disdat_kwargs=disdat_kwargs,
                                               input_artifact_list=container.component_spec.outputs,
                                               core_code=core_code_str,
                                               return_signature='''str''',
                                               generated_func_name='generated_' + func_name)
    user_params = {'param1': '1', 'param2': 2.0, 'param3': True}
    artifacts = {'reserve_disdat_{}'.format(key): '' for key in ['val1', 'val2', 'val3']}
    signature = inspect.signature(gen_code)
    # check signature
    check_signature(signature, container, push_mode=True)
    # check functionality
    user_params.update(artifacts)
    result = json.loads(gen_code(**user_params, **disdat_params))
    assert result['user_kwargs'] == user_params
    assert result['disdat_kwargs'] == disdat_params


def test_named_tuple_return_str_gather_mode():
    def namedtuple_container(param1: str, param2: float, param3: bool) \
        -> NamedTuple('Output', [('val1', str), ('val2', int), ('val3', bool)]):
        import collections
        Output = collections.namedtuple('Output', ['val1', 'val2', 'val3'])
        return Output('1', 2, True)
    func_name = inspect.currentframe().f_code.co_name
    container = components.create_component_from_func(namedtuple_container, base_image=config.BASE_IMAGE)
    gen_code = Caching._code_wrapper_generator(user_kwargs=[],
                                               disdat_kwargs=disdat_kwargs,
                                               output_artifact_list=container.component_spec.outputs,
                                               core_code=core_code_str,
                                               return_signature='''str''',
                                               generated_func_name='generated_' + func_name)
    user_params = {'param1': '1', 'param2': 2.0, 'param3': True}
    artifacts = {key: '' for key in ['val1', 'val2', 'val3']}
    signature = inspect.signature(gen_code)
    # check signature
    check_signature(signature, container, push_mode=False)
    # check functionality
    user_params.update(artifacts)
    result = json.loads(gen_code(**artifacts, **disdat_params))
    assert result['user_kwargs'] == artifacts
    assert result['disdat_kwargs'] == disdat_params


def test_complex_input_output_push_mode():
    def namedtuple_container(param1: dict, param2: list, param3: bool,
                             param4: float, param5: components.InputPath(str),
                             param6: components.OutputPath(float)) -> bool:
        return True

    func_name = inspect.currentframe().f_code.co_name
    container = components.create_component_from_func(namedtuple_container, base_image=config.BASE_IMAGE)
    gen_code = Caching._code_wrapper_generator(user_kwargs=container.component_spec.inputs,
                                               disdat_kwargs=disdat_kwargs,
                                               input_artifact_list=container.component_spec.outputs,
                                               core_code=core_code_str,
                                               return_signature='''str''',
                                               generated_func_name='generated_' + func_name)
    signature = inspect.signature(gen_code)
    check_signature(signature, container, push_mode=True)


def test_complex_input_output_gather_mode():

    def namedtuple_container(param1: dict, param2: list, param3: bool,
                             param4: float, param5: components.InputPath(str),
                             param6: components.OutputPath(float)) -> bool:
        return True

    func_name = inspect.currentframe().f_code.co_name
    container = components.create_component_from_func(namedtuple_container, base_image=config.BASE_IMAGE)
    gen_code = Caching._code_wrapper_generator(user_kwargs=[],
                                               disdat_kwargs=disdat_kwargs,
                                               output_artifact_list=container.component_spec.outputs,
                                               core_code=core_code_str,
                                               return_signature='''str''',
                                               generated_func_name='generated_' + func_name)
    signature = inspect.signature(gen_code)
    check_signature(signature, container, push_mode=False)
    artifacts = {key: '' for key in ['param6', 'Output']}
    result = json.loads(gen_code(**artifacts, **disdat_params))
    assert result['user_kwargs'] == artifacts
    assert result['disdat_kwargs'] == disdat_params
