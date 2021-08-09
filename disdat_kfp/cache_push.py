"""
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
"""


from typing import NamedTuple


def caching_push(user_kwargs: dict,
                 disdat_kwargs: dict) -> NamedTuple('Output', [('bundle_id', str)]):
    """
    push output from user's container to disdat remote repo
    parameters suppiled to user's container will be used as an unique identifier of current execution

    :param user_kwargs: parameters supplied to user's container
    :param disdat_kwargs: parameters that disdat use to push data to repo
    :return: the uuid of the newly created bundle
    """
    import logging, os
    from disdat import api
    from collections import namedtuple
    import shutil

    bundle_name = disdat_kwargs.get('bundle_name')
    context_name = disdat_kwargs.get('context_name')
    s3_url = disdat_kwargs.get('s3_url')
    use_verbose = bool(disdat_kwargs.get('use_verbose', False))
    container_used = disdat_kwargs.get('container_used', '')
    container_cmd = disdat_kwargs.get('container_cmd', '')

    input_src_folder = disdat_kwargs.get('input_src_folder', '/tmp/inputs')
    zip_file_name = disdat_kwargs.get('zip_file_name', 'data_cache')
    assert '.zip' not in zip_file_name, 'zip file name must not include file format extension'
    user_params, user_artifacts = {}, {}
    for key, val in user_kwargs.items():                        # retrieve the list of input variables
        if key.startswith('reserve_disdat'):
            user_artifacts[key] = val
        else:
            user_params[key] = val

    if use_verbose:
        logging.basicConfig(level=logging.INFO)
    else:
        logging.basicConfig(level=logging.WARNING)

    func_name = caching_push.__name__
    logging.info('{} - {}'.format(func_name, 'initialized'))
    logging.info('{} - {}'.format(func_name, user_kwargs))
    logging.info('{} - {}'.format(func_name, disdat_kwargs))

    # host = os.getenv('KUBERNETES_SERVICE_HOST')
    # port = os.getenv('KUBERNETES_SERVICE_PORT')
    # print(os.environ)
    # client = kfp.Client(host=f'http://{host}:{port}')
    # print(client.list_experiments())

    os.system("dsdt init")
    api.context(context_name)
    api.remote(context_name, remote_context=context_name, remote_url=s3_url)
    logging.info('{} - {}'.format(func_name, 'data: ' + str(user_artifacts)))
    component_signature = {k: str(v) for k, v in user_params.items()}
    proc_name = api.Bundle.calc_default_processing_name(bundle_name, component_signature, dep_proc_ids={})

    if len(user_artifacts) > 0:
        for key, path in user_artifacts.items():
            src = path
            dst = path.replace('reserve_disdat_', '').replace('inputs', 'copy')
            os.makedirs(os.path.dirname(dst), exist_ok=True)
            logging.info('{} - {}'.format(func_name,  src + ' copied to ' + dst))
            shutil.copy(src, dst)                               # copy data from /tmp/inputs to /tmp/copy
        logging.info('{} - {}'.format(func_name,  os.listdir(input_src_folder.replace('inputs', 'copy'))))
        shutil.make_archive(base_name=zip_file_name, format='zip', root_dir=input_src_folder.replace('inputs', 'copy'))
        logging.info('{} - {}'.format(func_name, 'file zipped ' + zip_file_name))
    with api.Bundle(context_name, name=bundle_name, processing_name=proc_name) as b:
        b.add_params(component_signature)
        if len(user_artifacts) > 0:                             # it is possible that there's no artifacts to save
            b.add_data(zip_file_name + '.zip')
        b.add_tags({'container_used': container_used, 'container_cmd': container_cmd})

    api.commit(context_name, bundle_name)
    api.push(context_name, bundle_name)                         # save the bundle to S3
    logging.info('{} - {}'.format(func_name, 'data saved on s3'))
    Output = namedtuple('output', ['bundle_id'])
    return Output(b.uuid)                                       # return the uuid of the bundle
