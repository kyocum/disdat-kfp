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


def gather_data(user_kwargs: dict,
                disdat_kwargs: dict) -> str:
    """
    determine whether the caching wrapper should return the result from user's container (cache not used)
    or re-use a previously executed result identified by uuid returned fom cahcing_check()

    :param user_kwargs: should be an empty dictionary, used to simplify code generation logic
    :param disdat_kwargs: parameters that disdat use to push data to repo
    :return: the uuid of the chosen bundle
    """

    from disdat import api
    import os
    import shutil
    import logging
    import re

    bundle_name = disdat_kwargs['bundle_name']
    context_name = disdat_kwargs['context_name']
    s3_url = disdat_kwargs['s3_url']
    use_verbose = bool(disdat_kwargs.get('use_verbose', False))
    caching_check_uuid = disdat_kwargs['caching_check_uuid']
    caching_push_uuid = disdat_kwargs['caching_push_uuid']
    # output_var_names = disdat_kwargs['output_var_name_list']
    unzip_path = disdat_kwargs.get('unzip_path', 'unzipped_folder')
    output_path = disdat_kwargs.get('output_path', '/tmp/outputs')

    if use_verbose:
        logging.basicConfig(level=logging.INFO)
    else:
        logging.basicConfig(level=logging.WARNING)

    func_name = gather_data.__name__
    logging.info('{} - {}'.format(func_name, 'initialized'))
    os.system("dsdt init")
    api.context(context_name)
    api.remote(context_name, remote_context=context_name, remote_url=s3_url)
    logging.info('{} - {}'.format(func_name, user_kwargs))
    logging.info('{} - {}'.format(func_name, disdat_kwargs))

    # aws_access_key_id = disdat_kwargs.get('s3_access_key_id', None)
    # aws_secret_access_key = disdat_kwargs.get('s3_secret_access_key_id', None)
    # aws_session_token = disdat_kwargs.get('s3_session_token', None)
    #
    # write_to_file = (aws_access_key_id is not None) and (aws_secret_access_key is not None)
    # if write_to_file:
    #     logging.info('{} - {}'.format(func_name, 'write aws credentials to file'))
    #     credential_file = os.path.expanduser('~/.aws')
    #     os.makedirs(credential_file, exist_ok=True)
    #     with open(credential_file + '/credentials', 'w') as fp:
    #         fp.write('[default]\n')
    #         fp.write('aws_access_key_id={}\n'.format(aws_access_key_id))
    #         fp.write('aws_secret_access_key={}\n'.format(aws_secret_access_key))
    #         if aws_session_token:
    #             fp.write('aws_session_token={}'.format(aws_session_token))

    if not re.match(pattern=r'\{\{.+\}\}', string=caching_push_uuid):           # use cached bundle if condition is met
        uuid = caching_push_uuid
    else:
        uuid = caching_check_uuid

    api.pull(context_name, uuid=uuid, localize=True)                            # bundle the bundle with all its files
    bundle = api.get(context_name, bundle_name=bundle_name, uuid=uuid)
    disdat_dir = bundle.local_dir
    zip_file = os.path.join(disdat_dir, 'data_cache.zip')
    if os.path.isdir(unzip_path):                                               # check if zip file is present
        os.system('rm -r {}'.format(unzip_path))
    os.makedirs(unzip_path, exist_ok=True)
    if os.path.isfile(zip_file):
        shutil.unpack_archive(zip_file, extract_dir=unzip_path, format='zip')
        logging.info('{} - {}'.format(func_name, 'unzipped files -' + ','.join(os.listdir(unzip_path))))
        shutil.copytree(src=unzip_path, dst=output_path, dirs_exist_ok=True)    # copy unzipped folder to /tmp/outputs/

    return uuid