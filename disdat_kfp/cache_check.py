
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


def caching_check(user_kwargs: dict,
                  disdat_kwargs: dict) -> NamedTuple('Output', [('use_cache', bool), ('bundle_id', str)]):
    """
    :param user_kwargs: user's parameters for the container op to be cached. Caching plugin uses this dictionary
        as signature to determine whether re-execution is necessary
    :param disdat_kwargs: parameters reserved for disdat, including bundle name, context name, s3 path, etc
    :return: named tuple
        use_cache: true if re-execution is not needed
        bundle_id: uuid of the bundle with identical signature
    """
    from disdat import api
    import logging
    from collections import namedtuple
    import os

    Outputs = namedtuple('Outputs', ['use_cache', 'bundle_id'])

    func_name = caching_check.__name__.upper()
    force_rerun = bool(disdat_kwargs.get('force_rerun', False))
    if force_rerun:
        logging.info('{} - {}'.format(func_name, 'forced rerun'))
        return Outputs(use_cache=False, bundle_id='')

    bundle_name = disdat_kwargs.get('bundle_name')
    context_name = disdat_kwargs.get('context_name')
    s3_url = disdat_kwargs.get('s3_url')
    use_verbose = bool(disdat_kwargs.get('use_verbose', False))

    if use_verbose:
        logging.basicConfig(level=logging.INFO)
    else:
        logging.basicConfig(level=logging.WARNING)
    logging.info('{} - parameters received: {}, {}'.format(func_name, user_kwargs, disdat_kwargs))

    os.system("dsdt init")                                      # disdat is initialized before first execution
    api.context(context_name)                                   # set disdat context
    api.remote(context_name, remote_context=context_name, remote_url=s3_url)
    component_signature = {k: str(v) for k, v in user_kwargs.items()}
    proc_name = api.Bundle.calc_default_processing_name(bundle_name, component_signature, dep_proc_ids={})

    api.pull(context_name, bundle_name)                         # pull all bundles with the same bundle name
    bundle = api.search(context_name, processing_name=proc_name)# get the one with the same processing bane
    use_cache, bid = False, ''                                  # by default data should be an empty dict

    if len(bundle) > 0:                                         # right now just compare parameter signature
        logging.info('{} - {}'.format(func_name, 'bundle found'))
        latest_bundle = bundle[0]                               # could have multiple because of forced reruns
        use_cache = True not in [v != latest_bundle.params.get(k, None)
                                 for k, v in component_signature.items()]
        bid = latest_bundle.uuid
    else:
        logging.info('{} - {}'.format(func_name, 'bundle not found'))

    return Outputs(use_cache=use_cache, bundle_id=bid)
