import os
from tests.config import ARTIFACT_DIR


tar_dir = ARTIFACT_DIR + '/tars'


def validate_container_execution(bundle_name: str,
                                 context_name: str,
                                 s3_url: str,
                                 younger_than: float) -> str:
    from disdat import api
    import os
    import logging
    func_name = validate_container_execution.__name__.upper()
    os.system('dsdt init')
    api.context(context_name)
    api.remote(context_name, remote_context=context_name, remote_url=s3_url)
    logging.basicConfig(level=logging.INFO)
    logging.info('{} - {}'.format(func_name, 'disdat initialized'))

    api.pull(context_name, bundle_name=bundle_name, localize=False)
    latest_bundle= api.get(context_name, bundle_name)
    creation_time = latest_bundle.creation_date

    assert creation_time > younger_than, "task has been skipped unexpectedly!"
    return latest_bundle.uuid


def validate_container_no_execution(bundle_name: str,
                                    uuid: str,
                                    context_name: str,
                                    s3_url: str) -> bool:
    from disdat import api
    import os
    import logging
    func_name = validate_container_no_execution.__name__.upper()
    os.system('dsdt init')
    api.context(context_name)
    api.remote(context_name, remote_context=context_name, remote_url=s3_url)
    logging.basicConfig(level=logging.INFO)
    logging.info('{} - {}'.format(func_name, 'disdat initialized'))

    api.pull(context_name, bundle_name=bundle_name, localize=False)
    latest_bundle = api.get(context_name, bundle_name)

    assert uuid == latest_bundle.uuid, "task has been re-executed unexpectedly!"
    return True


def submit_and_validate(zip_file, expected_status_code=0, args=None):
    folder_name = zip_file.split('.')[0]
    dir_name = os.path.join(tar_dir, folder_name)
    print('submitting - ' + dir_name)
    os.makedirs(dir_name , exist_ok=True)
    os.system("tar -xvf {} -C {}".format(zip_file, dir_name))
    # cmd = 'argo submit --wait --serviceaccount mlp-pipelines {}'.format(
    #     os.path.join(dir_name, 'pipeline.yaml '))
    # if args is not None:
    #     cmd += args
    # status = os.system(cmd)
    # assert status == expected_status_code, 'pipeline {} yields unwanted result'.format(folder_name)


def version_checker(context_name: str,
                    bundle_name: str,
                    s3_url: str,
                    uuid: str = '',
                    gap: int = 0,
                    check: bool = False) -> str:
    from disdat import api
    import os
    import logging

    func_name = version_checker.__name__.upper()
    os.system('dsdt init')
    api.context(context_name)
    api.remote(context_name, remote_context=context_name, remote_url=s3_url)

    logging.basicConfig(level=logging.INFO)
    logging.info('{} - {}'.format(func_name, 'disdat initialized'))

    api.pull(context_name, bundle_name=bundle_name, localize=False)

    if not check:
        latest_bundle = api.get(context_name, bundle_name)
        if latest_bundle is None:
            return ''
        else:
            return latest_bundle.uuid
    else:
        versions = api.search(context_name, bundle_name)
        diff = 0
        found = False
        for version in versions:
            logging.info('{} - {}'.format(func_name, version.uuid))
            if uuid == version.uuid:
                found = True
                break
            diff += 1

        if found is False:
            diff = -1

        assert diff == gap, 'diff={}, expected diff={}'.format(diff, gap)
        return ''


