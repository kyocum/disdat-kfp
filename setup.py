from setuptools import setup


setup(
    # use_scm_version={
    #     'write_to': 'disdat/version.py',
    #     'write_to_template': '__version__ = "{version}"'
    # },
    # setup_requires=['setuptools_scm'],


    name='disdat-kfp',
    version='0.0.4',
    packages=['disdat_kfp'],
    install_requires=[
        'disdat >= 0.9.16',
        'kfp >= 1.6.5'
    ],
    # Choose your license
    license='Apache License, version 2.0',
    author='Ken Yocum, Zixuan Zhang',
    author_email='kyocum@gmail.com, zz2777@columbia.edu',
    url='https://github.com/kyocum/disdat-kfp',

    extras_require={
        'dev': [
            'pytest',
            'ipython',
            'pytest-xdist'
        ],
    },
)