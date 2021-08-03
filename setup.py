from setuptools import setup


setup(
    name='disdat-kfp',
    version='0.0.1',
    packages=['disdat_kfp'],
    install_requires=[
        'disdat >= "0.9.16"',
        'kfp == "1.6.5"'
    ],
)