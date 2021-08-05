from setuptools import setup


def local_version(version):
    """
    Patch in a version that can be uploaded to test PyPI
    """
    return ""

setup(
    use_scm_version={
        'write_to': 'disdat_kfp/version.py',
        'write_to_template': '__version__ = "{version}"',
        'local_scheme': local_version
    },

    setup_requires=['setuptools_scm'],

    name='disdat-kfp',
    # version='0.0.4rc02',
    packages=['disdat_kfp'],
    install_requires=[
        'disdat >= 0.9.16',
        'kfp == 1.6.5'
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
            'pytest-xdist',
            'twine',
            'build'
        ],
    },

    classifiers=[
        # How mature is this project? Common values are
        #   3 - Alpha
        #   4 - Beta
        #   5 - Production/Stable
        'Development Status :: 4 - Beta',

        # Indicate who your project is intended for
        'Intended Audience :: Science/Research',
        'Topic :: Scientific/Engineering',

        # Pick your license as you wish (should match "license" above)
        'License :: OSI Approved :: Apache Software License',

        # Specify the Python versions you support here. In particular, ensure
        # that you indicate whether you support Python 2, Python 3 or both.
        'Programming Language :: Python :: 3.8',
        'Operating System :: OS Independent',
        'Natural Language :: English',
    ],
)