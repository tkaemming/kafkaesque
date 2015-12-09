from setuptools import (
    find_packages,
    setup,
)


setup(
    name='kafkaesque',
    setup_requires=(
        'pytest-runner',
    ),
    install_requires=(
        'click',
        'redis',
    ),
    tests_require=(
        'pytest',
    ),
    packages=find_packages(),
)
