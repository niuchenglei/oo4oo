import os
import re
import subprocess

import sys
from setuptools import setup, find_packages, Command

try:  # for pip >= 10
    from pip._internal.req import parse_requirements
except ImportError:  # for pip <= 9.0.3
    from pip.req import parse_requirements

def setup_requirements():
    """
    figure out what we need to install, depending on whether we are in CI or not
    :return: a list of ["package==version",...]
    """
    try:  # for pip < 20.1.x
        all_reqs = [str(r.req) for r in parse_requirements('../requirements.txt', session=False)]
    except:  # for pip 20.1.1, check https://stackoverflow.com/questions/62114945/attributeerror-parsedrequirement-object-has-no-attribute-req
        all_reqs = [str(r.requirement) for r in parse_requirements('../requirements.txt', session=False)]

    conda_preinstalls = {'numpy', 'scipy', 'pandas', 'scikit-learn'}

    if os.environ.get('TRAVIS'):
        return [r for r in all_reqs if r.partition('==')[0] not in conda_preinstalls]
    else:
        return all_reqs


##### Begin setup.py logic
reqs = setup_requirements()
version = None

try:
    run_version = subprocess.run(["git", "describe", "--abbrev=0", "--tags"], stdout=subprocess.PIPE)
    git_version = run_version.stdout.strip().decode("utf-8")
    run_branch = subprocess.run(["git", "branch", "--show-current"], stdout=subprocess.PIPE)
    git_branch = run_branch.stdout.strip().decode("utf-8")
    if git_branch != "master":
        version = git_version + ".dev0"
    else:
        version = git_version
except:
    version = '1.0.dev0'

setup(
    name='TubiPySparkJobs',
    version=version,
    setup_requires=['requests==2.21.0'],
    install_requires=reqs,
    include_package_data=True,
    dependency_links=['git+https://github.com/assiotis/click-datetime#egg=click-datetime-0.2'],
    description='pySpark code for Tubi TV',
    author='Tubi Engineering',
    zip_safe=True,
    cmdclass={},
    packages=find_packages(exclude=["*.tests", "*.tests.*", "tests.*", "tests"]) + ['settings'],
    author_email='engineering@tubi.tv'
)
