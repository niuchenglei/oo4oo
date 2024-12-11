# This file is to be placed in DBFS in order to run python jobs

import os
import runpy
import sys

from setuptools.command import easy_install

print(sys.argv)
print(sys.path)


def install_eggs():
    """
    install eggs in the python search path
    :return:
    """
    try:
        eggs = {x for x in sys.path if x.endswith('.egg')}
        for egg in eggs:
            easy_install.main([egg])
    except:
        print('Failed to install egg')
        raise


def append_new_eggs_to_path():
    """
    append newly installed eggs to python search path

    databricks cloud runs python scripts using `/databricks/python/bin/python` as `root`
    after easy_installing `TubiPySparkJobs-1.0.31_SNAPSHOT-py2.7.egg`, its dependencies are not in the `sys.path`
    :return:
    """
    try:
        # in our case, the module `os` is in the common ancestor directory with installed eggs
        pkg = os.path.join(os.path.dirname(os.__file__), 'site-packages')
        # absolute paths of eggs
        eggs = {os.path.join(pkg, x) for x in os.listdir(pkg) if x.endswith('.egg')}
        sys.path = list(set(sys.path).union(eggs))
    except Exception as e:
        print(e)


# attach pypi libraries when setting up clusters
# install_eggs()
# append_new_eggs_to_path()

script = sys.argv.pop(0)
# don't delete module
module = sys.argv[0]
runpy.run_module(module, init_globals=globals(), run_name='__main__')
