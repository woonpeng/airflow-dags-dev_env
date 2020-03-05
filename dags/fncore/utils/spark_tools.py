"""Spark context manager."""

import os
from contextlib import contextmanager
from copy import deepcopy

from pyspark import SparkConf, SparkContext


class SparkConfFactory(object):
    """Delay set up of SparkConf."""
    def __init__(self, master=None, app_name=None, configs=None, **kwargs):
        self.master = master
        self.app_name = app_name
        self.configs = configs or dict()

        for config_name, config_value in kwargs:
            self.configs.update({str(config_name): str(config_value)})

    def set_master(self, value):
        """Set master URL to connect to."""
        return SparkConfFactory(master=value,
                                app_name=self.app_name,
                                configs=self.configs)

    def set_app_name(self, value):
        """Set application name."""
        return SparkConfFactory(master=self.master,
                                app_name=value,
                                configs=self.configs)

    def set(self, config_name, config_value):
        """Set a config."""
        new_configs = deepcopy(self.configs)
        new_configs.update({str(config_name): str(config_value)})
        return SparkConfFactory(master=self.master,
                                app_name=self.app_name,
                                configs=new_configs)

    def create(self):
        """Create an actual SparkConf."""
        return (SparkConf()
                .setAppName(self.app_name)
                .setMaster(self.master)
                .setAll(self.configs.items()))


@contextmanager
def get_spark_context(conf):
    """Get the spark context for submitting pyspark applications"""
    spark_context = None
    try:
        spark_context = SparkContext(conf=conf)

        from fncore.utils.zip_py_module import zip_py

        import fncore
        spark_context.addPyFile(zip_py(os.path.dirname(fncore.__file__)))
        import neo4j
        spark_context.addPyFile(zip_py(os.path.dirname(neo4j.__file__)))
        import croniter
        spark_context.addPyFile(zip_py(os.path.dirname(croniter.__file__)))
        import marshmallow
        spark_context.addPyFile(zip_py(os.path.dirname(marshmallow.__file__)))

        yield spark_context
    except:
        raise
    finally:
        if spark_context:
            spark_context.stop()
