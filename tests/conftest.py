import os
import shutil
import tempfile

import pytest

# local Spark needs a JVM; honour JAVA_HOME if it is set, otherwise rely on PATH
from falcon.spark import get_spark


@pytest.fixture(scope="session")
def spark():
    cfg = {"spark": {"app_name": "falcon-tests", "master": "local[2]", "driver_memory": "4g",
                     "shuffle_partitions": 2}}
    s = get_spark(cfg)
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()


@pytest.fixture()
def wh():
    d = tempfile.mkdtemp(prefix="falcon_test_")
    yield d
    shutil.rmtree(d, ignore_errors=True)
