import logging
import os
import shutil

import pytest

# Formatting of testing log messages.
from dls_logformatter.dls_logformatter import DlsLogformatter

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------------
@pytest.fixture(scope="session")
def constants(request):

    constants = {}

    yield constants


# --------------------------------------------------------------------------------
@pytest.fixture(scope="session")
def logging_setup():
    # print("")

    formatter = DlsLogformatter(type="long")
    logger = logging.StreamHandler()
    logger.setFormatter(formatter)
    logging.getLogger().addHandler(logger)

    # Log level for all modules.
    logging.getLogger().setLevel("DEBUG")

    # Turn off noisy debug.
    logging.getLogger("asyncio").setLevel("WARNING")
    logging.getLogger("pika").setLevel("WARNING")
    logging.getLogger("stomp").setLevel("WARNING")
    logging.getLogger("luigi-interface").setLevel("WARNING")
    logging.getLogger("luigi.bx_scheduler").setLevel("INFO")
    logging.getLogger("urllib3.connectionpool").setLevel("INFO")

    logging.getLogger("dls_utilpack.things").setLevel("INFO")

    # Cover the version.
    # logger.info("\n%s", (json.dumps(version_meta(), indent=4)))

    yield None


# --------------------------------------------------------------------------------
@pytest.fixture(scope="function")
def output_directory(request):
    # TODO: Get a newline in conftest after pytest emits class name.
    print("")

    # Tmp directory which we can write into.
    output_directory = "/tmp/%s/%s/%s" % (
        "/".join(__file__.split("/")[-3:-1]),
        request.cls.__name__,
        request.function.__name__,
    )

    # Tmp directory which we can write into.
    if os.path.exists(output_directory):
        shutil.rmtree(output_directory, ignore_errors=False, onerror=None)
    os.makedirs(output_directory)

    # logger.debug("output_directory is %s" % (output_directory))

    yield output_directory
