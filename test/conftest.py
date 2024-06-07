"""
Fixtures and configurations for the PyTest suite
"""

import pytest
from snowflake.snowpark.session import Session
from src.util.local import get_env_var_config, get_dev_config

@pytest.fixture
def session(scope='module'):
    # pylint: disable=unused-argument
    """
    Creates a Session object for tests
    """

    return Session.builder.configs(get_env_var_config()).create()

@pytest.fixture
def dev_session(scope='module'):
    # pylint: disable=unused-argument
    """
    Creates a Session object for tests
    """

    return Session.builder.configs(get_dev_config()).create()
