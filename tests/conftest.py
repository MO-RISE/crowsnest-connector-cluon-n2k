import time
import pytest
import subprocess


@pytest.fixture(scope="session")
def broker(docker_ip, docker_services):
    """Stupid simple way of making sure everything is up and running"""

    time.sleep(10)


@pytest.fixture
def app():
    subprocess.Popen(["python", "main.py"])
