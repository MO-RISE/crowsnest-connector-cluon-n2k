import time
import threading
import subprocess
from contextlib import contextmanager
from unittest.mock import MagicMock

import pytest
from paho.mqtt import subscribe


@contextmanager
def subscriber(*args, **kwargs):
    t = threading.Thread(target=subscribe.callback, args=args, kwargs=kwargs)
    t.setDaemon(True)
    t.start()
    time.sleep(0.1)
    yield


def test_all_parts(broker, app):

    mock = MagicMock()

    with subscriber(mock, "#"):
        subprocess.Popen(["cluon-replay", "--cid=111", "tests/test.rec"])
        time.sleep(5)

    assert mock.called
