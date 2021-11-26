from main import entrypoint, brefv_rudder, brefv_propeller

from pathlib import Path
from unittest.mock import MagicMock
from pycluon import Envelope
from pycluon.importer import import_odvd

## Import and generate code for message specifications
THIS_DIR = Path(__file__).parent.parent
memo = import_odvd(THIS_DIR / "memo" / "memo.odvd")


def create_n2k_envelope(payload):

    frame = memo.memo_raw_NMEA2000()
    frame.data = payload

    envelope = Envelope()
    envelope.sampled = 0
    envelope.serialized_data = frame.SerializeToString()

    return envelope


def test_rudder_message(pinned):

    # Use a mock for the output
    output = MagicMock()
    brefv_rudder.sink(output)

    entrypoint.emit(create_n2k_envelope("09F10DE5 00F8FF7FF9FEFFFF"))

    assert output.called

    args, _ = output.call_args
    topic, brefv = args[0]
    assert topic == pinned
    assert brefv.json() == pinned


def test_propeller_message(pinned):

    # Use a mock for the output
    output = MagicMock()
    brefv_propeller.sink(output)

    entrypoint.emit(create_n2k_envelope("09F200C9 005730FFFF01FFFF"))

    assert output.called

    args, _ = output.call_args
    topic, brefv = args[0]
    assert topic == pinned
    assert brefv.json() == pinned
