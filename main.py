"""Main entrypoint for this application"""

from pathlib import Path
from math import degrees
from datetime import datetime

from environs import Env

from streamz import Stream

from paho.mqtt.client import Client as MQTT

from pycluon import OD4Session, Envelope as cEnvelope
from pycluon.importer import import_odvd

from marulc import NMEA2000Parser
from marulc.utils import filter_on_pgn, deep_get
from marulc.exceptions import MultiPacketInProcessError

from brefv.envelope import Envelope
from brefv.messages.observations.rudder import Rudder
from brefv.messages.observations.propeller import Propeller

# Reading config from environment variables
env = Env()

CLUON_CID = env.int("CLUON_CID", 111)

MQTT_BROKER_HOST = env("MQTT_BROKER_HOST")
MQTT_BROKER_PORT = env.int("MQTT_BROKER_PORT", 1883)
MQTT_CLIENT_ID = env("MQTT_CLIENT_ID", None)
MQTT_TRANSPORT = env("MQTT_TRANSPORT", "tcp")
MQTT_USER = env("MQTT_USER", None)
MQTT_TLS = env.bool("MQTT_TLS", False)
MQTT_PASSWORD = env("MQTT_PASSWORD", None)
MQTT_BASE_TOPIC = env("MQTT_BASE_TOPIC", "/test/test")

RUDDER_CONFIG = env.dict("RUDDER_CONFIG", default={})
PROPELLER_CONFIG = env.dict("PROPELLER_CONFIG", default={})


## Import and generate code for message specifications
THIS_DIR = Path(__file__).parent
memo = import_odvd(THIS_DIR / "memo" / "memo.odvd")


mq = MQTT(client_id=MQTT_CLIENT_ID, transport=MQTT_TRANSPORT)
mq.username_pw_set(MQTT_USER, MQTT_PASSWORD)
if MQTT_TLS:
    mq.tls_set()
mq.connect(MQTT_BROKER_HOST, MQTT_BROKER_PORT)

# Not empty filter
not_empty = lambda x: x is not None


## Main entrypoint for N2k frames
entrypoint = Stream()
parser = NMEA2000Parser()


def unpack_n2k_frame(envelope: cEnvelope):
    """Extract an n2k frame from an envelope and unpack it using marulc"""
    try:
        frame = memo.memo_raw_NMEA2000()
        frame.ParseFromString(envelope.serialized_data)
        msg = parser.unpack(frame.data)
        msg["timestamp"] = envelope.sampled
        return msg

    except MultiPacketInProcessError:
        return None
    except Exception as exc:  # pylint: disable=broad-except
        print(exc)
        return None


unpacked = entrypoint.map(unpack_n2k_frame).filter(not_empty)


## Rudder
def pgn127245_to_brefv(msg):
    """Converting a marulc dict to a brefv messages and packaging it into a a brefv construct"""
    n2k_id = str(deep_get(msg, "Fields", "instance"))

    if sensor_id := RUDDER_CONFIG.get(n2k_id):
        crowsnest_id = list(RUDDER_CONFIG.keys()).index(n2k_id)

        rud = Rudder(
            sensor_id=sensor_id, angle=degrees(-1 * msg["Fields"]["angleOrder"])
        )  # Negating to adhere to brefv conventions

        envelope = Envelope(
            sent_at=datetime.utcfromtimestamp(msg["timestamp"]).isoformat(),
            message_type="https://mo-rise.github.io/brefv/0.1.0/messages/observations/rudder.json",
            message=rud.dict(
                exclude_none=True, exclude_unset=True, exclude_defaults=True
            ),
        )

        return f"/observations/rudder/{crowsnest_id}", envelope

    return None


brefv_rudder = (
    unpacked.filter(filter_on_pgn(127245)).map(pgn127245_to_brefv).filter(not_empty)
)


## Propeller (Using engine data for now...)
def pgn127488_to_brefv(msg):
    """Converting a marulc dict to a brefv messages and packaging it into a a brefv construct"""
    n2k_id = str(deep_get(msg, "Fields", "instance"))

    if sensor_id := PROPELLER_CONFIG.get(n2k_id):
        crowsnest_id = list(PROPELLER_CONFIG.keys()).index(n2k_id)

        prop = Propeller(sensor_id=sensor_id, rpm=msg["Fields"]["speed"])

        envelope = Envelope(
            sent_at=datetime.utcfromtimestamp(msg["timestamp"]).isoformat(),
            message_type="https://mo-rise.github.io/brefv/0.1.0/messages/observations/propeller.json",  # pylint: disable=line-too-long
            message=prop.dict(
                exclude_none=True, exclude_unset=True, exclude_defaults=True
            ),
        )

        return f"/observations/propeller/{crowsnest_id}", envelope

    return None


brefv_propeller = (
    unpacked.filter(filter_on_pgn(127488)).map(pgn127488_to_brefv).filter(not_empty)
)


# Finally, publish to mqtt
def to_mqtt(data):
    """Push data to a mqtt topic"""
    subtopic, envelope = data
    mq.publish(
        f"{MQTT_BASE_TOPIC}{subtopic}",
        envelope.json(),
    )


if __name__ == "__main__":
    print("All setup done, lets start processing messages!")

    # Connect remaining pieces
    brefv_rudder.latest().rate_limit(0.1).sink(to_mqtt)
    brefv_propeller.latest().rate_limit(0.1).sink(to_mqtt)

    # Register triggers
    session = OD4Session(CLUON_CID)
    session.add_data_trigger(10002, entrypoint.emit)

    mq.loop_forever()
