# crowsnest-connector-cluon-n2k
A connector to a cluon-based UDP multicast setup for listening in on raw N2k frames

Currently supports:
* Rudder (127245)
* Engine (127488)

## Setup
To setup the development environment:

python3 -m venv venv
source ven/bin/activate
Install everything thats needed for development:

pip install -r requirements.txt -r requirements_dev.txt

In addition, code for `brefv` must be generated using the following commands:

mkdir brefv
datamodel-codegen --input brefv-spec/envelope.json --input-file-type jsonschema --output brefv/envelope.py
datamodel-codegen --input brefv-spec/messages --input-file-type jsonschema  --reuse-model --output brefv/messages

To run the linters:

black main.py tests
pylint main.py

To run the tests:

pytest --verbose tests


