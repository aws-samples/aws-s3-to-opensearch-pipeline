#!/bin/sh

set -ex
python3 -m black src/
python3 -B -m isort src/
python3 -m flake8 --config=setup.cfg .