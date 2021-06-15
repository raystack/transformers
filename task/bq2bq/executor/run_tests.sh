#!/bin/sh -e

pip install -r requirements.txt
python -m unittest discover tests/
