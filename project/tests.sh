#!/bin/bash

echo "Setting up test environment !!!"
mkdir -p ./data
rm -f ./data/*.db

echo "Running data pipeline test !!!"
python ./project/test.py

