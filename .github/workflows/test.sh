#! /bin/bash

source activate mobie
python -m unittest discover -s test -v
