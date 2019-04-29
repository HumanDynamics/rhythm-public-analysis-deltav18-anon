#!/bin/bash
#python ./make_dataset.py download
#gzip ../../data/raw/hub_data/*txt
python ./make_dataset.py group
python ./make_dataset.py process
python ./make_dataset.py clean
python ./make_dataset.py analysis
