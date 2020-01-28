#!/bin/bash
source environment.sh
python3 src/__main__.py 2>&1 | tee dpss_log.txt
