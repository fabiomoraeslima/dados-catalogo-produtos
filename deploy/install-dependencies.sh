#!/bin/bash

#!/bin/bash

WORKSPACE=$1

mkdir -p bin

apt-get --yes --force-yes install python3-venv 

python3 --version

python3 -m venv databricks-pipe

ls