#!/bin/sh

requirements=$1
if [[ -z "$requirements" ]] ; then
  requirements="requirements.txt"
fi

python3 -m venv defoe_env
source defoe_env/bin/activate
pip3 install -r $requirements
venv-pack -o defoe_env.tar.gz

targetDir=$2
if [[ -z "$targetDir" ]] ; then
  targetDir="./defoe_lib"
fi
zip -r defoe_lib.zip $targetDir
