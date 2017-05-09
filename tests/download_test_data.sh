#!/usr/bin/env bash

DIR="`dirname \"$0\"`"
DIR="`( cd \"$DIR\" && pwd )`"
if [ -e $DIR/test_data/test_niamoto_home/data ]
then
    echo YO
    rm -r $DIR/test_data/test_niamoto_home/data
    mkdir $DIR/test_data/test_niamoto_home/data
fi
git clone https://github.com/niamoto/niamoto-core-test-data.git $DIR/test_data/test_niamoto_home/data
