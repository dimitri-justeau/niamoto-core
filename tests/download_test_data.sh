#!/usr/bin/env bash

DIR="`dirname \"$0\"`"
DIR="`( cd \"$DIR\" && pwd )`"
if [ -e $DIR/test_data/test_niamoto_home/data ]
then
    rm -r $DIR/test_data/test_niamoto_home/data
    mkdir $DIR/test_data/test_niamoto_home/data
fi
wget https://github.com/niamoto/niamoto-core-test-data/archive/master.zip -P $DIR/test_data/test_niamoto_home/data
unzip $DIR/test_data/test_niamoto_home/data/master.zip -d $DIR/test_data/test_niamoto_home/data
mv $DIR/test_data/test_niamoto_home/data/niamoto-core-test-data-master/* $DIR/test_data/test_niamoto_home/data
rm -R $DIR/test_data/test_niamoto_home/data/niamoto-core-test-data-master
rm $DIR/test_data/test_niamoto_home/data/master.zip
