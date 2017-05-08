#!/usr/bin/env bash

DIR="`dirname \"$0\"`"
DIR="`( cd \"$DIR\" && pwd )`"
rm -r $DIR/test_data/test_niamoto_home/data/*
git clone https://github.com/niamoto/niamoto-core.git $DIR/test_data/test_niamoto_home/data
