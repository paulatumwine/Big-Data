#!/bin/bash

bin/hadoop fs -mkdir "relative"
bin/hadoop fs -mkdir "relative/input"
bin/hadoop fs -put input/testData.txt relative/input
mv pairs-relative-1.0-SNAPSHOT.jar PairsRelative.jar
bin/hadoop fs -rm -r -f relative/output #in case it already exists, remove it
bin/hadoop jar PairsRelative.jar hadoop.relative.frequencies.pairs.Compute relative/input relative/output
