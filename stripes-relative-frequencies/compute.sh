#!/bin/bash

bin/hadoop fs -mkdir "relative"
bin/hadoop fs -mkdir "relative/input"
bin/hadoop fs -put input/testData.txt relative/input
mv stripes-relative-1.0-SNAPSHOT.jar StripesRelative.jar
bin/hadoop fs -rm -r -f relative/output #in case it already exists, remove it
bin/hadoop jar StripesRelative.jar hadoop.relative.frequencies.stripes.Compute relative/input relative/output
