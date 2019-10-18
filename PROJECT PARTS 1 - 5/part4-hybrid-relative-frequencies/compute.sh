#!/bin/bash

bin/hadoop fs -mkdir "relative"
bin/hadoop fs -mkdir "relative/input"
bin/hadoop fs -put input/testData.txt relative/input
mv hybrid-relative-1.0-SNAPSHOT.jar HybridRelative.jar
bin/hadoop fs -rm -r -f relative/output #in case it already exists, remove it
bin/hadoop jar HybridRelative.jar hadoop.relative.frequencies.hybrid.Compute relative/input relative/output
