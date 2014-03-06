#!/bin/sh

git clone https://github.com/rmetzger/stratosphere.git
cd stratosphere
git checkout sql_mainline_changes
mvn -B clean install -DskipTests

cd ..

mvn -B clean verify