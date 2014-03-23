#!/bin/sh

git clone https://github.com/rmetzger/stratosphere.git
cd stratosphere
git checkout sql_mainline_changes
mvn -B clean install -DskipTests

cd ..

git clone https://github.com/rmetzger/optiq
cd optiq
git checkout new_rex_test
mvn -B clean install -DskipTests

cd ..

mvn -B clean verify