#!/bin/sh

git clone https://github.com/rmetzger/stratosphere.git
cd stratosphere
git checkout sql_mainline_changes
mvn clean install -DskipTests

cd

cd rmetzger/stratosphere-sql

mvn clean verify