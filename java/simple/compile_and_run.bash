#!/usr/bin/env bash

# compile and create the jar file
if [[ ! -e target/test-4.19.0.1.jar ]] ; then
    mvn clean package
fi

# run from the jar file
java -jar target/test-4.19.0.1.jar
