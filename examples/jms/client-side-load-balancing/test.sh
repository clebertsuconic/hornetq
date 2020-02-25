#!/bin/bash


x=1
while [ $x -eq 1 ]
do
    killall -9 java
    mvn clean
    mvn verify | tee test.log

    result=$(grep -c "Timed out waiting to receive initial" test.log)
    echo "value is $result"
    if [ $result -gt 0 ]
    then
          echo "Found the error"
          x=2
    fi

done
