#!/bin/bash

#simple test script
if [ $# != 2 ]
then
    echo "the num of parameters error"
    exit -1
fi

i=1
while(( $i<=$2 ))
do
    echo "echo $i th... "
    fileName="log_$i.txt"
    $1 | tee $fileName 
if [ $? != 0  ]
then
    echo "$i exec error,exit"
    exit -1
else
    echo "$i exec ok,continue..."
    
    i=$(($i+1))
fi

done

echo "finished " 
