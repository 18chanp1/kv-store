#!/bin/bash

echo "starting test"
cd testjar
#java -jar a9_eval.jar --servers-list=servers_all.txt --submit --secret-code 2706426568
java -jar a11_eval.jar --servers-list=servers_all.txt --only-stage-4-seqcon --secret-code 2706426568

#java -jar a9_eval.jar --servers-list=servers_all.txt --secret-code 2706426568


echo "killing all java"
killall -9 java