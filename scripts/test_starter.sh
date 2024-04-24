#!/bin/bash

AWS_HOST=$(head -n 1 aws_host.txt)
AWS_TEST=$(head -n 1 aws_tester.txt)
PRIV_IP=$(head -n 1 private_ip.txt)
START_PORT=$(sed -n '2p' node_setup.txt)
SERVER_COUNT=$(sed -n '1p' node_setup.txt)
END_PORT=$(($START_PORT + $SERVER_COUNT - 1))

echo "Setting up directory on remote"
ssh -o StrictHostKeyChecking=no $AWS_TEST "mkdir -p testjar"

echo "copying eval client to remote"
scp -o StrictHostKeyChecking=no \
../a11_eval.jar $AWS_TEST:~/testjar/a11_eval.jar

echo "copying server to remote"
scp -o StrictHostKeyChecking=no \
../target/CPEN431_2024_PROJECT_7-1.0-SNAPSHOT-jar-with-dependencies.jar \
$AWS_TEST:~/testjar/A7.jar

echo "copying servers.txt to remote"
echo "127.0.0.1:43100" | ssh $AWS_TEST 'cat > ~/testjar/servers.txt'

echo "copying full server (private ip) list to remote"
> servers_all_tmp.txt
for i in $(seq $START_PORT $END_PORT);
do
  echo "$AWS_HOST:$i" >> servers_all_tmp.txt
done
scp -o StrictHostKeyChecking=no \
./servers_all_tmp.txt $AWS_TEST:~/testjar/servers_all.txt
rm servers_all_tmp.txt

echo "ssh to remote server"
cat ./helper/single_starter.sh | ssh -o StrictHostKeyChecking=no $AWS_TEST

echo "start tester"
cat ./helper/start_eval.sh | ssh -o StrictHostKeyChecking=no $AWS_TEST

echo "Test complete"


