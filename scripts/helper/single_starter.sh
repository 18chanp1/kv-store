#!/bin/bash

SERVER_COUNT=1
START_PORT=43100
END_PORT=$(($START_PORT + $SERVER_COUNT - 1))
N_THREADS=32
MEM_MAX=60817408
N_REPLICAS=1

cd testjar

killall -9 java

for i in $(seq $START_PORT $END_PORT);
do
  echo $i
  nohup java \
  -XX:+UseCompressedOops \
  -Xmx64m \
  --add-exports=java.base/jdk.internal.ref=ALL-UNNAMED \
  --add-exports=java.base/sun.nio.ch=ALL-UNNAMED \
  --add-exports=jdk.unsupported/sun.misc=ALL-UNNAMED \
  --add-exports=jdk.compiler/com.sun.tools.javac.file=ALL-UNNAMED \
  --add-opens=jdk.compiler/com.sun.tools.javac=ALL-UNNAMED \
  --add-opens=java.base/java.lang=ALL-UNNAMED \
  --add-opens=java.base/java.lang.reflect=ALL-UNNAMED \
  --add-opens=java.base/java.io=ALL-UNNAMED \
  --add-opens=java.base/java.util=ALL-UNNAMED \
  -jar A7.jar \
  $i \
  $MEM_MAX \
  $N_THREADS \
  $N_REPLICAS \
  >serverlog.log 2>&1 </dev/null&

done

echo "Client tester server setup complete"
exit