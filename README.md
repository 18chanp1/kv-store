# Something about the A7
Group ID: 7
Verification Code: 49C23866D1C090D3821DA9D17F29E2D6
Used Run Command:
``` shell
 java -Xmx512m \
  -XX:+UseCompressedOops \
  --add-exports=java.base/jdk.internal.ref=ALL-UNNAMED \
  --add-exports=java.base/sun.nio.ch=ALL-UNNAMED \
  --add-exports=jdk.unsupported/sun.misc=ALL-UNNAMED \
  --add-exports=jdk.compiler/com.sun.tools.javac.file=ALL-UNNAMED \
  --add-opens=jdk.compiler/com.sun.tools.javac=ALL-UNNAMED \
  --add-opens=java.base/java.lang=ALL-UNNAMED \
  --add-opens=java.base/java.lang.reflect=ALL-UNNAMED \
  --add-opens=java.base/java.io=ALL-UNNAMED \
  --add-opens=java.base/java.util=ALL-UNNAMED \
  -jar A9.jar \
  $i \
  $MEM_MAX \
  $N_THREADS \
```

Please set the bash variable $i for port, $MEM_MAX for maximum memory alloc, and $N_THREADS for maximum amount
of handler threads.

## Additional Notes on Running
Some scripts are provided starting a large number of nodes on AWS instances.
Please see ./scripts for details. 


The servers list provided may be inaccurate since the IP address changes depending on where you run it. 
However, the scripts below dynamically generate it based on what you write into the txt files. 

### Prerequisites
- You need to upload the CPEN 431 public key to the AWS instance

### Setup
1. Enter the public IP of the 20-node Server AWS instance to ./scripts/aws_host.txt
2. Enter the public IP of the test client's AWS instance to ./scripts/aws_tester.txt
3. Enter the private IP of the 20-node server AWS instance to ./scripts/aws_tester.txt
4. Enter the amount of nodes on the first row of ./scripts/node_setup.txt
5. Enter the starting port on the second row of ./scripts/node_setup.txt
6. Paste a version of the evaluation client in the root directory ./



