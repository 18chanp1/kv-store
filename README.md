# Something about the A12
Group ID: 7
Verification Code: 40252CEFC4BBAE85DA750D7E84FF3926
Used Run Command:
``` shell
java -Xmx512m \
--add-exports=java.base/jdk.internal.ref=ALL-UNNAMED \
--add-exports=java.base/sun.nio.ch=ALL-UNNAMED \
--add-exports=jdk.unsupported/sun.misc=ALL-UNNAMED \
--add-exports=jdk.compiler/com.sun.tools.javac.file=ALL-UNNAMED \
--add-opens=jdk.compiler/com.sun.tools.javac=ALL-UNNAMED \
--add-opens=java.base/java.lang=ALL-UNNAMED \
--add-opens=java.base/java.lang.reflect=ALL-UNNAMED \
--add-opens=java.base/java.io=ALL-UNNAMED \
--add-opens=java.base/java.util=ALL-UNNAMED \
-jar A7.jar
```

## Additional Notes on Running
Some scripts are provided starting a large number of nodes on AWS instances.
Please see ./scripts for details. 

### Prerequisites
- You need to upload the CPEN 431 public key to the AWS instance

### Setup
1. Enter the public IP of the 20-node Server AWS instance to ./scripts/aws_host.txt
2. Enter the public IP of the test client's AWS instance to ./scripts/aws_tester.txt
3. Enter the private IP of the 20-node server AWS instance to ./scripts/aws_tester.txt
4. Enter the amount of nodes on the first row of ./scripts/node_setup.txt
5. Enter the starting port on the second row of ./scripts/node_setup.txt
6. Paste a version of the evaluation client in the root directory ./

