# FIT 3182 Assignment 3

## Getting Started
### 1. Creating AWS EC2 Instances
First, create 4 EC2 instances:
1. **Application and OS Images**
  Set to Ubuntu Server 24.04 LTS (HVM), EBS General Purpose (SSD) Volume Type.
2. **Instance Type**
   Set to m7i-flex.large (2 vCPU, 8GB RAM) for master instance and t3.small (2 vCPU, 2GB RAM) for the two worker and mongodb instances.
3. **Key Pair**
   Create a new key pair for SSH access and download the private key file (key.pem).
   Use the same key pair for all instances.
4. **Security Group**
   When creating the first instance (master), create a new security group with the following inbound rules:
   | Port Range | Protocol | Source | Description |
   |------------|----------|--------|-------------|
   | 22 | TCP | Your IP | SSH access |
   | 2049 | TCP | 0.0.0.0/0  | NFS access |
   | 2181 | TCP | 0.0.0.0/0 | ZooKeeper Port |
   | 4040 | TCP | 0.0.0.0/0 | Spark Web UI Port |
   | 7077 | TCP | 0.0.0.0/0 | Spark Master Port |
   | 8080 | TCP | 0.0.0.0/0 | Spark Master Web UI Port |
   | 9092 | TCP | 0.0.0.0/0 | Kafka Brokers |
   | 27017 | TCP | 0.0.0.0/0 | MongoDB |
   | 39000 | TCP | 0.0.0.0/0 | Spark Driver Port |
   | 39001-39010 | TCP | 0.0.0.0/0 | Spark Block Manager Ports |

   Make sure to replace 0.0.0.0/0 with specific IP addresses for better security in a production environment.

   Use the existing security group for the remaining instances to ensure they can communicate with each other.
5. **Storage**
   Increase the storage of each instance to 20GB to allocate enough space for the docker images and containers, application data, and logs.
### 2. Creating EFS File System
1. Go to the EFS service in the AWS console and create a new file system.
2. In the network tab of the created EFS, add the existing security groups created in Section 1 for the EC2 instances to allow NFS access.



### 3. Updating Configuration Files
1. Copy the `key.pem` file to each directory for the master, worker and mongodb instances.

2. **Update Environment Variables**
   Update the ports, hostnames and local IP addresses `.env` for each directory (including visualisation) based on the values below:
   ```bash
   ZOOKEEPER_PORT=2181
   KAFKA_PORT=9092
    MONGO_PORT=27017
    JUPYTER_PORT=8888
    SPARKUI_PORT=4040
    KAFKA_BOOTSTRAP=<WORKER-1_PRIVATE_IP>:9092,<WORKER-2_PRIVATE_IP>:9092
    SPARK_MASTER_URL=spark://<MASTER_PRIVATE_IP>:7077
    MONGO_URI=mongodb://<MONGO_PRIVATE_IP>:<MONGO_PORT>/
    MONGO_PUBLIC_URI=mongodb://<MONGO_PUBLIC_IP>:<MONGO_PORT>/
    ZOOKEEPER_HOST=<ZOOKEEPER_HOST_IP_ADDRESS>
    SPARK_LOCAL_IP=<LOCAL_IP_ADDRESS>

    # Volumes
    PROJECT_ROOT=.
    MONGO_DATA=./mongodb
    ```
3. Update the private IPs in `transfer.bat` and `connect.bat` for the master, worker and mongodb instances based on the private IP addresses of each instance.

4. Change the following in the `docker-compose.yml` for workers:
   ```bash
    kafka:
    ...
      environment:
        KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://${LOCAL_IP_ADDRESS}:${KAFKA_PORT}
        KAFKA_ADVERTISED_HOST_NAME: ${LOCAL_IP_ADDRESS}
   ```
   And set network mode to host for worker services so they can refer to their local IP addresses:
   ```bash

    pyspark:
    ...
      network_mode: host
   ```

### 4. Installing Dependencies
1. **Connect with SSH**
   SSH into the EC2 instance using the downloaded `key.pem` file and the public IP address of the instance:
   
   ```bash
   ssh -i key.pem ubuntu@<EC2_PUBLIC_IP>
   ```

   `connect.bat` contains the above SSH command for connecting to the master, worker and mongodb instances in their respective directories.
2. **Install Docker and Docker Compose**
   Run the following commands to install Docker and Docker Compose on the EC2 instance to acquire the necessary docker images with the dependencies for running the application:
   ```bash
   sudo apt update
   sudo apt install docker.io -y
   sudo apt install docker-compose-v2 -y
   sudo systemctl enable docker
   sudo systemctl start docker
   ```
3. **Mount EFS File System**
   Run the following commands to mount the EFS file system on the EC2 master and worker instances to allow sharing of checkpoints between the Spark master and worker nodes:
   ```bash
   sudo apt install -y nfs-common
   sudo mkdir -p /mnt/shared
   sudo mount -t nfs4 -o nfsvers=4.1,rsize=1048576,wsize=1048576,hard,timeo=600,retrans=2,noresvport fs-<EFS_FILE_SYSTEM_ID>.efs.ap-southeast-1.amazonaws.com:/ \
     /mnt/shared
   ```
4. **Transfer Application Code**
   For the master instance, clone the following GitHub repository containing the application code:
   ```bash
    git clone https://github.com/OpagueGlass/FIT3182-A3-master.git
   ```
   You can alternative use `scp` (shown below) to transfer the master instance application code locally.
 
   For the other instances, transfer the application code from your local machine to their respective EC2 instances:
   ```bash
    scp key.pem -r <files> ubuntu@<EC2_PUBLIC_IP>:~/
   ```
   `transfer.bat` contains the above `scp` command for transferring the application code for the worker and mongodb instances in their respective directories.

### Starting the Deployment
Launch the local visualisation Jupyter notebook container with `start.bat` and access the notebook at `http://localhost:8888` to view the live visualisation. 

**Follow the exact sequence below to run the deployment correctly:**

1. SSH into the mongodb instance and run the following command to start the MongoDB container:
   ```bash
    bash start.sh
   ```

2. SSH into the master instance and run the following
   command to start the Spark and ZooKeeper containers and spark master process:
   ```bash
    cd FIT3182-A3-master
    bash start.sh
   ```

3. Once completed, SSH into the worker instances and run the following
   command to start the Spark and Kafka containers and spark worker process:
   ```bash
   bash start.sh
   ```

4. If topics have not been created, wait for 30 seconds until ZooKeeper registers both brokers and run the following command in worker 2 to create the Kafka topics:
   ```bash
    bash create_topics.sh
   ```
5. In the master instance, run the following command to start the Spark streaming job:
   ```bash
    bash start_spark.sh
   ```
6. Once the spark streaming job is running, showing `Logging Dropped Pairs:`, SSH into the master instance and run the following command to start the producers to send data to the Kafka topics:
   ```bash
    bash start_producers.sh
   ```

Once completed, the visualisation notebook should start showing the live results of the streaming job processing the data from the Kafka topics and writing to MongoDB.

To stop the deployment, run the following command in each instance to stop the master/worker processes and the docker containers:
   ```bash
    bash stop.sh
   ```