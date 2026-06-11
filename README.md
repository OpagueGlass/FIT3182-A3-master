Ubuntu 24.04 LTS
2 vCPU
8GB RAM
Download key pair and save as key.pem

Step 3: Configure Security Group

Open with Inbound Rules:
Port	Purpose
22	SSH (already open by default)
4040	Spark UI
27017	MongoDB

Increase storage to 20GB

SSH into EC2
```bash
ssh -i key.pem ubuntu@<EC2_PUBLIC_IP>
```

Install Docker and Docker Compose
```bash
sudo apt update
sudo apt install docker.io -y
sudo apt install docker-compose-v2 -y
sudo systemctl enable docker
sudo systemctl start docker
```

Clone the repository
```bash
git clone https://github.com/OpagueGlass/FIT3182-A3-master.git
```

Run the application
```bash
cd FIT3182-A3
bash start.sh
```

Git pull for changes
```bash
cd FIT3182-A3
git pull
```