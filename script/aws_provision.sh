#
#     PROVISION SCRIPT FOR AWS (UBUNTU)
#
#     Provision aws ubuntu 20 image by installing docker, docker-compose and configure some important settings to make the pipeline run.
#     To run: sudo bash script/aws_provision.sh
#     After this step the pipeline automatically starts in production mode.
#

#!/bin/bash

curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg
echo \
  "deb [arch=amd64 signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/ubuntu \
  $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list >/dev/null

apt-get update
apt-get -y install \
  apt-transport-https \
  ca-certificates \
  curl \
  gnupg \
  lsb-release \
  docker-ce docker-ce-cli containerd.io

curl -L "https://github.com/docker/compose/releases/download/1.29.2/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
chmod +x /usr/local/bin/docker-compose


cp /etc/sysctl.conf /tmp/
echo "vm.max_map_count = 262144" >> /tmp/sysctl.conf
sudo cp /tmp/sysctl.conf /etc/

sysctl -w vm.max_map_count=262144

bash script/run_production.sh

exit 0
