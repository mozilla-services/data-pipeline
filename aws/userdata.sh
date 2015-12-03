#!/bin/bash

#sudo su -
# As of 2015/09/10, build requires geoip >= 1.6.3.
# Add a PPA for recent versions.
add-apt-repository ppa:maxmind/ppa -y

apt-get update
apt-get --yes install mdadm xfsprogs jq git python-pip python-protobuf cmake libgeoip-dev zlib1g-dev mercurial debhelper libpq-dev libssl-dev
pip install awscli boto
umount /mnt
yes | mdadm --create /dev/md0 --level=0 -c64 --raid-devices=2 /dev/xvdb /dev/xvdc
echo 'DEVICE /dev/xvdb /dev/xvdc' >> /etc/mdadm/mdadm.conf
mdadm --detail --scan >> /etc/mdadm/mdadm.conf
mkfs.xfs /dev/md0
mount /dev/md0 /mnt
mkdir -p /mnt/work
chown -R ubuntu:ubuntu /mnt/work

cd /mnt/work
wget https://storage.googleapis.com/golang/go1.4.2.linux-amd64.tar.gz
tar -C /usr/local -xzf go1.4.2.linux-amd64.tar.gz

wget http://people.mozilla.org/~mreid/heka-data-pipeline-linux-amd64.tar.gz
tar xzvf heka-data-pipeline-linux-amd64.tar.gz

echo "ubuntu hard nofile 200000" >> /etc/security/limits.conf
echo "ubuntu soft nofile 50000" >> /etc/security/limits.conf
