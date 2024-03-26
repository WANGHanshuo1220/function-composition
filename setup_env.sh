sudo apt-get update --yes
sudo apt-get upgrade --yes
sudo apt-get install python3-pip --yes
sudp apt-get install imagemagick libmagickwand-dev --yes
sudo apt-get install python3-opencv --yes
sudo apt-get install protobuf-compiler --yes
sudo apt-get install python3-tk --yes

sudo pip install -r requirements.txt --yes

cd ~
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
unzip awscliv2.zip
sudo ./aws/install
rm awscliv2.zip

aws --version
aws configure

export FC_HOST_IP="localhost"
