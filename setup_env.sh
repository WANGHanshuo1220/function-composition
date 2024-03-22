sudo apt-get update --yes
sudo apt-get upgrade --yes
sudo apt-get install python3-pip --yes
sudp apt-get install imagemagick libmagickwand-dev --yes
sudo apt-get install python3-opencv --yes

sudo pip install -r requirements.txt --yes

mkdir ~/.aws
touch ~/.aws/config
echo "[default]" >> ~/.aws/config
echo "region = ap-northeast-1" >> ~/.aws/config

touch ~/.aws/credentials
echo "[default]" >> ~/.aws/credentials
echo "aws_access_key_id = $AWS_ACCESS_KEY_ID >> ~/.aws/credentials
echo "aws_secret_access_key = $AWS_SECRET_ACCESS_KEY" >> ~/.aws/credentials

export FC_HOST_IP="172.31.44.66"
