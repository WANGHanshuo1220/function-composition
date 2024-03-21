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
echo "aws_access_key_id = AKIATENRELP3YJPFMBFM" >> ~/.aws/credentials
echo "aws_secret_access_key = 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" >> ~/.aws/credentials

export FC_HOST_IP="172.31.44.66"