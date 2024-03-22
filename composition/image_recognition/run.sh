./clean.sh

python3 upload.py
python3 extract_mdata.py
python3 preprocess.py
python3 recognition.py
python3 mosaic.py

./clean.sh
