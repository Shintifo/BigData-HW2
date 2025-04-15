#!/bin/bash
service ssh restart

bash start-services.sh

python3 -m venv .venv
source .venv/bin/activate

pip install -r requirements.txt

venv-pack -o .venv.tar.gz


# Create virtual environment
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
venv-pack -o .venv.tar.gz

FILES=($(hdfs dfs -ls /data | awk '{print $NF}' | tail -n +2 | head -n 100))

for hdfs_file in "${FILES[@]}"
do
    bash index.sh "$hdfs_file"

    if [ $? -ne 0 ]; then
        echo "Failed to process $hdfs_file"
        exit 1
    fi
done

echo "Successfully processed 100 files:"
