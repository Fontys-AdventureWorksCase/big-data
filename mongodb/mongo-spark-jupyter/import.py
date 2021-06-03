import os
import subprocess

directory="/import/reaguursels/"
for filename in os.listdir(directory):
    print(filename)
    # mongoimport c:\data\books.json -d bookdb -c books --drop
    try:
        command = "mongoimport " +directory+filename+" -h rs0/mongo1:27017,mongo2:27018,mongo3:27019 -d reaguursels -c reaguursels --drop"
        result = subprocess.run(command, check=True, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    except subprocess.CalledProcessError as err:
        raise Exception(str(err.stderr.decode("utf-8")))
    except Exception as err:
        raise Exception(err)
