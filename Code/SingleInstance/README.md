docker build -t agustinnormand/stations:18 .

docker push agustinnormand/stations:18


docker run -d --volume=/home/agustin/stations/results/:/usr/src/app/results --volume=/home/agustin/stations/log/:/usr/src/app/log agustinnormand/stations:18
