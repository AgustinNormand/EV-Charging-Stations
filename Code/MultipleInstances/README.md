docker build -t agustinnormand/multiple_stations:3 .

docker push agustinnormand/multiple_stations:3


sudo docker run -d --volume=/home/agustin/stations/results/:/usr/src/app/results --volume=/home/agustin/stations/log/:/usr/src/app/log agustinnormand/multiple_stations:3 4 0

mkdir stations
mkdir stations/results
mkdir stations/log

tail -f stations/log/application.log

cat stations/log/application.log | grep Error
cat stations/log/application.log | grep Sleeping

ls stations/results/ | wc -l