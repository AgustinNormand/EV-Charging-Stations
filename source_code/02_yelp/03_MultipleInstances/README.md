IN HOST:
docker build -t agustinnormand/multiple_stations:17 .

docker push agustinnormand/multiple_stations:17

IN VM:

(If not installed)
curl -fsSL get.docker.com -o get-docker.sh && sh get-docker.sh

(If Exists)
sudo rm -r stations

mkdir stations
mkdir stations/results
mkdir stations/log

sudo docker run -d --volume=/home/agustin/stations/results/:/usr/src/app/results --volume=/home/agustin/stations/log/:/usr/src/app/log agustinnormand/multiple_stations:17 4 0

tail -f stations/log/application.log

head stations/log/application.log

cat stations/log/application.log | grep "ERROR:"
cat stations/log/application.log | grep Sleeping

cat stations/log/application.log | grep "offset 950, Total in response 50"

cat stations/log/application.log | grep "offset 0, Total in response 0"

head stations/log/application.log

ls stations/results/ | wc -l

tar -zcvf instance-1.tar.gz ./stations/
tar -zcvf instance-2.tar.gz ./stations/
tar -zcvf instance-3.tar.gz ./stations/
tar -zcvf instance-4.tar.gz ./stations/
