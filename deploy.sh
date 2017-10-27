count=$(ps aux | grep -c kafka-test)
n=1

if [ $count -gt $n ]
then
  echo "killing active process"
  pkill -f kafka-test 
else
  echo "no process is running"
fi

echo "starting new process in background"

nohup java -jar -Dswarm.bind.address=172.31.22.34 -Dswarm.port.offset=201 -jar kafka-test-swarm.jar &
