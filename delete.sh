make env;
sudo docker ps -a | grep Exit | cut -d ' ' -f 1 | xargs sudo docker rm ;
sudo docker rmi $(sudo docker images | grep "^<none>" | awk "{print $3}") ;
sudo docker volume rm $(sudo docker volume ls -qf dangling=true);
make env;
make run
