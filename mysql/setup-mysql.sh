while true
do
  if docker exec -it mysql-hive mysql -uroot -proot -e "source /setup_script.sql" | grep -q '1050';
  then
    break
  else
    sleep 3
    echo 'retrying setup metastore in mysql'
  fi
done

echo "mysql setup done!"
