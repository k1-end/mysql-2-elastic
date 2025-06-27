json_config=$(cat config.json)
user=$(echo "$json_config" | jq -r '.database.username')
password=$(echo "$json_config" | jq -r '.database.password')
port=$(echo "$json_config" | jq -r '.database.port')
host=$(echo "$json_config" | jq -r '.database.host')
world_database_path=$(echo "$json_config" | jq -r '.world_database_path')
mysql --host=$host --user=$user --password=$password --port=$port < $world_database_path 
