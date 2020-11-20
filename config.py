import os.path

# Replace /Users/gaston/python_certificates with the path
# in which you saved the certificate authority file,
# the client certificate file and the client key
#certificates_path = "/Users/gaston/python_certificates"
#ca_certificate = os.path.join(certificates_path, "ca.crt")
#client_certificate = os.path.join(certificates_path, "board001.crt")
#client_key = os.path.join(certificates_path, "board001.key")
# Replace 192.168.1.101 with the IP or hostname for the Mosquitto
# or other MQTT server
# Make sure the IP or hostname matches the value 
# you used for Common Name
mqtt_server_host = "192.168.56.1"
mqtt_server_port = 1883
mqtt_keepalive = 60
db_user = "postgres"
db_password = "admin"
db_host = "127.0.0.1"
db_port = 5432
db_database = "Project"
api_key = 'AIzaSyBanRU8ivzy-31c9fyMsbzyvj2s1cMC8OU'