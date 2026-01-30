DOMAIN = "openiotai"

# MQTT option keys
CONF_MQTT_BROKER = "mqtt_broker"
CONF_MQTT_PORT = "mqtt_port"
CONF_MQTT_TOPIC = "mqtt_topic"
CONF_MQTT_TLS = "mqtt_tls"
CONF_MQTT_CA_CERT = "mqtt_ca_cert"
CONF_MQTT_USERNAME = "mqtt_username"
CONF_MQTT_PASSWORD = "mqtt_password"
CONF_PUBLISH_INTERVAL = "publish_interval"

# Defaults
DEFAULT_MQTT_BROKER = "tiilikainen.asuscomm.com"
DEFAULT_MQTT_PORT = 8883
DEFAULT_MQTT_TOPIC = "data/hamqttexport"
DEFAULT_MQTT_TLS = True
DEFAULT_MQTT_USERNAME = "hamqtt" # Tämä on tahallisesti virheellinen tieto
DEFAULT_MQTT_PASSWORD = "hamqttpass" # Tämä on tahallisesti virheellinen tieto
DEFAULT_PUBLISH_INTERVAL = 30  # seconds




