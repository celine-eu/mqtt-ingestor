# MQTT ingestor

Store MQTT messages to database (via `mongodb`, `postgres` or `sqlalchemy`)

## Usage

### Docker 

`docker run -v $PWD/.env:/.env --rm ghcr.io/celine-eu/mqtt-ingestor:latest`

### python API

```py
from mqtt_ingestor.api import MqttIngestor

ingestor = MqttIngestor()
ingestor.start()
```

## Configuration

```sh
# log level
LOG_LEVEL=INFO

# broker hostname
MQTT_BROKER=mqtt
MQTT_PORT=1883
MQTT_USER=mqtt_user
MQTT_PASS=secretpass
# enable TLS support
MQTT_TLS=0
# transport: tcp or websockets
MQTT_TRANSPORT=tcp
# list of topics, separated by comma
MQTT_TOPICS='#'

# backend selection: postgres, mongodb or sqlalchemy
STORAGE_BACKEND=postgres

# postgres DSN
POSTGRES_DSN=postgresql://postgres:postgres@postgres:5432/mqtt
# postgres table
POSTGRES_TABLE=mqtt_messages

# sqlalchemy DSN
SQLALCHEMY_DSN=postgresql+psycopg2://postgres:postgres@postgres:5432/mqtt
# sqlalchemy table
SQLALCHEMY_TABLE=mqtt_messages

# mongodb connection URI
MONGO_URI=mongodb://mongo:27017/
# mongodb database
MONGO_DB=mqtt_data
# mongodb collection
MONGO_COLLECTION=data
```


## LICENSE

Copyright >=2025 Spindox Labs

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
