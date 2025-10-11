from mqtt_ingestor.api import MqttIngestor


def main():
    ingestor = MqttIngestor()
    ingestor.start()


if __name__ == "__main__":
    main()
