from mqtt_ingestor.storage.noop import NoopStorage


def test_save_does_not_raise(sample_doc):
    storage = NoopStorage()
    storage.save(sample_doc)


def test_close_does_not_raise():
    storage = NoopStorage()
    storage.close()
