# RabbitMQ Ingestion and Transport

The rabbitmq module ingests messages and forwards the payload to a RabbitMQ instance without modifying the payload.

## Usage
There is a sample configuration in this module's `reference.conf` but more details about the configuration can be found at https://github.com/SpinGo/op-rabbit#usage

### Enable the Rabbit ingestor and start app
Add the hydra-rabbit module to your classpath; it will be loaded dynamically when Hydra starts.

In order to use the Rabbit ingest, first start hydra ingestors by running hydra.app.Main.

#### Sample using cURL with HTTP Ingest
To send to an exchange, use the hydra-rabbit-exchange header:
```
curl -X POST http://localhost:8080/ingest \
  -H "hydra-rabbit-exchange:test.exchange"
  -H "hydra-ack:transport"
  -d '{"name": "001c000001nlucqiaf", "handle": "152cba6e"}'
```

To send to a queue, use the hydra-rabbit-queue header:
```
curl -X POST http://localhost:8080/ingest \
  -H "hydra-rabbit-queue:test.queue"
  -H "hydra-ack:transport"
  -d '{"name": "001c000001nlucqiaf", "handle": "152cba6e"}'
```

#### Sample Response


### Rabbit libraries used
This module makes use of the RabbitMQ library[Op-Rabbit](https://github.com/SpinGo/op-rabbit)