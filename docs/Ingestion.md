# Ingestion Endpoints and Protocols

## Web Sockets

Hydra supports web sockets for event streaming with semantics are very similar to the HTTP endpoint, with a few differences:

#### Request Metadata

These can be specified at connection time via HTTP headers beginning with `"hydra-***"` or after the socket has 
connected by issuing `set` commands.

#### Request Payload
Once the metadata for the request has been set, clients can send payloads directly to the socket, without having to 
re-set any request metadata for every request. 

### Using the web socket connector

#### Enable the endpoint

Add this entry to the config:

```$xslt
hydra.ingest.websocket.enabled = true
```

The socket endpoint will be available at ```ws://[host]/ws-ingest```

### URL Format

| Path                      | Description                                                                                                        |
|---------------------------|--------------------------------------------------------------------------------------------------------------------|
| ws-ingest                 | Does not associate the request to any specific ingestor. <br> Events will be broadcast to all registered ingestors.     |
| ws-ingest/[name] | Associates the socket to a specific ingestor. <br> Events are not broadcast and are published to this ingestor only. |


### Communicating with the Socket

<div class="alert alert-success">
Web socket commands start with a ```-c``` switch.
</div>

#### `SET` command
Used to set request metadata.

**Syntax:**

To set request metadata:

```
-c SET hydra-kafka-topic = test.Topic
```

To list the current request metadata:

```
-c SET 
```

#### `HELP` command
Used to get a list of all available commands.

#### Ingestion

Sending any string without a command switch ```-c``` will initiate the ingestion protocol for that payload.

Ingestion payloads can include an optional request id, by prefixing the payload with a ```-i``` switch. For instance:

```-i 122 {"name":"test","value":"test"}```

### Sample Interaction
```bash
/connect ws://localhost:8080/ws-ingest/kafka_ingestor

-c set hydra-kafka-topic = test-topic
sent:	-c set hydra-kafka-topic = test-topic
response: {"status":200,"message":"OK[HYDRA-KAFKA_TOPIC=test-topic]"}

-c set hydra-ack = explicit
sent: -c set hydra-ack = explicit
response: {"status":200,"message":"OK[HYDRA-ACK=explicit]"}

{"name":"test","value":"test"}
sent: {"name":"test","value":"test"}

response: {"requestId":"GrnuqJBX","status":{"code":200,"message":"OK"},"ingestors":{"kafka_ingestor":{"code":200,"message":"OK"}}}
```


### Clients
You can use any client or library to communicate with the socket.

####Browser

1. [Chrome Smart Websocket Client](https://chrome.google.com/webstore/detail/smart-websocket-client/omalebghpgejjiaoknljcfmglgbpocdp?hl=en-US)

2. [Dark Socket Web Terminal](https://chrome.google.com/webstore/detail/dark-websocket-terminal/dmogdjmcpfaibncngoolgljgocdabhke?hl=en)

####Native

**Scala**

[Akka HTTP](http://doc.akka.io/docs/akka-http/current/scala.html)

**Python**

[websocket-client](https://pypi.python.org/pypi/websocket-client)


