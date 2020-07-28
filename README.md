# Hydra

[![Build Status](https://travis-ci.org/pluralsight/hydra.svg?branch=master)](https://travis-ci.org/pluralsight/hydra)
[![codecov](https://codecov.io/gh/pluralsight/hydra/branch/master/graph/badge.svg)](https://codecov.io/gh/pluralsight/hydra)
[![Join the chat at https://gitter.im/pluralsight/hydra](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/pluralsight/hydra?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)


Hydra is a real-time streaming and data replication platform that "unbundles" the receiving, transforming, 
and production of data streams.

It does so by abstracting these phases independently from each other while providing a unifying API across them.

## Why Replication?
The goal behind Hydra's replication protocol is to separate the ingestion of events from any transformation and storage.

This replication paradigm allows a single event to be ingested, transformed, and then replicated in real-time to several different data systems (Kafka, Postgres, etc.)


## Replication Phases

### Receive
The Receive phase receives "raw" data, converts it to a Hydra request containing a payload and metadata, and broadcasts the request to the underlying Hydra Ingestors.

### Ingestion
This phase involves matching the request with one or more ingestors, performing any data transformation and sending the request to a Transport.  Complex streaming operations, such as joins, cross-stream aggregations are outside the scope of the ingestion phase.

### Transport
The transport phase of the protocol the step at which the events are actually sent (produced) to the underlying data system.

## Hydra Modules

### Common
A set of common util-based classes meant to be shared across the entire Hydra ecosystem, including hydra-spark and dispatch modules. 

### Core
Core, shared traits and classes that define both the ingestion and transport protocols in Hydra.  This module is a dependency to any ingestor or transport implementation.

This also includes Avro schema resolution, validation, and management.

### Ingest
The ingestion implementation, including HTTP endpoints, actors, communication protocols, registration of ingestors, and a lot of other stuff.

### Kafka
A Transport implementation that replicates messages into Kafka.

## Building Hydra
Hydra is built using [SBT](http://www.scala-sbt.org/). To build Hydra, run:

```
sbt clean compile
```

## Docker

### Services needed to run Hydra 
- Kafka 2.0.0
- Confluent Schema Registry 5.0.0
- Zookeeper (3.x +)

This documentation walks through setting up the core basic components of Hydra.

### Create a VirtualBox instance

```
docker-machine create --driver virtualbox --virtualbox-memory 6000 hydra
```

### Configure Terminal to attach to the new machine

```
docker-machine env hydra
```

### Create a Docker network

```
docker network create hydra
```

### Start Zookeeper

Hydra uses Zookeeper as a coordination service to automate bootstrapping or joining a cluster.

It is also used by Kafka and the Schema Registry.

Since all services depend on Zookeeper being up, so we will start that first.  It is not always 
needed to do this, but doing so avoids race conditions tht may happen across the different containers.

```
docker-compose up -d zookeeper
```

### Start Hydra

```
docker-compose up hydra
```

> You can also start each service separately.

That should do it!

# Checking if Hydra is Running

You can test Hydra has started by going to this resource:

```http://localhost:8088/health```

You should see something like:
```json
{
	"host": "40f4ccc69ad4",
	"applicationName": "Container Service",
	"applicationVersion": "1.0.0.N/A",
	"containerVersion": "2.0.5.000",
	"time": "2017-04-03T15:18:48Z",
	"state": "OK",
	"details": "All sub-systems report perfect health",
	"checks": [{
		"name": "services",
		"state": "OK",
		"details": "Currently managing 8 services",
		"checks": []
	}, {
		"name": "Kafka",
		"state": "OK",
		"details": "",
		"checks": []
	}, {
		"name": "metrics-reporting",
		"state": "OK",
		"details": "The system is currently not managing any metrics reporters",
		"checks": []
	}, {
		"name": "http",
		"state": "OK",
		"details": "Currently connected on /0:0:0:0:0:0:0:0:8088",
		"checks": []
	}]
}
```

## Available Endpoints

### Container Metadata
| Path     | HTTP Method | Description                                                                                                                        |
|----------|-------------|------------------------------------------------------------------------------------------------------------------------------------|
| /health  | GET         | A summary overview of the overall health of the system.  Includes health checks for Kafka.                                         |
| /metrics | GET         | A collection of JVM-related memory and thread management metrics, including deadlocked threads, garbage collection run times, etc. |


### Ingestion Endpoints
| Path       | HTTP Method | Description                                                                          |
|------------|-------------|--------------------------------------------------------------------------------------|
| /ingestors | GET         | A list of all the registered ingestors currently managed by Hydra.                   |
| /ingestors | POST        | Allows creation of custom (stateful ingestors.) Not yet available; version 0.8 only. |
| /ingest    | POST        | Real-time ingestion and stream replication endpoint.  More info below.               |

### Schema Endpoints
| Path | HTTP Method | Description |
|------------------------------------|-------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| /schemas | GET | Returns a list of all schemas managed by Hydra. |
| /schemas/[NAME] | GET | Returns information for the **latest** version of a schema. |
| /schemas/[NAME]?schema | GET | Returns _only_ the JSON for the latest schema. |
| /schemas/[NAME]/versions/ | GET | Returns all versions for a schema. |
| /schemas/[NAME\/versions/[VERSION] | GET | Returns metadata for a specific version of a schema |
| /schemas | POST | Registers a new schema with the registry. Use this for both new and existing schemas; for existing schemas, compatibility will be checked prior to registration. |

### Transport Endpoints
| Path                                     | HTTP Method | Description                                                                                                                                                                                                                                                                                                                                                    |
|------------------------------------------|-------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| /transports/kafka/topics                 | GET         | Returns a list of all Kafka topics, including leader and ISR information.                                                                                                                                                                                                                                                                                      |
| /transports/kafka/topics?names           | GET         | Returns a list of all topic names in Kafka.                                                                                                                                                                                                                                                                                                                    |
| /transports/kafka/streaming/[TOPIC_NAME] | GET         | Creates an HTTP streaming response for a given Kafka topic, streaming from the latest offset. **Experimental.**  **Request Parameters:** - _group_ -  the group id for the request ('hydra' used by default.) - _format_ -  the topic format (defaults to 'avro'). - _ttl_ - The time to keep the stream alive if no records are received. (Defaults to 60 s.) |
| /transports/kafka/consumers              | POST        | Creates a new consumer. **Not available yet; will be part of Hydra 0.9.0.**                                                                                                                                                                                                                                                                                    |


# Taking the beast to a test run

The first step to ingest messages is to create and register an Avro schema.

## Create/Register a schema

We are using this schema to test:

```json
{
	"type": "record",
	"namespace": "HydraTest",
	"name": "exp.eng.docker",
	"fields": [{
		"name": "name",
		"type": "string"
	}, {
		"name": "age",
		"type": "int"
	}]
}
```

### Post it to Hydra
```bash
curl -X POST localhost:8088/schemas -d '{ "type": "record", "name": "HydraTest", "namespace": "exp.eng.docker", "fields": [{ "name": "name", "type": "string" }, { "name": "age", "type": "int" }] }'
```
You should see something like this:

```
{
	"id": 1,
	"version": 1,
	"schema": "{ \"type\": \"record\", \"name\": \"HydraTest\", \"namespace\": \"exp.eng.docker\", \"fields\": [{ \"name\": \"name\", \"type\": \"string\" }, { \"name\": \"age\", \"type\": \"int\" }] }"
}
```

> You can also use any of the schemas endpoints above to interact with the schema registry.


### Sending a message through HTTP

```bash
 curl -X POST -H "Hydra-Kafka-Topic: exp.eng.docker.HydraTest"  -d  '{"name":"test","age":10}' "http://localhost:8088/ingest"
```

> The schema is looked up from the schema registry using the Kafka topic name.  For topics with high throughout, it is best to provide the schema by using the "hydra-schema" header, as below:

```bash
 curl -X POST -H "Hydra-Kafka-Topic: exp.eng.docker.HydraTest" -H "Hydra-Schema: exp.eng.docker.HydraTest#1"  -d  '{"name":"test","age":10}' "http://localhost:8088/ingest"
```


> The format of the hydra-schema header is ```[schema_name]#[schema_version]```.

A sample ingestion response is:

```
{
	"requestId": "CawatHr1",
	"status": {
		"code": 200,
		"message": "OK"
	},
	"ingestors": {
		"kafka_ingestor": {
			"code": 200,
			"message": "OK"
		}
	}
}
```


### Message Validation
Hydra validates payloads against the underlying schema.  For instance:

```bash
 curl -X POST -H "Hydra-Kafka-Topic: exp.eng.docker.HydraTest"  -d  '{"name":"test"}' "http://localhost:8088/ingest"
```

Should return:
```
{
	"requestId": "mpTwYTsG",
	"status": {
		"code": 400,
		"message": "Bad Request"
	},
	"ingestors": {
		"kafka_ingestor": {
			"code": 400,
			"message": "com.pluralsight.hydra.avro.RequiredFieldMissingException: Field age (Type INT) is required, but it was not provided. [http://schema-registry:8081/ids/1]"
		}
	}
}
```



## Streaming (HTTP) from the Kafka Topic

The `streaming` resource allows consumption of Kafka in real-time. 

Example:

```bash
http://localhost:8088/transports/kafka/streaming/exp.eng.docker.HydraTest
```

You can leave a window/tab open in your browser and messages sent to that topic will stream through Hydra automatically.


# Online Documentation
We highly recommend checking out the project documentation [here.](https://hydra-ps.atlassian.net/wiki/spaces/DES/overview)  There you can find the latest documentation about the ingestion protocol, Akka actors, including examples, API endpoints, and a lot more info on how to get started.

This README file only contains basic definitions and set up instructions.

# Contribution and Development
Contributions via Github Pull Request are welcome.  

Profiling software provided by ![](https://www.yourkit.com/images/yklogo.png)

YourKit supports open source projects with its full-featured Java Profiler.
YourKit, LLC is the creator of <a href="https://www.yourkit.com/java/profiler/">YourKit Java Profiler</a>
and <a href="https://www.yourkit.com/.net/profiler/">YourKit .NET Profiler</a>,
innovative and intelligent tools for profiling Java and .NET applications.

## Contact
Try using the gitter chat link above!

## License
Apache 2.0, see LICENSE.md



