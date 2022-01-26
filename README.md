# Hydra

[![Build Status](https://travis-ci.org/pluralsight/hydra.svg?branch=master)](https://travis-ci.org/pluralsight/hydra)
[![codecov](https://codecov.io/gh/pluralsight/hydra/branch/master/graph/badge.svg)](https://codecov.io/gh/pluralsight/hydra)
[![Join the chat at https://gitter.im/pluralsight/hydra](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/pluralsight/hydra?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)


The Hydra platform provides a streamlined data streaming experience by abstracting away the underlying implementation, instead providing users a simple REST API.
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

Hydra uses Zookeeper as a coordination service to automate bootstrapping and joining a cluster.

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
  "BuildInfo": {
    "builtAtMillis": "1639518050466",
    "name": "hydra-ingest",
    "scalaVersion": "2.12.11",
    "version": "0.11.3.979",
    "sbtVersion": "1.3.13",
    "builtAtString": "2021-12-14 21:40:50.466"
  },
  "ConsumerGroupisActive": {
    "ConsumerGroupName": "v2MetadataConsumer",
    "State": true
  }
}
```

## Available Endpoints

### Container Metadata
| Path     | HTTP Method | Description                                                                                                                        |
|----------|-------------|------------------------------------------------------------------------------------------------------------------------------------|
| /health  | GET         | A summary overview of the overall health of the system.  Includes health checks for Kafka.                                         |
| /metrics | GET         | A collection of JVM-related memory and thread management metrics, including deadlocked threads, garbage collection run times, etc. |

### Schema Endpoints
| Path | HTTP Method | Description |
|------------------------------------|-------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| /schemas | GET | Returns a list of all schemas managed by Hydra. |
| /schemas/[NAME] | GET | Returns information for the **latest** version of a schema. |
| /schemas/[NAME]?schema | GET | Returns _only_ the JSON for the latest schema. |
| /schemas/[NAME]/versions/ | GET | Returns all versions for a schema. |
| /schemas/[NAME\/versions/[VERSION] | GET | Returns metadata for a specific version of a schema |
| /schemas | POST | Registers a new schema with the registry. Use this for both new and existing schemas; for existing schemas, compatibility will be checked prior to registration. |

### Topics Endpoints
| Path       | HTTP Method | Description                                                                          |
|------------|-------------|--------------------------------------------------------------------------------------|
| /v2/topics | GET         | A list of all the registered topics currently managed by Hydra.  
| /v2/topics/[NAME] | GET  | Get the current schema for requested topic.
| /v2/topics/[NAME] | POST        | Create or update custom topics. Also registers key and value schemas in Schema Registry if applicable. |
| /v2/topics/[NAME] | DELETE      | Delete topics. Requires authentication.

### Records Endpoints
| Path                      | HTTP Method   | Description
|---------------------------|---------------|------------------------------
| /v2/topics/[NAME]/records | POST          | Creates a new record in the specified topic.

# Taking your Hydra for a Walk

*"If you want to make a Kafka topic from scratch, you must first invent the universe."* -Carl Sagan

## Create & Register a Topic

We are using this topic to test:

```json
{
  "streamType": "Entity",
  "deprecated": false,
  "dataClassification": "InternalUseOnly",
  "contact": {
    "email": "john.doe@email.com"
  },
  "createdDate": "2022-01-25T12:00:00Z",
  "notes": "Here are some notes.",
  "parentSubjects": [],
  "teamName": "team-john-doe",
  "schemas": {
    "key": {
      "type": "record",
      "name": "key",
      "namespace": "",
      "fields": [
        {
          "name": "id",
          "type": {
            "type":"string",
            "logicalType":"uuid"
          },
          "doc":"This is a doc field."
        }
      ]
    },
    "value": {
      "type": "record",
      "name": "val",
      "namespace": "dvs.data_platform.dvs_sandbox",
      "fields": [
        {
          "name": "myValue",
          "type": "string",
          "doc":"It is my value."
        }
      ]
    }
  }
}
```

### Post it to Hydra
```bash
curl -X POST localhost:8088/topics/tech.my-first-topic -d '{
  "streamType": "Entity",
  "deprecated": false,
  "dataClassification": "InternalUseOnly",
  "contact": {
    "email": "john.doe@email.com"
  },
  "createdDate": "2022-01-25T12:00:00Z",
  "notes": "Here are some notes.",
  "parentSubjects": [],
  "teamName": "team-john-doe",
  "schemas": {
    "key": {
      "type": "record",
      "name": "Test2",
      "namespace": "",
      "fields": [
        {
          "name": "id",
          "type": {
            "type":"string",
            "logicalType":"uuid"
          },
          "doc":"This is a doc field."
        }
      ]
    },
    "value": {
      "type": "record",
      "name": "Test2",
      "namespace": "dvs.data_platform.dvs_sandbox",
      "fields": [
        {
          "name": "myValue",
          "type": "string",
          "doc":"It is my value."
        }
      ]
    }
  }
}'
```
You should see something like this:

```
OK
```
### Feeding Your Hydra (with HTTP)

```bash
 curl -X POST -d '{"key": {"id":"7db11b7a-4560-4a86-b00b-6f380bfb1564"}, "value":{"myValue":"someValue"}}' -H 'Content-Type: application/json'  'http://localhost:8088/v2/topics/tech.my-first-topic/records'
 ```

A sample Hydra response is:

```
{"offset":0,"partition":6}
```


### Message Validation
Hydra validates payloads against the underlying schema.  For instance:

```bash
curl -X POST -d '{"key": {"id":"123"}, "value":{"myValue":"someValue"}}' -H 'Content-Type: application/json'  'http://localhost:8088/v2/topics/tech.my-first-topic/records'
```

Should return:
```
hydra.avro.convert.StringToGenericRecord$InvalidLogicalTypeError: Invalid logical type. 
Expected UUID but received 123 
[http://schema-registry:8081/subjects/tech.my-first-topic-key/versions/latest/schema]
```

##But what about v1?
<img src="https://i.imgflip.com/62t69n.jpg"/>

You may have noticed that the JSON above uses "/v2" endpoints. 
Those of you capable of extrapolating from incomplete data sets may have questions about "/v1" endpoints. 
They exist, but are deprecated. 
If you prefer to use "/v1" endpoints, simply remove the "/v2" segment.

# Online Documentation
We used to highly recommend checking out the project documentation [here](https://hydra-ps.atlassian.net/wiki/spaces/DES/overview), but then we forgot about it for two years. 
We might get around to updating it in the future. 

There you can find the "latest"
<a href='https://c.tenor.com/0yi68Ri2JS4AAAAC/jameson-laugh.gif'><img id='jjonahjameson' style='height:30px; width: 30px' src='https://c.tenor.com/0yi68Ri2JS4AAAAC/jameson-laugh.gif' /></a> documentation about the Hydra project, including examples, API endpoints, and a lot more info on how to get started.

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



