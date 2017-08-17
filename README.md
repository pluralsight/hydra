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

### JDBC
A transport implementation that replicates messages into databases via JDBC.

## Building Hydra
Hydra is built using [SBT](http://www.scala-sbt.org/). To build Hydra, run:

```
sbt clean compile
```

## Running Hydra
We provide a `sandbox` module with some simple ingestors that can be used to test Hydra's main functionality.

The walk-through below shows you how to use Hydra with some example Ingetors, by running Hydra in local development mode in SBT. This is *not* an example of usage in production.

### From SBT
You need to have SBT installed.

We are using Spray's SBT [Revolver](https://github.com/spray/sbt-revolver) plugin.  In order to run the example sandbox project, fire up sbt and then type:

```
sbt 
> sandbox/re-start
```

SBT Revolver forks Hydra in a separate process. If you make a code change, simply type re-start again at the SBT shell prompt, it will compile your changes and restart the process. It enables very fast turnaround cycles.

Once Hydra is up and running you can send an HTTP request:

```curl localhost:8080/ingest -d 'this is a test' -H "logging-enabled:true" -H "hydra-file-stream:default" ```

You should see a 'this is a test' message transported (or replicated) to both the sbt console and the ```/tmp/hydra-sandbox.txt``` file.


## Online Documentation
We highly recommend checking out the project documentation [here.](www.pluralsight.com)  There you can find the latest documentation about the ingestion protocol, Akka actors, including examples, API endpoints, and a lot more info on how to get started.

This README file only contains basic definitions and set up instructions.

# Main Features

## Message Delivery Guarantees


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



