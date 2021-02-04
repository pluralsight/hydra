# Hydra

[![Build Status](https://travis-ci.org/pluralsight/hydra.svg?branch=master)](https://travis-ci.org/pluralsight/hydra)
[![codecov](https://codecov.io/gh/pluralsight/hydra/branch/master/graph/badge.svg)](https://codecov.io/gh/pluralsight/hydra)
[![Join the chat at https://gitter.im/pluralsight/hydra](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/pluralsight/hydra?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

Hydra is an HTTP interface that can be used to work with Kafka. The main function is to ingest records into Kafka topics, but creation of topics and metadata management are other functionalities provided by Hydra.

# Getting Started

## Running

To run Hydra, you can use `sbt ingest/run`. This will boot an HTTP server on port 8080 by default. The only dependencies for running Hydra are Kafka and Confluent Schema Registry.

## Tests

You can run all of the tests with `sbt test`. You can also run tests for only specific modules with `sbt kafka/test`, for example. Integration tests can be run with `sbt it:test`.

## Settings

You can override Hydra configuration settings using environment variables. For an example, see [example.env](example.env).

# Versions

There are two versions of Hydra's API, V1 and V2. V1 creates topics and ingests records with a String key and Avro value. V2 creates topics and ingests records with Avro keys and values.

# Contribution and Development
Contributions via Github Pull Request are welcome.

## Contact
Try using the gitter chat link above!

## License
Apache 2.0, see LICENSE.md
