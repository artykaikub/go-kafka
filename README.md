# go-kafka

Wrapper of Shopify/sarama that make it easier for use. Hit the project with a star if you find it useful ⭐.  

This package is made to make Kafka easier to use. This package wrapper lib sarama and some tweaks make it easier for users to execute and end commands in a single command. Including adjusting to be able to use TLS easily in either string or file format.

## Environment

```sh
KAFKA_SASL_ENABLED=
KAFKA_BROKERS=
KAFKA_VERBOSE=
KAFKA_USERNAME=
KAFKA_PASSWORD=
KAFKA_CLIENT_ID=
```

## How to use

**Consumer**  
The example in this folder [consumergroup](https://github.com/artykaikub/go-kafka/tree/main/kafka/example/consumergroup). In this folder, you will find examples of various consumer usage and you can customize different consumers with the option functions that this package provides.

**Producer**  
This exampe in this folder [producer](https://github.com/artykaikub/go-kafka/tree/main/kafka/example/producer). In this folder, you will find examples of various producer usage and you can customize different producer with the option functions that this package provides. It has both sync and async functions of the producer. It also supports converting partitioner from sarama package to use in this package.
