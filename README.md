# utils.kafka

State importer Tool
=====================

Allows to replace messages from one Kafka's topic to another /  delete messages from Kafka's topic

Last consumed offsets save in file. 
File name when messages replaced : {topicName}-delete_offsets-{hash of regular expressions string (expext)}-.json
File name when messages deleted  : {topicName}-copy_offsets-{hash of regular expressions string (expext)}-.json

## Arguments

Argument           | Required | Description
-------------------|----------|------------
-in                | yes      | Topic for consumed messages
-out               | no       | Topic for produced messages
-brokers           | yes      | The Kafka brokers to connect to (separate by space)
-expect            | yes      | Regular expressions for select messages
-delete           | no       | Delete messages from consumed topic
-test              | no       | Messages consumed only and print in logs

## Example

Replace messages with keys .*(-first-key) .*(-second-key) from topic first-statejournal to topic second-statejournal
```
go run -in=first-statejournal -out=second-statejournal -brokers=kafka1:9092 kafka2:9092 kafka3:9092 -expected=.*(-first-key) .*(-second-key)

```

Delete messages with keys .*(-first-key) .*(-second-key) from topic first-statejournal
```
go run -in=first-statejournal -brokers=kafka1:9092 kafka2:9092 kafka3:9092 -expected=.*(-first-key) .*(-second-key) -delete

```

Print in logs consumed messages with keys .*(-first-key) .*(-second-key) from topic first-statejournal
```
go run -in=first-statejournal -brokers=kafka1:9092 kafka2:9092 kafka3:9092 -expected=.*(-first-key) .*(-second-key) -test

```

