# utils.kafka

https://wiki.inn.ru/pages/viewpage.action?pageId=54267625

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

Replace messages with keys .*(-bns-pro) .*(-bns-epic) from topic achievements-statejournal to topic bns-statejournal
```
go run -in=achievements-statejournal -out=bns-statejournal -brokers=kafka1:9092 kafka2:9092 kafka3:9092 -expected=.*(-bns-pro) .*(-bns-epic)

```

Delete messages with keys .*(-bns-pro) .*(-bns-epic) from topic achievements-statejournal
```
go run -in=achievements-statejournal -brokers=kafka1:9092 kafka2:9092 kafka3:9092 -expected=.*(-bns-pro) .*(-bns-epic) -delete

```

Print in logs consumed messages with keys .*(-bns-pro) .*(-bns-epic) from topic achievements-statejournal
```
go run -in=achievements-statejournal -brokers=kafka1:9092 kafka2:9092 kafka3:9092 -expected=.*(-bns-pro) .*(-bns-epic) -test

```

