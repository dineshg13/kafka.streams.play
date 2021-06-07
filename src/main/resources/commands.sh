bin/kafka-storage.sh format -t nadK8yTRRoW0HyHPnycTAA -c ./config/kraft/server.properties

bin/kafka-server-start.sh config/kraft/server.properties


bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic dedupe-in --partitions 32
bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic dedupe-out --partitions 32

bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic dedupe-in