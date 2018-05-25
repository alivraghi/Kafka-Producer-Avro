# Confluent-Avro-Producer
Perl module to produce Kafka messages compliant with Confluent Avro serialization and Schema Registry

	
	- $kc = new Kafka::Connection(...)
	- $csr = new Confluent::SchemaRegistry(...)
	- define $topic = <load from resource>
	- $schema = $csr->get_schema($topic, ...)
	- 
	
