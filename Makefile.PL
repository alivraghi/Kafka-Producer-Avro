use Config;
use inc::Module::Install;

license 'MIT';
version_from 'lib/Kafka/Producer/Avro.pm';
readme_from 'lib/Kafka/Producer/Avro.pm';
all_from 'lib/Kafka/Producer/Avro.pm';
build_requires 'Test::More', 0.88;
test_requires 'Math::BigInt';
test_requires 'Test::Exception';
requires 'JSON::XS', 3.02;
requires 'Kafka::Producer', 1.07;
requires 'Avro::BinaryEncoder';
requires 'Avro::Schema';
requires 'Confluent::SchemaRegistry', 0.01;


WriteMakefile(
	NAME => 'Kafka::Producer::Avro',
	AUTHOR => 'Alvaro Livraghi <alvarol@cpan.org>'
);
