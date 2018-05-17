#!/bin/env perl

use strict;
use warnings;

use Data::Dumper;
$Data::Dumper::Purity = 1;
$Data::Dumper::Terse = 1;
$Data::Dumper::Useqq = 1;
use IO::String;

use Avro::BinaryDecoder;
use Avro::BinaryEncoder;
use Avro::DataFile;
use Avro::DataFileReader;
use Avro::DataFileWriter;
#use Avro::Protocol;
use Avro::Schema;

use Kafka qw($BITS64);
use Kafka::Connection;
use Kafka::Producer;

use constant MAGIC_BYTE => 0;

use Confluent::SchemaRegistry;

my $schema_registry = Confluent::SchemaRegistry->new();

my $schema1 = {
                "fields" => [
                              {
                                "name" => "f1",
                                "type" => "string"
                              }
                            ],
                "name" => "myrecord",
                "type" => "record"
              };
my $schema2 = {
                "fields" => [
                              {
                                "name" => "f1",
                                "type" => "string"
                              },
                              {
                                "name" => "f2",
                                "type" => "int"
                              }
                            ],
                "name" => "myrecord",
                "type" => "record"
              };

print 'get_subjects: ' . Dumper $schema_registry->get_subjects();
print 'get_schema_versions: ' . Dumper $schema_registry->get_schema_versions(SUBJECT => 'test-elasticsearch-sink', TYPE => 'value');
print 'get_schema_by_id: ' . Dumper $schema_registry->get_schema_by_id(SCHEMA_ID => 1);
print 'get_schema: ' . Dumper $schema_registry->get_schema(SUBJECT => 'test-elasticsearch-sink', TYPE => 'value', VERSION => 1);
print 'get_schema (latest): ' . Dumper $schema_registry->get_schema(SUBJECT => 'test-elasticsearch-sink', TYPE => 'value');
print 'check_schema: ' . Dumper $schema_registry->check_schema(SUBJECT => 'test-elasticsearch-sink', TYPE => 'value', SCHEMA => $schema1);
print 'check_schema: ' . Dumper $schema_registry->check_schema(SUBJECT => 'test-elasticsearch-sink', TYPE => 'value', SCHEMA => $schema2);
print 'test_schema: ' . ($schema_registry->test_schema(SUBJECT => 'test-elasticsearch-sink', TYPE => 'value', SCHEMA => $schema1) ? 'is compatible' : 'is NOT compatible'), "\n";
print 'test_schema: ' . ($schema_registry->test_schema(SUBJECT => 'test-elasticsearch-sink', TYPE => 'value', SCHEMA => $schema2) ? 'is compatible' : 'is NOT compatible'), "\n";

exit(1);




#
# Definizione schema di validazione AVRO
#
my $schema = Avro::Schema->parse(<<SCHEMA);
{
	"type": "record",
	"name": "myrecord",
	"fields": [
		{
			"name": "f1",
			"type": "string"
		}
	]
}
SCHEMA
my $schema_subject = 'test-elasticsearch-sink-value';
my $schema_version = 1;
my $schema_id = 1;


# common information
#print 'This is Kafka package ', $Kafka::VERSION, "\n";
#print 'You have a ', $BITS64 ? '64' : '32', ' bit system', "\n";

# kafka setup
my $kConnection = Kafka::Connection->new( host => 'localhost' ) or die "Kafka::Connection failure: $!";
my $kProducer = Kafka::Producer->new( Connection => $kConnection ) or die "Kafka::Producer failure: $!";

#my $count = 100;
##
## Creazione encoder AVRO
##
#my @encoded = ();
#for (my $i=0; $i<$count; $i++) {
#	push @encoded, '';
#	Avro::BinaryEncoder->encode(
#		schema	=> $schema,
#		data	=> { f1 => sprintf("%04d", $i) },
#		emit_cb	=> sub {
#			$encoded[$i] .= ${ $_[0] }
#		}
#	);
#}
##print 'ENCODED: ', Dumper \@encoded;
#
#my $res = $kProducer->send(
#	'test-elasticsearch-sink', # topic
#	0, # partition
#	\@encoded
#);

my $encodedRecord = pack('bN', &MAGIC_BYTE, $schema_id);
Avro::BinaryEncoder->encode(
	schema	=> $schema,
	data	=> { f1 => 'alvaro' },
	emit_cb	=> sub {
		$encodedRecord .= ${ $_[0] };
	}
);
print 'encodedRecord: ', Dumper $encodedRecord;
my $res = $kProducer->send(
	'test-elasticsearch-sink',	# topic
	0,							# partition
	$encodedRecord,				# record(s)
#	,							# keys(s)
#	time						# timestamp(s)	
);

print 'SEND RESPONSE: ', Dumper $res;

# my @decoded = ();
# for (my $i=0; $i<$count; $i++) {
# 	push @decoded, Avro::BinaryDecoder->decode(
# 		writer_schema	=> $schema,
# 		reader_schema	=> $schema,
# 		reader			=> IO::String->new($encoded[$i]),
# 	);
# }
# print 'DECODED: ', Dumper \@decoded;
# 

# cleaning up kafka
undef $kProducer;
$kConnection->close;
undef $kConnection;
