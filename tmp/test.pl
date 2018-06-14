#!/bin/env perl

use strict;
use warnings;
use Data::Dumper;
#$Data::Dumper::Purity = 1;
#$Data::Dumper::Terse = 1;
#$Data::Dumper::Useqq = 1;

use Try::Tiny;

use Confluent::SchemaRegistry;
use Confluent::Avro::Producer;
#use Avro::Schema;
use API::ART;
use API::ART::Collection::Activity;

die "Usage: $0 avro_schema_file topic artid artuser artpwd [key_prefix]"
	if scalar(@ARGV) < 5;
my ($schema_value_file, $topic, $artid, $artuser, $artpwd, $key_prefix) = @ARGV;
$key_prefix = $key_prefix || '';

# Read schema for message keys
my $schema_key = <<KEY_SCHEMA;
{
	"type": "string",
	"name": "_id"
}
KEY_SCHEMA

# Read schema for message values
my $schema_value = '';
open AVSC, $schema_value_file or die $!;
while (<AVSC>) {
	$schema_value .= $_;
}
close AVSC;

# Connect to Kafka
my $kc = Kafka::Connection->new('host' => 'localhost');

# Create Schema Registry object
my $sr = Confluent::SchemaRegistry->new();

# Create Kafka producer
my $cap = Confluent::Avro::Producer->new(
	'Connection' => $kc, 
	'SchemaRegistry'=>$sr
);


#my $xxx = try { 
#	Avro::Schema->parse($schema_value);
#} catch {
#	warn 'Avro::Schema->parse says: ' . $_ . "\n";
#};
#print STDERR 'Avro::Schema: ', Dumper($xxx);

#my $schema_value_id = $sr->add_schema(SUBJECT => $topic, TYPE => $type, SCHEMA => $schema_value);
#die 'Bad schema: ' . Dumper($sr->get_error()) . "\n"
#	unless defined $schema_value_id;
#print 'Schema Id: ', $schema_value_id, "\n";

#my $avro_schema_value = $sr->get_schema_by_id(SCHEMA_ID => $schema_value_id);
#die "Unable to retreive schema $schema_value_id: " . Dumper($sr->get_error())
#	unless defined $avro_schema_value;
#print "Avro schema $schema_value_id: ", Dumper($avro_schema_value);


# Connect to ART
my $art = API::ART->new(ARTID => $artid, USER => $artuser, PASSWORD => $artpwd);

# Create collection
my $ac = API::ART::Collection::Activity->new(ART => $art);

# Find activities
my $acts = [
	map { 
		$_->dump( SYSTEM=>{EXCLUDE_ALL=>1} ) # dump activity as a Perl structure 
	}
	@{
		$ac->find_object(
			#LIMIT	=> 10,
			#ID_IN	=> [ 5987, 5988 ]
			#ACTIVE	=> -1
		)
	} 
];

die 'No matches found!'
	unless scalar @$acts;
print "Found: ", scalar @$acts, "\n";


=pod

# Start Confluent environment
confluent start

# Start ElasticSearch
elasticsearch --daemonize --pidfile=/tmp/.elasticsearch.pid

	# zookeeper:2181
	# kafka:9092
	# schema-registry:8081
	# kafka-rest:8082
	# connect:8083
	# ksql:
	# control-center:
	# elasticsearch:9200+9300

# Load ES connector
confluent load api-art-activity-elasticsearch-sink -d ~/Confluent-Avro-Producer/resource/api-art-activity-elasticsearch-sink.properties

# Produce messages by API::ART
perl tmp/test.pl resource/api-art-activity.work.avsc

# Consume messages using console Avro utility
kafka-avro-console-consumer --bootstrap-server=localhost:9092 --from-beginning --topic api-art-activity

# Search in ES index
curl -X GET "localhost:9200/api-art-activity/_search?q=pippo&pretty"

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

# Delete Schema Registry subject 
curl -X DELETE http://localhost:8081/subjects/api-art-activity-value | json_pp

# Delete ES index
curl -X DELETE "localhost:9200/api-art-activity?pretty"

# Delete Kafka topic
kafka-topics --zookeeper localhost:2181 --delete --topic api-art-activity

# Unload connectort
confluent unload api-art-activity-elasticsearch-sink

# Stop Confluent environment
confluent stop

# Stop ES
kill $(cat /tmp/.elasticsearch.pid)

=cut

my $message_count = scalar(@$acts); 
my $bulk_size = 500;
my $sent = 0;
for (my $i=1; $i<=$message_count; $i+=$bulk_size) {
	print sprintf("Sending messages %d-%d... ", $i, ($i+$bulk_size<$message_count ? $i+$bulk_size-1 : $message_count));
	my @messages = splice @$acts, 0, $bulk_size;
	my $res = $cap->send(
		topic				=> $topic, 
		partition			=> 0, 
		messages			=> [
			@messages
		], 
		keys				=> [
			map { 
				  $key_prefix				# FIXME better using ES type: now it's defined in connector's "type.name" config 
				. ($key_prefix ? '-' : '')
				. 'A-'						# stands for Activity; for systems will be 'S'
				. $_->{info}->{type} . '-'	# concatenate with Activity Type name 
				. $_->{id}					# finally use activity id
			} @messages 
		],
		compressione_codec	=> undef,
		key_schema			=> $schema_key,
		value_schema		=> $schema_value
	);
	die "Unable to send messages: " . Dumper($sr->get_error())
		unless defined $res;
	$sent += scalar(@messages);
	print "done\n";
}

print "\nSent $sent message(s)\n";

