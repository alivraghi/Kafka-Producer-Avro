#!/bin/env perl

use strict;
use warnings;

use Data::Dumper;
#$Data::Dumper::Purity = 1;
#$Data::Dumper::Terse = 1;
#$Data::Dumper::Useqq = 1;
use IO::String;

use Avro::BinaryDecoder;
use Avro::BinaryEncoder;
use Avro::DataFile;
use Avro::DataFileReader;
use Avro::DataFileWriter;
#use Avro::Protocol;
use Avro::Schema;

use Kafka qw(
	$DEFAULT_MAX_BYTES
	$DEFAULT_MAX_NUMBER_OF_OFFSETS
	$RECEIVE_EARLIEST_OFFSET
);
use Kafka::Connection;
use Kafka::Consumer;

use Confluent::SchemaRegistry;


# common information
print 'This is Kafka package ', $Kafka::VERSION, "\n";

my $topic = 'api-art-activity-wpsocore-ATTIVITA';

# kafka setup
my $kConnection = Kafka::Connection->new( host => 'localhost' ) or die "Kafka::Connection failure: $!";
my $kConsumer = Kafka::Consumer->new( Connection => $kConnection ) or die "Kafka::Consumer failure: $!";
my $sr = Confluent::SchemaRegistry->new() || die "Confluent::SchemaRegistry failure: $!";

my $avail_schemas = $sr->get_schema_versions(
	SUBJECT => $topic,
	TYPE => 'value'
) or die "No schemas available for topic '$topic'";

my $schemas = [];
foreach my $v (@$avail_schemas) {
	$schemas->[$v] = $sr->get_schema(
		SUBJECT => $topic,
		TYPE => 'value',
		VESRION => $v
	);
}
my $latest_schema = $sr->get_schema(
	SUBJECT => $topic,
	TYPE => 'value'
) or die "No schemas available for topic '$topic'";
#print Dumper($schema->{schema}), "\n";
my $schema = $latest_schema->{schema};

# Get a valid offset before the given time
my $offsets = $kConsumer->offset_before_time(
	$topic,               # topic
	0,                    # partition
	(time()-3600) * 1000, # time
);

if ( $offsets && @$offsets ) {
	print "Received offset: $_\n" foreach @$offsets;
} else {
	warn "Error: Offsets are not received\n";
}

# Consuming messages
my $messages = $kConsumer->fetch(
	$topic,				# topic
	0,					# partition
	0,					# offset
	$DEFAULT_MAX_BYTES	# Maximum size of MESSAGE(s) to receive
);

if ( $messages && scalar(@$messages) ) {
	foreach my $message ( @$messages ) {
		my $reader = IO::String->new($message->payload);
		$reader->setpos(5); # Skip the message header first 5 bytes
		my $decodePayload = Avro::BinaryDecoder->decode(
			writer_schema	=> $schema,
			reader_schema	=> $schema,
			reader			=> $reader,
		);
		print 'decodedPayload: ', Dumper($decodePayload), "\n";
		if ( $message->valid ) {
			my $hex_payload = $message->payload;
			$hex_payload =~ s/(.)/sprintf("%02X ",ord($1))/eg;
			#print 'payload    : ', $message->payload, "\n";
			print 'payload    : ', $hex_payload, "\n";
			print 'key        : ', $message->key, "\n";
			print 'offset     : ', $message->offset, "\n";
			print 'next_offset: ', $message->next_offset, "\n";
		} else {
			print 'error      : ', $message->error, "\n";
		}
	}
} else {
	warn "No message(s) found.\n";
}
		
# cleaning up kafka
undef $kConsumer;
$kConnection->close;
undef $kConnection;
