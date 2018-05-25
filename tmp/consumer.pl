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

use Kafka qw(
	$DEFAULT_MAX_BYTES
	$DEFAULT_MAX_NUMBER_OF_OFFSETS
	$RECEIVE_EARLIEST_OFFSET
);
use Kafka::Connection;
use Kafka::Consumer;

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

# common information
print 'This is Kafka package ', $Kafka::VERSION, "\n";

# kafka setup
my $kConnection = Kafka::Connection->new( host => 'localhost' ) or die "Kafka::Connection failure: $!";
my $kConsumer = Kafka::Consumer->new( Connection => $kConnection ) or die "Kafka::Consumer failure: $!";

# Get a valid offset before the given time
my $offsets = $kConsumer->offset_before_time(
	'test-elasticsearch-sink',                      # topic
	0,                              # partition
	(time()-3600) * 1000,           # time
);

if ( $offsets && @$offsets ) {
	print "Received offset: $_\n" foreach @$offsets;
} else {
	warn "Error: Offsets are not received\n";
}

# Consuming messages
my $messages = $kConsumer->fetch(
	'test-elasticsearch-sink',	# topic
	0,							# partition
	0,							# offset
	$DEFAULT_MAX_BYTES			# Maximum size of MESSAGE(s) to receive
);

if ( $messages && scalar(@$messages) ) {
	foreach my $message ( @$messages ) {
		my $decodePayload = Avro::BinaryDecoder->decode(
			writer_schema	=> $schema,
			reader_schema	=> $schema,
			reader			=> IO::String->new($message->payload),
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
