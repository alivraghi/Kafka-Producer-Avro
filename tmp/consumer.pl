#!/bin/env perl

use strict;
use warnings;

use Data::Dumper;
#$Data::Dumper::Purity = 1;
#$Data::Dumper::Terse = 1;
#$Data::Dumper::Useqq = 1;
use IO::String;
use Time::HiRes;

use Avro::BinaryDecoder;
#use Avro::BinaryEncoder;
#use Avro::DataFile;
#use Avro::DataFileReader;
#use Avro::DataFileWriter;
#use Avro::Protocol;
#use Avro::Schema;

use Kafka qw(
	$DEFAULT_MAX_BYTES
	$DEFAULT_MAX_NUMBER_OF_OFFSETS
	$RECEIVE_EARLIEST_OFFSET
);
use Kafka::Connection;
use Kafka::Consumer;

use Confluent::SchemaRegistry;


$| = 1;

die "Usage: $0 topic partition [wait_secs]\n"
	if scalar(@ARGV) < 2;
my ($topic, $partition, $wait_sec) = @ARGV;
$wait_sec = $wait_sec || 60;


# kafka setup
my ($kConnection, $kConsumer, $sr);
die "Kafka::Connection failure: $!\n"
	unless defined ($kConnection = Kafka::Connection->new(host => 'localhost'));
die "Kafka::Consumer failure: $!\n"
	unless defined ($kConsumer = Kafka::Consumer->new(Connection => $kConnection));
die "Confluent::SchemaRegistry failure: $!\n"
	unless defined ($sr = Confluent::SchemaRegistry->new());

# get all the available schemas for $topic's values 
my $avail_schemas = $sr->get_schema_versions(
	SUBJECT => $topic,
	TYPE => 'value'
) or die "No schemas available for topic '$topic'";

# create a array where the index is the global id of the schema and the value is the schema itself 
my $schemas = [];
foreach my $v (@$avail_schemas) {
	my $s = $sr->get_schema(
		SUBJECT => $topic,
		TYPE => 'value',
		VESRION => $v
	);
	$schemas->[$s->{id}] = $s->{schema} 
}

#1542187449040
my $offset_time = int(Time::HiRes::time*1000) - (60*60*1000);
print "Starting time: $offset_time\n";

my $offset = 0;

# Get a valid offset before the given time
$offset = $kConsumer->offset_before_time(
	$topic,               # topic
	$partition,           # partition
	$offset_time #(time()-3600) * 1000, # time
);
if ( defined $offset ) {
	print "Received offset $offset\n";
} else {
	$offset = 0;
	warn "Error: Offset is not received at given time\n";
}

# Get a valid offset at the given time
my $offset_struct = $kConsumer->offset_at_time(
	$topic,               # topic
	$partition,           # partition
	$offset_time #(time()-3600) * 1000, # time
);
if ( defined $offset_struct ) {
	print "Received offset" . Dumper($offset_struct) . "\n";
	$offset = $offset_struct->{offset}
		if ($offset_struct->{offset} && $offset_struct->{offset} != -1);
} else {
	warn "Error: Offset is not received at given time\n";
}

# Consuming messages
MESSAGES: {
	while (1) {
	
		my $messages = $kConsumer->fetch(
			$topic,				# topic
			$partition,			# partition
			$offset,			# offset
			$DEFAULT_MAX_BYTES	# Maximum size of MESSAGE(s) to receive
		);
		
		#last MESSAGES
		print "Waiting $wait_sec seconds ...\n" 
			and sleep $wait_sec
				and next
					unless $messages && scalar(@$messages);
					
		print "=============================================================================\n";
		print "Starting at offset $offset\n";
		print "Found " . scalar(@$messages) . " message(s)...\n"; 
				
		foreach my $message ( @$messages ) {
			my $reader = IO::String->new($message->payload);
			seek($reader, 1, 0); # Skip magic byte
			my $buf = "\0\0\0\0";
			read($reader, $buf, 4); # Read schema version stored in avro message header
			my $encoded_schema_version = unpack("N", $buf); # Convert schema version from unsigned long (32 byte) 
			my $decodePayload = Avro::BinaryDecoder->decode(
				writer_schema	=> $schemas->[$encoded_schema_version],
				reader_schema	=> $schemas->[$encoded_schema_version],
				reader			=> $reader
			);
			#print 'decodedPayload: ', Dumper($decodePayload), "\n";
			#print Dumper(keys %$message);
			print "-----------------------------------------------------------------------------\n";
			if ( $message->valid ) {
				my $hex_payload = $message->payload;
				#print 'payload..........: ', $message->payload, "\n";
				#$hex_payload =~ s/(.)/sprintf("%02X ",ord($1))/eg;
				#print 'payload..........: ', $hex_payload, "\n";
				#print 'key..............: ', $message->key, "\n";
				#print 'offset...........: ', $message->offset, "\n";
				#print 'next_offset......: ', $message->next_offset, "\n";
				
				#print qq/valid               : $message->{valid              }\n/;
				print qq/Using schema version id...: $encoded_schema_version\n/;
				print qq/key.......................: $message->{key                }\n/;
				print qq/Timestamp.................: $message->{Timestamp          }\n/;
				print qq/Attributes................: $message->{Attributes         }\n/;
				print qq/MagicByte.................: $message->{MagicByte          }\n/;
				print qq/offset....................: $message->{offset             }\n/;
				print qq/next_offset...............: $message->{next_offset        }\n/;
				print qq/HighwaterMarkOffset.......: $message->{HighwaterMarkOffset}\n/;
				
			} else {
				print qq/error.....................: $message->error\n/;
			}
#			my $commit = $kConsumer->commit_offsets(
#				$topic, 
#				$partition, 
#				$message->{offset}, 
#				'perl_consumer' 
#			);
			$offset = $message->{next_offset};
		}
		print "=============================================================================\n";
	}
}
		
# cleaning up kafka
undef $kConsumer;
$kConnection->close;
undef $kConnection;
