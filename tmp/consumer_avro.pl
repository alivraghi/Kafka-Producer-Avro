#!/bin/env perl

use strict;
use warnings;

use Data::Dumper;
#$Data::Dumper::Purity = 1;
#$Data::Dumper::Terse = 1;
#$Data::Dumper::Useqq = 1;
use Time::HiRes;

use Kafka qw(
	$DEFAULT_MAX_BYTES
	$DEFAULT_MAX_NUMBER_OF_OFFSETS
	$RECEIVE_EARLIEST_OFFSET
);
use Kafka::Connection;
use Kafka::Consumer::Avro;
use Kafka::Producer::Avro;
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
die "Confluent::SchemaRegistry failure: $!\n"
	unless defined ($sr = Confluent::SchemaRegistry->new());
die "Kafka::Consumer failure: $!\n"
	unless defined ($kConsumer = Kafka::Consumer::Avro->new(Connection => $kConnection, SchemaRegistry => $sr));

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
			$offset = $message->{next_offset};
		}
		print "=============================================================================\n";
	}
}
		
# cleaning up kafka
undef $kConsumer;
$kConnection->close;
undef $kConnection;
