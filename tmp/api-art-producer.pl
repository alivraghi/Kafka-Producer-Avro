#!/bin/env perl

use 5.010;
use strict;
use warnings;

use Data::Dumper;
	$Data::Dumper::Purity = 1;
	$Data::Dumper::Terse = 1;
	$Data::Dumper::Useqq = 1;
use JSON;
use Try::Tiny;
use DateTime;

use Kafka qw(
    $KAFKA_SERVER_PORT
    $REQUEST_TIMEOUT
    $RECEIVE_EARLIEST_OFFSET
    $DEFAULT_MAX_NUMBER_OF_OFFSETS
    $DEFAULT_MAX_BYTES
);
use Kafka::Connection;
use Confluent::SchemaRegistry;
use Confluent::Avro::Producer;
#use Avro::Schema;
use API::ART;
use API::ART::Collection::Activity;


$| = 1;

die "Usage: $0 instance_name artid artuser artpwd [schema_value_file]"
	if scalar(@ARGV) < 4;
my ($instance_name, $artid, $artuser, $artpwd, $value_schema_file) = @ARGV;
$value_schema_file = 'resource/api-art-activity.work.avsc' unless $value_schema_file;
my $topic_prefix = 'api-art-activity-' . $instance_name . '-';

# Read schema for message keys
my $key_schema = <<KEY_SCHEMA;
{
	"type": "string",
	"name": "_id"
}
KEY_SCHEMA

# Read schema for message values
my $value_schema = '';
open AVSC, $value_schema_file or die $!;
while (<AVSC>) {
	$value_schema .= $_;
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

# Connect to ART
my $art = API::ART->new(ARTID => $artid, USER => $artuser, PASSWORD => $artpwd);

# Create collection
my $ac = API::ART::Collection::Activity->new(ART => $art);


foreach my $at (sort keys %{$art->enum_activity_type()}) {
	next if grep /^$at$/, qw/API::TEST::01 API::TEST::02 API::TEST::03 TMP1 TMP2 KARTATT/;
	print 'Activity Type ', $at, ': ';
	# Find activities
	my $acts = [
		@{
			$ac->find_id(
				ACTIVITY_TYPE_NAME_IN => [ $at ] 
				#LIMIT	=> 10,
				#ID_IN	=> [ 5987, 5988 ]
				#ACTIVE	=> -1
			)
		} 
	];
	print "No match\n"
		and next
			unless scalar @$acts;
	print scalar @$acts, "\n";
	
	my $topic = lc($topic_prefix . $at);
	utf8::downgrade($topic); # Needs utf8::downgrade due to mysterious Kafka::Producer failure!
	my $partition = 0;
	
	my $res = $cap->bulk_send(
		'topic'					=> $topic, 
		'partition'				=> $partition, 
		'messages'				=> $acts, 
		'keys'					=> [
										map { 
											  $instance_name . $_
										} @$acts 
								   ],
		'compressione_codec'	=> undef,
		'key_schema'			=> $key_schema,
		'value_schema'			=> $value_schema,
		'size'					=> 10,
		'on_before_send_bulk'	=> sub {
			my ($bulk_num, $bulk_messages, $bulk_keys, $index_from, $index_to) = @_;
			for (my $i=0; $i<scalar(@$bulk_messages); $i++) {
				$bulk_messages->[$i] = API::ART::Activity->new(
					ART => $art,
					ID => $bulk_messages->[$i]
				)->dump();
			}
		},
		'on_after_send_bulk'	=> sub {
			my ($sent, $total_sent) = @_;
			print sprintf("\tProgress....: %3d%%\r", int($total_sent / scalar(@$acts) * 100)); 
		},
		'on_init'				=> sub {
			my ($to_send, $bulk_size) = @_;
			print "\tTOPIC.......: $topic\n";
			print "\tPARTITION...: $partition\n";
			print "\tInit........: sending $to_send message(s) using bulk of $bulk_size\n";
		},
		'on_complete'			=> sub {
			my ($to_send, $total_sent, $errors) = @_;
			print "\tProgress....: done" . ($errors ? ' with error(s)' : '') . "\n";
			print "\tSent........: $total_sent/" . $to_send . "\n";
		},
		'on_send_error'			=> sub {
			my ($error, $bulk_num, $bulk_messages, $bulk_keys, $index_from, $index_to) = @_;
			print "\tError on bulk \#", $bulk_num, ": ", Dumper($error); 
		}
	);
	print "Unable to send message(s): " . Dumper($cap->get_error())
		and exit(1)
			unless defined $res;
}
exit;

__END__



#my $xxx = try { 
#	Avro::Schema->parse($value_schema);
#} catch {
#	warn 'Avro::Schema->parse says: ' . $_ . "\n";
#};
#print STDERR 'Avro::Schema: ', Dumper($xxx);

#my $value_schema_id = $sr->add_schema(SUBJECT => $topic, TYPE => 'value', SCHEMA => $value_schema);
#die 'Bad schema: ' . Dumper($sr->get_error()) . "\n"
#	unless defined $value_schema_id;
#print 'Schema Id: ', $value_schema_id, "\n";

#my $avro_schema_value = $sr->get_schema_by_id(SCHEMA_ID => $value_schema_id);
#die "Unable to retreive schema $value_schema_id: " . Dumper($sr->get_error())
#	unless defined $avro_schema_value;
#print "Avro schema $value_schema_id: ", Dumper($avro_schema_value);



#my $isodate_to_epoch = sub {
#	my $isodate = shift;
#	print $isodate, ' ==> ';
#	$isodate =~ m/^(\d{4})\-(\d{2})\-(\d{2})T(\d\d):(\d\d):(\d\d)\.(\d{9})(.*)$/;
#	my $dt = DateTime->new(
#		 year       => $1
#		,month      => $2
#		,day        => $3
#		,hour       => $4
#		,minute     => $5
#		,second     => $6
#		,nanosecond => $7
#		,time_zone  => $8
#	);
#	print $dt->epoch(), ' ==> ';
#	my $dt1 = DateTime->from_epoch(epoch => $dt->epoch());
#	$dt1->set_time_zone('Europe/Rome');
#	print $dt1->strftime("%FT%T.%6N%z"), "\n";
#	return $dt->epoch();
#};

# Find activities
my $acts = [
#	map {
#		print $_->{id}, ': ';
#		$_->{info}->{creationDate} = $isodate_to_epoch->($_->{info}->{creationDate});
#		$_;
#	}
	map { 
		my $x = $_->dump( 
			#SYSTEM=>{ EXCLUDE_ALL=>1 } 
		); # dump activity as a Perl structure
		print STDERR to_json($x), "\n";
		$x;
	}
	@{
		$ac->find_object(
			ACTIVITY_TYPE_NAME_IN => [ $at ] 
			#LIMIT	=> 10,
			#ID_IN	=> [ 5987, 5988 ]
			#ACTIVE	=> -1
		)
	} 
];

die 'No matches found!'
	unless scalar @$acts;
print "Found: ", scalar @$acts, "\n";


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
				  $key_prefix				#  
				. ($key_prefix ? '-' : '')
				. $_->{id}					# finally use activity id
			} @messages 
		],
		compressione_codec	=> undef,
		key_schema			=> $key_schema,
		value_schema		=> $value_schema
	);
	print "Unable to send message(s): " . Dumper($cap->get_error())
		and exit(1)
			unless defined $res;
	$sent += scalar(@messages);
	print "done\n";
}

print "\nSent $sent message(s)\n";

