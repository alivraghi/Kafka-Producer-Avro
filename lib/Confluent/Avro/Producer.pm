package Confluent::Avro::Producer;

=head1 SYNOPSIS

    use Kafka::Connection;
    use Confluent::Avro::Producer;

    my $connection = Kafka::Connection->new( host => 'localhost' );

    my $producer = Confluent::Avro::Producer->new( Connection => $connection );

    # TODO Interact with Avro & SchemaRegistry before sending messages

    # Sending a single message
    my $response = $producer->send(
        'mytopic',          # topic
        0,                  # partition
        { ... }    # message
    );

    # Sending a series of messages
    $response = $producer->send(
        'mytopic',          # topic
        0,                  # partition
        [                   # messages
            { ... },
            { ... },
            { ... }
        ]
    );

    # Closes the producer and cleans up
    undef $producer;
    $connection->close;
    undef $connection;

=head1 DESCRIPTION

C<Confluent::Avro::Producer> main feature is to provide object-oriented API for 
producing messages according to Confluent Schemaregistry and Avro serialization.

C<Confluent::Avro::Producer> inerhits from L<Kafka::Producer>.

=cut

use 5.010;
use strict;
use warnings;

use JSON;

use Data::Dumper;
$Data::Dumper::Purity = 1;
$Data::Dumper::Terse = 1;
$Data::Dumper::Useqq = 1;

use base 'Kafka::Producer';

use Avro::BinaryDecoder;
use Avro::BinaryEncoder;
use Avro::DataFile;
use Avro::DataFileReader;
use Avro::DataFileWriter;
#use Avro::Protocol;
use Avro::Schema;

use Kafka qw($BITS64);
use Kafka::Connection;

use constant MAGIC_BYTE => 0;

use Confluent::SchemaRegistry;


our $VERSION = '0.01';


=head2 CONSTRUCTOR

=head3 C<new>

Creates new producer client object.

C<new()> takes arguments in key-value pairs. The following arguments are currently recognized:

=over 3

=item C<Connection =E<gt> $connection>

C<$connection> is the L<Kafka::Connection|Kafka::Connection> object responsible for communication with
the Apache Kafka cluster.

=item C<ClientId =E<gt> $client_id>

This is a user supplied identifier (string) for the client application.

If C<ClientId> is not passed to constructor, its value will be automatically assigned
(to string C<'producer'>).

=item C<RequiredAcks =E<gt> $acks>

The C<$acks> should be an int16 signed integer.

Indicates how many acknowledgements the servers should receive before responding to the request.

If it is C<$NOT_SEND_ANY_RESPONSE> the server does not send any response.

If it is C<$WAIT_WRITTEN_TO_LOCAL_LOG>, (default)
the server will wait until the data is written to the local log before sending a response.

If it is C<$BLOCK_UNTIL_IS_COMMITTED>
the server will block until the message is committed by all in sync replicas before sending a response.

C<$NOT_SEND_ANY_RESPONSE>, C<$WAIT_WRITTEN_TO_LOCAL_LOG>, C<$BLOCK_UNTIL_IS_COMMITTED>
can be imported from the L<Kafka|Kafka> module.

=item C<Timeout =E<gt> $timeout>

This provides a maximum time the server can await the receipt
of the number of acknowledgements in C<RequiredAcks>.

The C<$timeout> in seconds, could be any integer or floating-point type not bigger than int32 positive integer.

Optional, default = C<$REQUEST_TIMEOUT>.

C<$REQUEST_TIMEOUT> is the default timeout that can be imported from the
L<Kafka|Kafka> module.

=back

=cut

sub new {
	#	Connection => $connection
	#	ClientId => $client_id
	#	RequiredAcks => $acks
	#	Timeout => $timeout
    my $this  = shift;
    my $class = ref($this) || $this;
	my $self = $class->SUPER::new(@_);
	return bless($self, $class);
}


=head2 METHODS

The following methods are defined for the C<Kafka::Producer> class:

=head3 C<send( $topic, $partition, $messages, $keys, $compression_codec )>

Sends a messages on a L<Kafka::Connection|Kafka::Connection> object.

Returns a non-blank value (a reference to a hash with server response description)
if the message is successfully sent.

C<send()> takes the following arguments:

=over 3

=item C<$topic>

The C<$topic> must be a normal non-false string of non-zero length.

=item C<$partition>

The C<$partition> must be a non-negative integer.

=item C<$messages>

The C<$messages> is an arbitrary amount of data (a simple data string or
a reference to an array of the data strings).

=item C<$keys>

The C<$keys> are optional message keys, for partitioning with each message,
so the consumer knows the partitioning key.
This argument should be either a single string (common key for all messages),
or an array of strings with length matching messages array.

=item C<$compression_codec>

Optional.

C<$compression_codec> sets the required type of C<$messages> compression,
if the compression is desirable.

Supported codecs:
C<$COMPRESSION_NONE>,
C<$COMPRESSION_GZIP>,
C<$COMPRESSION_SNAPPY>,
C<$COMPRESSION_LZ4>.
The defaults that can be imported from the L<Kafka|Kafka> module.

=item C<$timestamps>

Optional.

This is the timestamps of the C<$messages>.

This argument should be either a single number (common timestamp for all messages),
or an array of integers with length matching messages array.

Unit is milliseconds since beginning of the epoch (midnight Jan 1, 1970 (UTC)).

B<WARNING>: timestamps supported since Kafka 0.10.0.


Do not use C<$Kafka::SEND_MAX_ATTEMPTS> in C<Kafka::Producer-<gt>send> request to prevent duplicates.

=back

=cut

sub send {
	my $self = shift;
	my ($topic, $partition, $messages, $keys, $compression_codec) = @_;
	return $self->SUPER::send($topic, $partition, $messages, $keys, $compression_codec);
}

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


1;