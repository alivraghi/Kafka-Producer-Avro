package Confluent::Avro::Producer;

=pod

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

C<Confluent::Avro::Producer> inerhits from and extends L<Kafka::Producer|Kafka::Producer>.

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

#use Kafka qw($BITS64);
#use Kafka::Connection;

use constant MAGIC_BYTE => 0;

use Confluent::SchemaRegistry;


our $VERSION = '0.01';


=head2 CONSTRUCTOR

=head3 C<new>

Creates new producer client object.

C<new()> takes arguments in key-value pairs as described in L<Kafka::Producer|Kafka::Producer> from which it inherits.

In addition, takes in the following arguments:

=over 3

=item C<SchemaRegistry =E<gt> $schema_registry> (B<mandatory>)

C<$schema_registry> is the L<Confluent::SchemaRegistry|Confluent::SchemaRegistry> object responsible for communication with
the Confluent Schema Registry that controls the schema version to use to validate and serialize messages.

=back

=cut

sub new {
    my $this  = shift;
    my $class = ref($this) || $this;
    my $schema_registry_class = 'Confluent::SchemaRegistry';
    my %params = @_;

	# Check SchemaRegistry param
    die "Missing SchemaRegistry param"
    	unless exists $params{SchemaRegistry};
    die "SchemaRegistry param must be a $schema_registry_class instance object"
    	unless ref($params{SchemaRegistry}) eq $schema_registry_class;
    $schema_registry = delete $params{SchemaRegistry};
    
    # Use parent class constructor
	my $self = $class->SUPER::new(%params);
	
	# Add ans internal reference to SchemaRegistry
	$self->{__SCHEMA_REGISTRY} = $schema_registry;
	
	return bless($self, $class);
}

##### Private methods

sub _schema_registry { $_[]->{__SCHEMA_REGISTRY} }



##### Public methods

=head2 METHODS

The following methods are defined for the C<Kafka::Avro::Producer> class:

=cut


=head3 C<send( $topic, $partition, $messages, $keys, $compression_codec, $json_schema )>

Sends a messages on a L<Kafka::Connection|Kafka::Connection> object.

Returns a non-blank value (a reference to a hash with server response description)
if the message is successfully sent.

In addition to L<Kafka::Producer|Kafka::Producer> arguments, C<send()> takes extra arguments
to validate key/value schemas against Schema Registry service.

=over 3

=item C<$key_schema>, C<$value_schema>

Both C<$key_schema> and C<$value_schema> are optional JSON strings who represent the schemas used 
to validate and serialize key/value messages.

These schemas are validated against C<$schema_registry> and, if compliant, they are added to the registry
under the C<$topic+'key'> or C<$topic+'value'> subjects.

If an expected schema isn't provided, latest version from Schema Registry is used according to the related 
subject (key or value). 

=back

You can also specify arguments in key-value flavour; the following two calls are identical: 

  $producer->send($topic, $partition, $messages, $keys, $compression_codec, $key_schema, $value_schema);
  
  $producer->send(
  	topic => $topic, 
  	partition => $partition, 
  	messages => $messages, 
  	keys => $keys, 
  	compression_codec => $compression_codec, 
  	key_schema => $key_schema, 
  	value_schema => $value_schema
  );    

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