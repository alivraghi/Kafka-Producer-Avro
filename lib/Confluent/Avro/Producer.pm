package Confluent::Avro::Producer;

=pod

=head1 SYNOPSIS

    use Kafka::Connection;
    use Confluent::Avro::Producer;

    my $connection = Kafka::Connection->new( host => 'localhost' );

    my $producer = Confluent::Avro::Producer->new( Connection => $connection );

    # TODO Interact with Avro & SchemaRegistry before sending messages

    # Sending a single message
    my $response = $producer->send(...);

    # Sending a series of messages
    $response = $producer->send(...);

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

use Avro::BinaryEncoder;
use Avro::Schema;
use Confluent::SchemaRegistry;

use constant MAGIC_BYTE => 0; 

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
    my $schema_registry = delete $params{SchemaRegistry};
    
    # Use parent class constructor
	my $self = $class->SUPER::new(%params);
	
	# Add ans internal reference to SchemaRegistry
	$self->{__SCHEMA_REGISTRY} = $schema_registry;
	
	return bless($self, $class);
}

##### Private methods

sub _clear_error { $_[0]->_set_error() } 
sub _set_error   { $_[0]->{__ERROR} = $_[1] } 
sub _get_error   { $_[0]->{__ERROR} }



##### Public methods

=head2 METHODS

The following methods are defined for the C<Kafka::Avro::Producer> class:

=cut


sub schema_registry { $_[0]->{__SCHEMA_REGISTRY} }
sub get_error { $_[0]->_get_error() }



=head3 C<send( $topic, $partition, $messages, $keys, $compression_codec, $json_schema )>

Sends a messages on a L<Kafka::Connection|Kafka::Connection> object.

Returns a non-blank value (a reference to a hash with server response description)
if the message is successfully sent.

Despite L<Kafka::Producer|Kafka::Producer>C<->send()> method that expects positional arguments:

  $producer->send($topic, $partition, $messages, $keys, $compression_codec, $key_schema, $value_schema);

C<Confluent::Avro::Producer->send()> method looks for named parameters:

  $producer->send(
  	topic             => $topic, 
  	partition         => $partition, 
  	messages          => $messages, 
  	keys              => $keys, 
  	compression_codec => $compression_codec, 
  	key_schema        => $key_schema, 
  	value_schema      => $value_schema
  );    

Extra arguments:

=over 3

=item C<key_schema =E<gt> $key_schema> and C<value_schema =E<gt> $value_schema>

Both C<$key_schema> and C<$value_schema> named parametrs are optional and provide JSON strings that represent 
Avro schemas to validate and serialize key and value messages.

These schemas are validated against C<schema_registry> and, if compliant, they are added to the registry
under the C<$topic+'key'> or C<$topic+'value'> subjects.

If an expected schema isn't provided, latest version from Schema Registry is used according to the related 
subject (key or value). 

=back

=cut

sub send {
	my $self = shift;
	my %params = @_;
	my $schema_id;
	my $avro_schema;
	my $sr = $self->schema_registry();
	my $key_subject = $params{topic} . '-key';
	my $value_subject = $params{topic} . '-value';

	$self->_clear_error();
	
	# FIXME need to use caching w/ TTL to avoid these checks for every call
	
	# If a schema is supplied...	
	if ($params{value_schema}) {
		
		# If the subject exixts...
		my $subjects = $sr->get_subjects();
		if (grep(/^$value_subject$/, @$subjects) ) {
			
			# ...check if it already exists in registry
			my $schema_info = $sr->check_schema(
				SUBJECT => $params{topic},
				TYPE => 'value',
				SCHEMA => $params{value_schema} 
			);
			if ( defined $schema_info ) {
				
				$schema_id   = $schema_info->{id};
				$avro_schema = $schema_info->{schema};

			# ...if it does not already exist in the registry....
			} else {
			
				# ...test new schema compliancy against latest version 
				my $compliant = $sr->test_schema(
					SUBJECT => $params{topic},
					TYPE => 'value',
					SCHEMA => $params{value_schema}
				);
				$self->_set_error('Schema not compliant with latest one from registry') &&
					return undef
						unless $compliant;
			
			}
			
		}
		
		# ...if a previous id for the schema is not available, try to add the one supplied to the registry....
		unless ($schema_id) {
					  
			# ...procede adding it to the registry
			$schema_id = $sr->add_schema(
				SUBJECT => $params{topic},
				TYPE => 'value',
				SCHEMA => $params{value_schema}
			);
			$self->_set_error('Error adding schema to registry: ' . encode_json($sr->get_error())) &&
				return undef
					unless $schema_id;
			
			# ...and bless new schema into an Avro schema object 
			$avro_schema = Avro::Schema->parse($params{value_schema});
			
		}
		
	} else {
		
		# retreive latest schema for the topic value
		my $schema_info = $sr->get_schema(
			SUBJECT => $params{topic},
			TYPE => 'value'
		);
		if ( defined $schema_info ) {
			
			$schema_id = $schema_info->{id};
			$avro_schema = $schema_info->{schema};
			
		} else {
			$self->_set_error("No schema in registry for subject " . $params{topic} . '-' . 'value') &&
				return undef
					unless $schema_info;
		}
		 
	}

	# Avro encoding of messages
	my $messages = [];
	foreach my $message (@{$params{messages}}) {
		my $enc = pack('bN', &MAGIC_BYTE, $schema_id);
		Avro::BinaryEncoder->encode(
			schema	=> $avro_schema,
			data	=> $message,
			emit_cb	=> sub {
				$enc .= ${ $_[0] };
			}
		);
		push @$messages, $enc;
	}
	
	# Send messages through Kafka::Producer parent class
	return $self->SUPER::send(
		$params{topic},
		$params{partition},
		$messages,
		$params{keys},
		$params{compression_codec}
	);
	
}

1;

__END__

