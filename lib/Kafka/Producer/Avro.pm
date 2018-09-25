package Kafka::Producer::Avro;

=pod

=head1 SYNOPSIS

    use Kafka::Connection;
    use Kafka::Producer::Avro;

    my $connection = Kafka::Connection->new( host => 'localhost' );

    my $producer = Kafka::Producer::Avro->new( Connection => $connection );

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

C<Kafka::Producer::Avro> main feature is to provide object-oriented API for 
producing messages according to Confluent Schemaregistry and Avro serialization.

C<Kafka::Producer::Avro> inerhits from and extends L<Kafka::Producer|Kafka::Producer>.

=cut

use 5.010;
use strict;
use warnings;

use JSON::XS;

use Data::Dumper;
#$Data::Dumper::Purity = 1;
#$Data::Dumper::Terse = 1;
#$Data::Dumper::Useqq = 1;
use Try::Tiny;

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



##### Class methods

# Encode $payload in Avro format according to an Avro schema 
sub _encode {
	my $schema_ref = shift;
	my $payload = shift;
	return undef
		unless defined $payload;
#	print Dumper($payload->{id});
#	print encode_json($payload), "\n";
	my $encoded = pack('bN', &MAGIC_BYTE, $schema_ref->{id});
	Avro::BinaryEncoder->encode(
		schema	=> $schema_ref->{schema},
		data	=> $payload,
		emit_cb	=> sub {
			#print STDERR Dumper(\@_);
			$encoded .= ${ $_[0] };
		}
	);
	return $encoded;
}


##### Private methods

sub _clear_error { $_[0]->_set_error() } 
sub _set_error   { $_[0]->{__ERROR} = $_[1] } 
sub _get_error   { $_[0]->{__ERROR} }

# Interact with Schema Registry
sub _get_avro_schema {
	my $self = shift;
	my $topic = shift;
	my $type = shift;
	my $supplied_schema = shift;
	
	my $subject = $topic . '-' . $type;
	my $sr = $self->schema_registry();
	my ($schema_id, $avro_schema);
	
	# FIXME need to use caching w/ TTL to avoid these checks for every call
	
	# If a schema is supplied...	
	if ($supplied_schema) {
		
		# If the subject exixts...
		my $subjects = $sr->get_subjects();
		if (grep(/^$subject$/, @$subjects) ) {
			
			# ...check if it already exists in registry
			my $schema_info = $sr->check_schema(
				SUBJECT => $topic,
				TYPE => $type,
				SCHEMA => $supplied_schema 
			);
			if ( defined $schema_info ) {
				
				$schema_id   = $schema_info->{id};
				$avro_schema = $schema_info->{schema};

			# ...if it does not already exist in the registry....
			} else {
			
				# ...test new schema compliancy against latest version 
				my $compliant = $sr->test_schema(
					SUBJECT => $topic,
					TYPE => $type,
					SCHEMA => $supplied_schema
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
				SUBJECT => $topic,
				TYPE => $type,
				SCHEMA => $supplied_schema
			);
			$self->_set_error('Error adding schema to registry: ' . encode_json($sr->get_error())) &&
				return undef
					unless $schema_id;
			
			# ...and bless new schema into an Avro schema object 
			$avro_schema = Avro::Schema->parse($supplied_schema);
			
		}
		
	} else {
		
		# retreive latest schema for the topic value
		my $schema_info = $sr->get_schema(
			SUBJECT => $topic,
			TYPE => $type
		);
		if ( defined $schema_info ) {
			
			$schema_id = $schema_info->{id};
			$avro_schema = $schema_info->{schema};
			
		} else {
			$self->_set_error("No schema in registry for subject " . $topic . '-' . 'value') &&
				return undef
					unless $schema_info;
		}
		 
	}
	
	return ($schema_id, $avro_schema);
}





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

C<Kafka::Producer::Avro->send()> method looks for named parameters:

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
	my $avro_schemas = {
		key => {
			id => undef,
			schema => undef
		},
		value => {
			id => undef,
			schema => undef
		}
	};
	my ($keys, $messages);

	$self->_clear_error();
	
	$self->_set_error('Missing topic param')
		and return undef
			unless defined $params{topic};
	$self->_set_error('Missing partition param')
		and return undef
			unless defined $params{partition};
	$self->_set_error('Missing messages')
		and return undef
			unless defined $params{messages};
		
	# Get Avro schema for keys and values
	foreach my $type (qw/key value/) {
		($avro_schemas->{$type}->{id}, $avro_schemas->{$type}->{schema}) = $self->_get_avro_schema($params{topic}, $type, $params{"${type}_schema"});
	}

	if ($params{keys}) {
		
		# Return if key Avro schema was not found
		$self->_set_error('No key Avro schema found')
			and return undef
				unless defined $avro_schemas->{key}->{id} && defined $avro_schemas->{key}->{schema};
				
		# Avro encoding of keys
		if (ref($params{keys}) eq 'ARRAY') {
			$keys = [
				map {
					_encode($avro_schemas->{key}, $_);
				} @{$params{keys}} 
			];
		} else {
			$keys = _encode($avro_schemas->{key}, $params{keys});
		}
		
	}

	# Return if value Avro schema was not found
	$self->_set_error('No value Avro schema found')
		and return undef
			unless defined $avro_schemas->{value}->{id} && defined $avro_schemas->{value}->{schema};
			
	# Avro encoding of messages
	if (ref($params{messages}) eq 'ARRAY') {
		$messages = [
			map {
				_encode($avro_schemas->{value}, $_);
			} @{$params{messages}} 
		];
	} else {
		$messages = _encode($avro_schemas->{value}, $params{messages});
	}
			
	# Return in case of mismatch between keys and messages
	if ($avro_schemas->{key}->{id}) {
		$self->_set_error('Keys/messages format mismatch')
			and return undef
				if ref($keys) eq 'ARRAY' && ref($messages) ne 'ARRAY';
		$self->_set_error('Keys/messages count mismatch')
			and return undef
				if ref($keys) eq 'ARRAY' && ref($messages) eq 'ARRAY' && $#$keys != $#$messages;
	}
	
	# Send messages through Kafka::Producer parent class
	return $self->SUPER::send(
		$params{topic}, 
		$params{partition},
		$messages,
		$keys,
		$params{compression_codec}
	);
	
}



sub bulk_send {
	my $self = shift;
	my %params = @_;
	
	$self->_clear_error();
	
	# If there is no an array of messages use send()
	return $self->send(%params)
		unless ref($params{messages}) eq 'ARRAY';
	
	$self->_set_error('Missing bulk size')
		and return undef
			unless defined $params{size};
	$self->_set_error('Bad bulk size')
		and return undef
			unless $params{size} =~ /^\d+$/;

	my $messages = $params{messages};
	my $keys = $params{keys};
	$self->_set_error('Keys/messages format mismatch')
		and return undef
			if ref($keys) eq 'ARRAY' && ref($messages) ne 'ARRAY';
	$self->_set_error('Keys/messages count mismatch')
		and return undef
			if ref($keys) eq 'ARRAY' && ref($messages) eq 'ARRAY' && $#$keys != $#$messages;
	
	my @messages = @$messages;		# duplicate messages array to preserve external array
	# Check for key(s) format (ARRAY vs. SCALAR)
	my (@keys, $key);
	if (ref($params{keys}) eq 'ARRAY') {
		@keys = @$keys;
	} else {
		$key = $keys;
	}
	my $message_count = scalar(@messages);
	my $bulk_size = $params{size};
	my $sent = 0;
	my $errors = 0;
	my $bulk_num = 0;
	
	if (ref($params{on_init}) eq 'CODE') {
		# SCALAR $to_send, SCALAR $bulk_size
		$params{on_init}->($message_count, $bulk_size);
	}
	for (my $i=1; $i<=$message_count; $i+=$bulk_size) {
		$bulk_num++;
		my @bulk = splice @messages, 0, $bulk_size;
		my $bulk_keys = $key;
		unless ($bulk_keys) {
			$bulk_keys = [ splice(@keys, 0, $bulk_size) ];
		}
		if (ref($params{on_before_send_bulk}) eq 'CODE') {
			# SCALAR $bulk_num, ARRAYREF $bulk_messages, ARRAYREF $bulk_keys, SCALAR index_from, SCALAR index_to
			$params{on_before_send_bulk}->($bulk_num, \@bulk, $bulk_keys, $i, ($i+$bulk_size<$message_count ? $i+$bulk_size-1 : $message_count));
		}
#		try {
			my $res = $self->send(
				'topic'					=> $params{topic}, 
				'partition'				=> 0, 
				'messages'				=> [ @bulk ], 
				'keys'					=> $bulk_keys,
				'compressione_codec'	=> undef,
				'key_schema'			=> $params{key_schema},
				'value_schema'			=> $params{value_schema}
			);
			if (defined $res) {
				$sent += scalar(@bulk);
				if (ref($params{on_after_send_bulk}) eq 'CODE') {
					# SCALAR $bulk_num, SCALAR $sent, SCALAR $total_sent
					$params{on_after_send_bulk}->(scalar(@bulk), $sent);
				}
			} else {
				$errors++;
				if (ref($params{on_send_error}) eq 'CODE') {
					# OBJECT $error, SCALAR $bulk_num, ARRAYREF $bulk_messages, ARRAYREF $bulk_keys, SCALAR index_from, SCALAR index_to
					$params{on_send_error}->($self->get_error(), $bulk_num, \@bulk, $bulk_keys, $i, ($i+$bulk_size<$message_count ? $i+$bulk_size-1 : $message_count));
				}
			}
#		} catch {
#			$self->_set_error('Unable to send message(s): ' . $_);
#			$errors++;
#			if (ref($params{on_send_error}) eq 'CODE') {
#				# OBJECT $error, SCALAR $bulk_num, ARRAYREF $bulk_messages, ARRAYREF $bulk_keys, SCALAR index_from, SCALAR index_to
#				$params{on_send_error}->($self->get_error(), $bulk_num, \@bulk, $bulk_keys, $i, ($i+$bulk_size<$message_count ? $i+$bulk_size-1 : $message_count));
#			}
#			return undef;
#		}
	}
	if (ref($params{on_complete}) eq 'CODE') {
		# SCALAR $total_sent, SCALAR $errors
		$params{on_complete}->($message_count, $sent, $errors);
	}
	return $sent;
	
}


1;

__END__
