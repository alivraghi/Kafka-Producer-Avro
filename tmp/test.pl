#!/bin/env perl

use strict;
use warnings;
use Data::Dumper;
$Data::Dumper::Purity = 1;
$Data::Dumper::Terse = 1;
$Data::Dumper::Useqq = 1;

use Try::Tiny;

use Confluent::SchemaRegistry;
#use Confluent::Avro::Producer;
use Avro::Schema;

die "Usage: $0 avro_schema [subject [key|value]]"
	unless defined $ARGV[0];

my $sr = Confluent::SchemaRegistry->new();

my $schema = '';
open AVSC, $ARGV[0] or die $!;
while (<AVSC>) {
	$schema .= $_;
}
close AVSC;

my $avro_schema = try { 
	Avro::Schema->parse($schema);
} catch {
	warn 'Avro::Schema->parse says: ' . $_ . "\n";
};

my $res = $sr->add_schema(SUBJECT => ($ARGV[1] || 'api-art-activity'), TYPE => ($ARGV[2] || 'value'), SCHEMA => $schema);
die 'Bad schema: ' . Dumper($sr->get_error()) . "\n"
	unless defined $res;
print 'Schema Id: ', $res, "\n";

print q/Everything's ok!/, "\n";
