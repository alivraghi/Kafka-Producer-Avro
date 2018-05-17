#!/bin/env perl

use strict;
use warnings;
use Data::Dumper;
$Data::Dumper::Purity = 1;
$Data::Dumper::Terse = 1;
$Data::Dumper::Useqq = 1;

use Kafka::Connection;

use Test::More qw( no_plan );
#use Test::More tests => 32;

BEGIN { use_ok('Confluent::Avro::Producer', qq/Using/); }

my $class = 'Confluent::Avro::Producer';

my $kc = new_ok('Kafka::Connection' => [ 'host','localhost' ]);

my $cap = new_ok($class => [ 'Connection',$kc ], qq/Valid REST client config/);
isa_ok($cap, $class);

my $topic = 'confluent-avro-producer-test';
my $partition = 0;
my $messages = ['Message time ' . localtime];
my $keys = undef;
my $compression_codec = undef;
my $res = $cap->send($topic, $partition, $messages, $keys, $compression_codec);
ok(defined $res, 'Message(s) sent to topic \'' . $topic . '\'');
isa_ok($res, 'HASH', 'Message(s) sent');

$kc->close();
