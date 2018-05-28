#!/bin/env perl

use strict;
use warnings;
use Data::Dumper;
$Data::Dumper::Purity = 1;
$Data::Dumper::Terse = 1;
$Data::Dumper::Useqq = 1;

use Kafka::Connection;

use Confluent::SchemaRegistry;

use Test::More qw( no_plan );
#use Test::More tests => 32;

BEGIN { use_ok('Confluent::Avro::Producer', qq/Using/); }

my $class = 'Confluent::Avro::Producer';

my $kc = new_ok('Kafka::Connection' => [ 'host','localhost' ]);
isa_ok($kc, 'Kafka::Connection');
my $sr = new_ok('Confluent::SchemaRegistry' => []);
isa_ok($sr, 'Confluent::SchemaRegistry');

my $cap = new_ok($class => [ 'Connection',$kc , 'SchemaRegistry',$sr ], qq/Valid REST client config/);
isa_ok($cap, $class);

my $topic = 'confluent-avro-producer-test';
my $partition = 0;
my $messages = ['Message time ' . localtime];
my $keys = undef;
my $compression_codec = undef;
my $res = $cap->send($topic, $partition, $messages, $keys, $compression_codec);
ok(defined $res, 'Message(s) sent to topic \'' . $topic . '\'');
isa_ok($res, 'HASH', 'Message(s) sent');

$res = $cap->send(topic=>$topic, partition=>$partition, messages=>$messages, keys=>$keys, compressione_codec=>$compression_codec);
ok(defined $res, 'Message(s) sent to topic \'' . $topic . '\'');
isa_ok($res, 'HASH', 'Message(s) sent');

$kc->close();
