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
use Search::Elasticsearch;

use API::ART;
use API::ART::Collection::Activity;


$| = 1;

die qq{
Usage:
 
	$0 elastic_connection_url art_instance_id artid artuser artpwd q [activity_type]

Example:

	$0 http://localhost:9200 soadvlog SIRTI_SOADVLOG root pippo123 "searchtext"
	$0 http://localhost:9200 soadvlog SIRTI_SOADVLOG root pippo123 "searchtext" DICHIARAZIONE_KIT

} if scalar(@ARGV) < 6;
my ($elastic_connection_url, $art_instance_id, $artid, $artuser, $artpwd, $q, $activity_type) = @ARGV;
$activity_type = '' unless defined $activity_type;
my $index = undef;
$index = 'api-art-activity-' . lc($art_instance_id) . '-' . lc($activity_type) if $activity_type;

my $es = Search::Elasticsearch->new(
	nodes => [
		$elastic_connection_url 
	]
);

# // https://www.elastic.co/guide/en/elasticsearch/reference/6.3/query-dsl-bool-query.html
# GET /api-art-activity-wpretelit-tt_client/_search
# {
#   "query": {
#     "bool": {
#       "must": [
#         {
#           "multi_match": {
#             "query": "7679"
#           }
#         }
#         {
#           "match": {
#               "info.description": {
#               "query": "esagerata anomalia",
#               "operator": "and"
#             }
#           }
#         }
#       ],
#       "filter": {
#         "terms": {
#           "system.info.groups.keyword": ["PIPPO","C_GSD_USER"]
#         }
#       }
#     }
#   },
#   "_source": ["id", "info.description", "system.info.groups"]
# }


my $results = $es->search(
    index => $index,
    body  => {
		"query" => {
			"bool" => {
				"must" => [
#					{
#						"match" => {
#							"info.description" => {
#								"query"    => $q,
#								"operator" => "and"
#							}
#						}
#					}
					{
						"multi_match" => {
							"query" => $q
						}
					}
				],
				"filter" => {
					"terms" => {
						"system.info.groups.keyword" =>
						  [ "ROOT" ]
					}
				}
			}
		},
		"_source" => [ "id", "info.description", "system.info.groups" ]
	}
);

print Dumper $results;

exit;

__END__


# Connect to ART
my $art = API::ART->new(ARTID => $artid, USER => $artuser, PASSWORD => $artpwd);

# Create collection
my $ac = API::ART::Collection::Activity->new(ART => $art);

my $loader_file = "${dest_path}/load-${art_instance_id}.sh";
open LOADER, ">${loader_file}" or die "Error creating service loader ${loader_file}\n$!\n";
print LOADER '#!/usr/bin/env bash', "\n\n";

my $unloader_file = "${dest_path}/unload-${art_instance_id}.sh";
open UNLOADER, ">${unloader_file}" or die "Error creating service unloader ${unloader_file}\n$!\n";
print UNLOADER '#!/usr/bin/env bash', "\n\n";

foreach my $at (sort keys %{$art->enum_activity_type()}) {
	next if grep /^$at$/, qw/API::TEST::01 API::TEST::02 API::TEST::03 TMP1 TMP2 KARTATT KART_HISTORY/;
	print 'Activity Type ', $at, ': ';
	
	my $topic = lc($topic_prefix . $at);
	utf8::downgrade($topic); # Needs utf8::downgrade due to mysterious Kafka::Producer failure!
	my $partition = 0;
	
	my $connector_file = "${dest_path}/${topic}-sink.properties";
	open CONNECTOR, ">${dest_path}/${topic}-sink.properties" or die $!;
	print CONNECTOR <<"***EOT***";
connector.class=io.confluent.connect.elasticsearch.ElasticsearchSinkConnector
tasks.max=1
key.ignore=false
schema.ignore=true
connection.url=$elastic_connection_url
type.name=activity

name=${topic}-sink
topics=${topic}
***EOT***
;
	close CONNECTOR;
	print "done", "\n";
	print LOADER "confluent load ${topic}-sink -d ${connector_file}; sleep 2;\n";
	print UNLOADER "echo -n 'Unloading ${topic}-sink ...'; confluent unload ${topic}-sink; echo 'done';\n";
	next;
}

close LOADER;
close UNLOADER;
chmod 0755, $loader_file, $unloader_file;

exit;
