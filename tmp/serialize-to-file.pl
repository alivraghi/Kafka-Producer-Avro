#!/bin/env perl

use strict;
use warnings;

use Data::Dumper;
$Data::Dumper::Purity = 1;
$Data::Dumper::Terse = 1;

use Avro::BinaryDecoder;
use Avro::BinaryEncoder;
use Avro::DataFile;
use Avro::DataFileReader;
use Avro::DataFileWriter;
#use Avro::Protocol;
use Avro::Schema;


my @names  = qw/Rizzardo Alvaro Roberto Adalberto Samuele LucaB. Massimo IvanL. FabioMan. LucaD. Rocco Davide IvanF. Pietro LucaA Francesco Lorenzo Riccardo MatteoC. Andrea ChristianDF. FabioMig. Carmen AlessandroZ. ChristianZ. Valentina Stefania Fabrizio MatteoM. Daniela Giuseppe AlessandroF. Maurizio/;
my @colors = qw/AliceBlue AntiqueWhite Aqua Aquamarine Azure Beige Bisque Black BlanchedAlmond Blue BlueViolet Brown BurlyWood CadetBlue Chartreuse Chocolate Coral CornflowerBlue Cornsilk Crimson Cyan DarkBlue DarkCyan DarkGoldenRod DarkGray DarkGreen DarkGrey DarkKhaki DarkMagenta DarkOliveGreen DarkOrange DarkOrchid DarkRed DarkSalmon DarkSeaGreen DarkSlateBlue DarkSlateGray DarkSlateGrey DarkTurquoise DarkViolet DeepPink DeepSkyBlue DimGray DimGrey DodgerBlue FireBrick FloralWhite ForestGreen Fuchsia Gainsboro GhostWhite Gold Goldenrod Gray Green GreenYellow Grey HoneyDew HotPink IndianRed Indigo Ivory Khaki Lavender LavenderBlush LawnGreen LemonChiffon LightBlue LightCoral LightCyan LightGoldenRodYellow LightGray LightGreen LightGrey LightPink LightSalmon LightSeaGreen LightSkyBlue LightSlateGray LightSlateGrey LightSteelBlue LightYellow Lime LimeGreen Linen Magenta Maroon MediumAquaMarine MediumBlue MediumOrchid MediumPurple MediumSeaGreen MediumSlateBlue MediumSpringGreen MediumTurquoise MediumVioletRed MidnightBlue MintCream MistyRose Moccasin NavajoWhite Navy OldLace Olive OliveDrab Orange OrangeRed Orchid PaleGoldenRod PaleGreen PaleTurquoise PaleVioletRed PapayaWhip PeachPuff Peru Pink Plum PowderBlue Purple Red RosyBrown RoyalBlue SaddleBrown Salmon SandyBrown SeaGreen SeaShell Sienna Silver SkyBlue SlateBlue SlateGray SlateGrey Snow SpringGreen SteelBlue Tan Teal Thistle Tomato Turquoise Violet Wheat White WhiteSmoke Yellow YellowGreen/;
$colors[$#colors] = undef;
my $names_count = scalar(@names);
my $colors_count = scalar(@colors);

#
# Definizione schema di validazione AVRO
#
my $schema = Avro::Schema->parse(<<SCHEMA);
{"namespace": "example.avro",
 "type": "record",
 "name": "User",
 "fields": [
     {"name": "name", "type": "string"},
     {"name": "favorite_number",  "type": ["int", "null"]},
     {"name": "favorite_color", "type": ["string", "null"]},
	 {"name": "properties", "type": {"type": "map", "values": "string"}}
 ]
}
SCHEMA

#
# Creazione file di output
#
my $fh;
my $out_file = 'output.avro';
open ($fh, '+>', $out_file) or die "Unable to write to $out_file: $!";

#
# Creazione oggetto writer AVRO per scrittura nel file di output nel formato AVRO
#
my $writer = Avro::DataFileWriter->new(
	fh				=> $fh,
	writer_schema	=> $schema,
	codec			=> 'deflate',
	metadata		=> {
		some	=> 'metadata'
	}
);

for (my $i=0; $i < 1000; $i++) {

	my $name  = $names[int(rand($names_count))];
	my $color = $colors[int(rand($colors_count))];
	#print sprintf("%03d: %s %s\n", $i, $name, $color);
	
	#
	# Scrittura record AVRO nel file di output
	#
	$writer->print( {
		"name" => $name,
		"favorite_number" => $i,
		"favorite_color" => $color,
		"properties" => {
			"A" => sprintf("A%04d", $i),
			"B" => sprintf("B%04d", $i)
		}
	});
}

# Persiste tutte le scritture rimaste (eventualmente) nel buffer
$writer->flush();

# Chiude il writer AVRO (NOTA: la chiusura del writer comporta la chiusura del filehandle!!)
#$writer->close();

seek($fh, 0, 0);

#
# Creazione oggetto reader AVRO per lettura dal file nel formato AVRO
#
my $reader = Avro::DataFileReader->new(
	fh				=> $fh,
	reader_schema	=> $schema
);

# Recupera i metadati
print 'metadata: ', Dumper $reader->metadata;

# Legge tutti record
my @all = $reader->all();
print 'records', Dumper \@all;

close $fh;


