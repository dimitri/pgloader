#!/usr/bin/env perl
# inject-ms.pl <summary.json> <milliseconds>
# Adds "os-wall-ms": N to the top-level JSON object in-place.
use strict;
use warnings;
use JSON::PP;

my ($file, $ms) = @ARGV;
open(my $fh, '<', $file) or die "cannot read $file: $!";
local $/;
my $data = JSON::PP->new->decode(<$fh>);
close($fh);

$data->{'os-wall-ms'} = $ms + 0;

open(my $out, '>', $file) or die "cannot write $file: $!";
print $out JSON::PP->new->encode($data);
close($out);
