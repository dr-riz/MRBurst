#!/usr/bin/perl -w
use strict;
use warnings;

print "filtering out non-dictionary words...\n";
my @words;
open (my $inFile, '<', './gutenberg') or die $!;

my %dict = ();
open my $dict_file, '<', '/usr/share/dict/words' or die $!;

my $out_file = "./gutenberg-dict";
open(my $fh, '>', "$out_file") or die "Could not open file $out_file $!";

my $non_words = "./non_dict_words";
open(my $nw_fh, '>', "$non_words") or die "Could not open file $non_words $!";

my (%hash, $current_key);

while (my $word = <$dict_file>) {
	chomp($word);
  $current_key = lc($word);
 	$dict { "$current_key" } = 1;
}

my $dict_words = 0;
my $total_words = 0;
my $total_lines = 0;
while (my $line = <$inFile>) {
	chomp($line);
  @words =  split /[\[\]\"\-_\?\*\^\$\+\(\)\{\|:;!,\.\s\/]+/, $line;
  foreach my $word (@words) {
  	$total_words++;
  	$word = lc($word);
    if (exists $dict{$word}) {
    	$dict_words++;
  	  print $fh "$word ";
    } else {
    	print $nw_fh "$word ";
    }
  }
  $total_lines++;
  if(($total_lines % 1000000) == 0) {print "another million lines processed ($total_lines)\n"; }

  print $fh "\n";
  print $nw_fh "\n";
}

close $inFile;
close $out_file;
print "total_words (#) = $total_words\n";
print "dict_words (#) = $dict_words\n";
print "non_dict_words (#) = " . ($total_words - $dict_words) . "\n";
print "dict word %age in input file=" . ($dict_words/$total_words*100) . "%\n";
print "done\n";