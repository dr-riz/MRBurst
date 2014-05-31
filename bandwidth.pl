#!/usr/bin/perl -w
use strict;


my $iter = 50; # number of iterations.



for (my $i=1; $i <= $iter; $i++) {
  print "iter: $i\n";
  my $cmd = "iperf -c mianmaster -r -m";
  my $output = `$cmd`;
  print "$output";
  $cmd = "echo \"$output\" >> bw_measurements";
  $output = `$cmd`;
}