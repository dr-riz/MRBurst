#!/usr/bin/perl -w
use strict;
use Sys::Hostname;

print "MRBurst...\n";

my $edge_master = hostname; # localhost
my $core_master = "mianmaster";
my $hdfs_jar_loc= "/user/root/remote/jar";

# input: <binary.jar>, <class>, <local data dir>, <results data dir>
my $numArgs = $#ARGV + 1;
my $binary_jar = ();
my $class = ();
my $data_dir = ();
my $output_dir = ();
my $output_to_edge = 0;

if ($numArgs != 5) {
	print "please, provide <binary_jar>, <class_name>, <data_dir>, <output_dir>, <output to edge 0|1>  as parameters\n";
	exit(1);
} else {
  $binary_jar = $ARGV[0]; print "binary_jar = $binary_jar\n";
  $class = $ARGV[1]; print "class = $class\n";
  $data_dir = $ARGV[2]; print "input data dir = $data_dir\n";
  $output_dir = $ARGV[3]; print "output data dir = $output_dir\n";
  $output_to_edge = $ARGV[4]; print "write results on ? = $output_to_edge\n";
}

#example command:
#hadoop jar puma.jar org.apache.hadoop.examples.Classification hdfs://mianemaster/user/root/kmeans_30GB /user/root/kmeans_30GB_output

my $cmd =  "hadoop fs -rm -r -skipTrash $hdfs_jar_loc";
print "executing command: $cmd\n";
system($cmd) == 0 or die "system $cmd failed: $?";

$cmd = "hadoop fs -mkdir -p $hdfs_jar_loc";
print "executing command: $cmd\n";
system($cmd) == 0 or die "system $cmd failed: $?";

$cmd = "hadoop fs -put $binary_jar $hdfs_jar_loc";
print "executing command: $cmd\n";
system($cmd) == 0 or die "system $cmd failed: $?";

#copy jar file
$cmd = "ssh $core_master 'hadoop distcp -overwrite hdfs://$edge_master:8020/$hdfs_jar_loc hdfs://$core_master:8020/$hdfs_jar_loc'";
print "executing command: $cmd\n";
system($cmd) == 0 or die "system $cmd failed: $?";

$cmd = "ssh $core_master 'rm $binary_jar'";
print "executing command: $cmd\n";
system($cmd) == 0 or die "system $cmd failed: $?";

$cmd = "ssh $core_master 'hadoop fs -get $hdfs_jar_loc/$binary_jar .'";
print "executing command: $cmd\n";
system($cmd) == 0 or die "system $cmd failed: $?";

#copy data file
$cmd = "ssh $core_master 'hadoop distcp -overwrite hdfs://$edge_master:8020/$data_dir hdfs://$core_master:8020/$data_dir'";
print "executing command: $cmd\n";
system($cmd) == 0 or die "system $cmd failed: $?";

# submit hadoop job in the remote hadoop
$cmd = "time ssh $core_master 'hadoop jar $binary_jar $class $data_dir $output_dir'";
print "executing command: $cmd\n";
system($cmd) == 0 or die "system $cmd failed: $?";

if ($output_to_edge) {
	# copy the results back to the edge
	$cmd = "hadoop distcp -overwrite hdfs://$core_master:8020/$data_dir hdfs://$edge_master:8020/$data_dir";
	print "executing command: $cmd\n";
  system($cmd) == 0 or die "system $cmd failed: $?";
}