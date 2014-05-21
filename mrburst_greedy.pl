#!/usr/bin/perl -w
use strict;
use Sys::Hostname;
use JSON;
use Data::Dumper;
use WWW::Mechanize;

my $edge_master = "mianemaster"; # localhost
my $core_master = "mianmaster";
my $hdfs_jar_loc= "/user/root/remote/jar";

my $json_url = "http://$edge_master:50030/metrics?format=json";
my $browser = WWW::Mechanize->new();

# input: <binary.jar>, <class>, <local data dir>, <results data dir>
my $numArgs = $#ARGV + 1;
my $binary_jar = ();
my $class = ();
my $data_dir = ();
my $output_dir = ();
my $output_to_edge = 0;

# ./mrburst.pl puma.jar org.apache.hadoop.examples.InvertedIndex  /user/root/bookdb  /user/root/bookdb_output/InvertedIndex_ra 0 2>&1 | tee mrburst.log

if ($numArgs != 5) {
	print "please, provide <binary_jar>, <class_name>, <data_dir>, <output_dir>, <output to edge 0|1>  as parameters\n";
	exit(1);
} else {
  $binary_jar = $ARGV[0]; print "binary_jar: $binary_jar\n";
  $class = $ARGV[1]; print "class: $class\n";
  $data_dir = $ARGV[2]; print "input data dir: $data_dir\n";
  $output_dir = $ARGV[3]; print "output data dir: $output_dir\n";
  $output_to_edge = $ARGV[4]; print "write results on edge?: $output_to_edge\n";
}

if (edgeBusy()) {
#if (1) {
	print "edge busy, burst out to core\n";
  remoteAccess();
	#distcp();
} else {
	print "edge idle, submit in edge\n";
  my $cmd = "hadoop jar $binary_jar $class $data_dir $output_dir";
	print "executing command: $cmd\n";
	my $retcode = 0;
	$retcode = system($cmd) == 0 or die "system failed with ret code?: $?";
	print "retcode = $retcode\n";
  if(retcode == 0 ) {
	  print "results have been written into: $output_dir\n";
  }
}

sub edgeBusy {
# download the json page:
# ref: http://beerpla.net/2008/03/27/parsing-json-in-perl-by-example-southparkstudioscom-south-park-episodes/
	print "Getting Job Tracker $json_url\n";
	$browser->get( $json_url );
	my $content = $browser->content();
	my $json = $content; # from URL

# Development/Debugging
#	my $metrics_fn = "C:/Users/mian/Documents/tmp/metrics_kmeans";
#	open(my $fh, "<", $metrics_fn) or die "cannot open < $metrics_fn: $!";
# my $json = <$fh>; # from file
#	print "json object: " . $json . "\n";

# parsing json: http://search.cpan.org/~bkb/JSON-Parse-0.30/lib/JSON/Parse.pod
# http://www.tutorialspoint.com/json/json_perl_example.htm
	my $metrics = decode_json($json);
#print  Dumper($metrics);

#job tracker level metrics -- global
	my $jobtracker = $metrics -> { 'mapred' } -> { 'jobtracker' }[0][1];
	print "jobtracker...\n";
  print "*** job tracker level metrics -- global ***\n";

 	my $jobs_running = $jobtracker -> {'jobs_running'};
	print "jobs_running = $jobs_running\n";

 	my $map_slots = $jobtracker -> {'map_slots'};
	print "map_slots (i.e. slots not #maps or tasks) = $map_slots\n";

  my $waiting_maps = $jobtracker -> {'waiting_maps'};
	print "waiting_maps = $waiting_maps\n";

 	my $maps_launched = $jobtracker -> {'maps_launched'};
	print "maps_launched = $maps_launched\n";

 	my $running_maps = $jobtracker -> {'running_maps'};
	print "running_maps = $running_maps\n";

	my $occupied_map_slots  = $jobtracker -> {'occupied_map_slots'};
	print "occupied_map_slots = $occupied_map_slots\n";

  my $maps_completed = $jobtracker -> {'maps_completed'};
	print "maps_completed = $maps_completed\n";

	return $occupied_map_slots;
}

#example command:
#hadoop jar puma.jar org.apache.hadoop.examples.Classification hdfs://mianemaster/user/root/kmeans_30GB /user/root/kmeans_30GB_output

sub distcp {

# perl retcode: http://www.perlmonks.org/?node_id=486200
# http://perldoc.perl.org/functions/system.html

my $cmd =  "hadoop fs -rm -r -skipTrash $hdfs_jar_loc ";
print "executing command: $cmd\n";
system($cmd);# == 0 or die "system failed with ret code?: $?";

$cmd = "hadoop fs -mkdir -p $hdfs_jar_loc";
print "executing command: $cmd\n";
my $retcode = system($cmd) == 0 or die "system failed with ret code?: $?";
print "retcode = $retcode\n";
#my $results = `$cmd`;
#print $results;

$cmd = "hadoop fs -put $binary_jar $hdfs_jar_loc";
print "executing command: $cmd\n";
$retcode = system($cmd) == 0 or die "system failed with ret code?: $?";
print "retcode = $retcode\n";
#$results = `$cmd`;
#print $results;

#copy jar file
$cmd = "ssh $core_master 'hadoop distcp -overwrite hdfs://$edge_master:8020/$hdfs_jar_loc hdfs://$core_master:8020/$hdfs_jar_loc'";
print "executing command: $cmd\n";
$retcode = system($cmd) == 0 or die "system failed with ret code?: $?";
print "retcode = $retcode\n";
#$results = `$cmd`;
#print $results;

#copy data file  -- remove overwrite
$cmd = "ssh $core_master 'hadoop distcp -overwrite hdfs://$edge_master:8020/$data_dir hdfs://$core_master:8020/$data_dir'";
print "executing command: $cmd\n";
$retcode = system($cmd) == 0 or die "system failed with ret code?: $?";
print "retcode = $retcode\n";
#$results = `$cmd`;
#print $results;

$cmd = "ssh $core_master 'hadoop fs -get $hdfs_jar_loc/$binary_jar .'";
print "executing command: $cmd\n";
$retcode = system($cmd); # == 0 or die "system failed with ret code?: $?"; uncomment die statement
print "retcode = $retcode\n";
#$results = `$cmd`;
#print $results;

# submit hadoop job in the remote hadoop
$cmd = "ssh $core_master 'hadoop jar $binary_jar $class $data_dir $output_dir'";
print "executing command: $cmd\n";
$retcode = system($cmd) == 0 or die "system failed with ret code?: $?";
print "retcode = $retcode\n";
#$results = `$cmd`;
#print $results;

if ($output_to_edge) {
	# copy the results back to the edge
	$cmd = "hadoop distcp -overwrite hdfs://$core_master:8020/$output_dir hdfs://$edge_master:8020/$output_dir";
	print "executing command: $cmd\n";
  $retcode = system($cmd) == 0 or die "system failed with ret code?: $?";
  print "retcode = $retcode\n";
	#$results = `$cmd`;
  #print $results;
}
}

sub remoteAccess {
#hadoop jar puma.jar org.apache.hadoop.examples.Grep hdfs://mianemaster/user/root/wikipedia_50GB /user/root//user/root/wikipedia_output/Grep_ra Peace 2>&1 | tee log.log
	my $result_dir = ();
# submit hadoop job in the remote hadoop
	if ($output_to_edge) {
		# write the results back to the edge
    $result_dir = "hdfs://$edge_master/$output_dir";
 		my $cmd = "ssh $core_master 'hadoop jar $binary_jar $class hdfs://$edge_master/$data_dir $result_dir'";
		print "executing command: $cmd\n";
		my $retcode = system($cmd) == 0 or die "system failed with ret code?: $?";
		print "retcode = $retcode\n";
	} else {
    $result_dir = "$output_dir";
		my $cmd = "ssh $core_master 'hadoop jar $binary_jar $class hdfs://$edge_master/$data_dir $output_dir'";
		print "executing command: $cmd\n";
		my $retcode = system($cmd) == 0 or die "system failed with ret code?: $?";
		print "retcode = $retcode\n";
	}
  if(retcode == 0 ) {
	  print "results have been written into: $result_dir\n";
  }
}