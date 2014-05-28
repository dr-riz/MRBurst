#!/usr/bin/perl -w
use strict;
use Sys::Hostname;
use JSON;
use Data::Dumper;
use WWW::Mechanize;

my $edge_master = hostname; # localhost
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
	print "edge busy, burst out to core\n";
	distcp();
} else {
	print "edge idle, submit in edge\n";
  my $cmd = "hadoop jar $binary_jar $class $data_dir $output_dir";
	print "executing command: $cmd\n";
	my $retcode = system($cmd) == 0 or die "system failed with ret code?: $?";
	print "retcode = $retcode\n";
}

sub edgeBusy {
# download the json page:
# ref: http://beerpla.net/2008/03/27/parsing-json-in-perl-by-example-southparkstudioscom-south-park-episodes/
	print "Getting Job Tracker $json_url\n";
	$browser->get( $json_url );
	my $content = $browser->content();
	my $json = $content; # from URL

#	my $metrics_fn = "C:/Users/mian/Documents/tmp/metrics_kmeans";
#	open(my $fh, "<", $metrics_fn) or die "cannot open < $metrics_fn: $!";
	#my $json = <$fh>; # from file

	print "json object: " . $json . "\n";

# parsing json: http://search.cpan.org/~bkb/JSON-Parse-0.30/lib/JSON/Parse.pod
# http://www.tutorialspoint.com/json/json_perl_example.htm
	my $metrics = decode_json($json);
#print  Dumper($metrics);

#$uid =       $rHoH->{ $login }->{ 'uid' };   # method 1  most readable

#job tracker level metrics -- global
	my $jobtracker = $metrics -> { 'mapred' } -> { 'jobtracker' }[0][1];
	print "jobtracker...\n";
  print "*** job tracker level metrics -- global ***\n";
	#print  Dumper($jobtracker);
	my $map_slots = $jobtracker -> {'map_slots'};
	print "map_slots = $map_slots\n";

	my $map_jobs_running = $jobtracker -> {'jobs_running'};
	print "map_jobs_running = $map_jobs_running\n";

#job level metrics -- local
  print "*** job level metrics -- local ***\n";
 	my @jobs = $metrics -> { 'fairscheduler' } -> { 'jobs' };

	my $num_of_jobs = 0;
	if ((@jobs)) {
		$num_of_jobs = @jobs;
	}
	print "number of MR jobs = $num_of_jobs\n";

	if ($num_of_jobs > 0 ){
		my $job = $metrics -> { 'fairscheduler' } -> { 'jobs' }[1][1]; #map
  	my $runningMaps = $job -> {'runningTasks'};
		print "runningMaps = $runningMaps\n";

  	my $mapDemand = $job -> {'demand'};
		print "mapDemand = $mapDemand\n";
  	}
	return $num_of_jobs;
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