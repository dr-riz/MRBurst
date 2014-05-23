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
my $new_job_name = ();
my $existing_job_data_size = 0;

if ($numArgs != 5) {
	print "please, provide <binary_jar>, <class_name>, <data_dir>, <output_dir>, <output to edge 0|1>  as parameters\n";
	exit(1);
} else {
  $binary_jar = $ARGV[0]; print "binary_jar: $binary_jar\n";
  $class = $ARGV[1]; $new_job_name = "([a-zA-Z]+$)";

  if ($class =~ /([a-zA-Z]+)$/) {
   $new_job_name = $1;
	}

  print "class: $class, new_job_name: $new_job_name\n";

  my $existing_job_name = (); my $existing_job_id = ();
  my $cmd = 'mapred job -list | egrep \'^job\' | awk \'{print $1}\' | xargs -n 1 -I {} sh -c "mapred job -status {} | egrep \'^tracking\' | awk \'{print \$3}\'" | xargs -n 1 -I{} sh -c "echo -n {} | sed \'s/.*jobid=//\'; echo -n \' \';curl -s -XGET {} | grep \'Job Name\' | sed \'s/.* //\' | sed \'s/<br>//\'"';
  my $existing_job_name_str = `$cmd`;
  if ($existing_job_name_str =~ /([_a-zA-Z0-9]+) ([a-zA-Z]+)$/) {
    $existing_job_id = $1;
    $existing_job_name = $2;
	}
  print "existing_job_id = $existing_job_id\n";
	print "existing_job_name = $existing_job_name\n";

  $cmd = "mapred job -status $existing_job_id | grep file";
  my $output = `$cmd`;
  my $job_staging_file = ();
	if ($output =~  /([:_0-9\/\.a-zA-Z]+)$/) {
    $job_staging_file = $1;
	}
  print "job_staging_file:$job_staging_file\n";

  $cmd = "hadoop fs -cat $job_staging_file | grep mapred.input.dir";
  $output = `$cmd`;
  my $existing_job_input_folder = ();
  if ($output =~ /<value>([:_0-9\/\.a-zA-Z]+)<\/value>/) {
		$existing_job_input_folder = $1;
	}
  print "existing_job_input_folder:$existing_job_input_folder\n";

  $cmd = "hadoop fs -du -s $existing_job_input_folder";
  print "cmd=$cmd\n";
  $output = `$cmd`;
  print "output=$output\n";

	if ($output =~ /^([0-9]+)/) {
  	$existing_job_data_size = $1;
	}

  $existing_job_data_size  = $existing_job_data_size /(1024 * 1024 * 1024); #in GB
  print "existing_job_data_size = $existing_job_data_size GB\n";

  $data_dir = $ARGV[2]; print "input data dir: $data_dir\n";
  my $input_data_size = 0;
  $cmd = "hadoop fs -du -s $data_dir";
  $output = `$cmd`;
	if ($output =~ /^([0-9]+)/) {
  	$input_data_size = $1;
	}
  $input_data_size = $input_data_size /(1024 * 1024 * 1024); #in GB

  print "input_data_size = $input_data_size GB\n";

  $output_dir = $ARGV[3]; print "output data dir: $output_dir\n";
  $output_to_edge = $ARGV[4]; print "write results on edge?: $output_to_edge\n";
}

#if (edgeBusy()) {
if (1) {
	print "edge busy, burst out to core\n";
  #remoteAccess();
	#distcp();
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


sub remoteAccess {
#hadoop jar puma.jar org.apache.hadoop.examples.Grep hdfs://mianemaster/user/root/wikipedia_50GB /user/root//user/root/wikipedia_output/Grep_ra Peace 2>&1 | tee log.log

# submit hadoop job in the remote hadoop
	if ($output_to_edge) {
		# write the results back to the edge
 		my $cmd = "ssh $core_master 'hadoop jar $binary_jar $class hdfs://$edge_master/$data_dir hdfs://$edge_master/$output_dir'";
		print "executing command: $cmd\n";
		my $retcode = system($cmd) == 0 or die "system failed with ret code?: $?";
		print "retcode = $retcode\n";
	} else {
		my $cmd = "ssh $core_master 'hadoop jar $binary_jar $class hdfs://$edge_master/$data_dir $output_dir'";
		print "executing command: $cmd\n";
		my $retcode = system($cmd) == 0 or die "system failed with ret code?: $?";
		print "retcode = $retcode\n";
	}
}