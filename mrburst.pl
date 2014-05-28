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

my $bw = 245; #285 Mbps
my %job_profile = ();

# profile data of different benchmarks
# Mc = data consumed by each map task
# Mt = average time spent by each map in the occupied slots
# t = execution time of 1 slot with 1 GB of job data

$job_profile{'invertedindex'}{'mc'} =  0.122222222; #inverted index
$job_profile{'invertedindex'}{'mt'} =  52.13122222;
$job_profile{'invertedindex'}{'t'} =  7.10880303;

$job_profile{'wordcount'}{'mc'} =  0.122222222; #word count
$job_profile{'wordcount'}{'mt'} =  50.519;
$job_profile{'wordcount'}{'t'} =  6.888954545;

$job_profile{'grep-search'}{'mc'} =  0.122222222; #grep
$job_profile{'grep-search'}{'mt'} =  8.960333333;
$job_profile{'grep-search'}{'t'} =  1.221863636;

$job_profile{'classification'}{'mc'} =  0.125; #classification
$job_profile{'classification'}{'mt'} =  40.9185;
$job_profile{'classification'}{'t'} =  5.4558;

# input: <binary.jar>, <class>, <local data dir>, <results data dir>, <output to edge 0|1>
my $numArgs = 0;
my $binary_jar = ();
my $class = ();
my $data_dir = ();
my $output_dir = ();
my $job_params = ();
my $output_to_edge = 0;

my $new_job_name = ();
my $input_data_size = 0;
my $existing_job_name = ();
my $existing_job = 0; # by default, there is no existing job
my $existing_job_data_size = 0;

$numArgs = $#ARGV + 1;
if ($numArgs != 6) {
	print "please, provide <binary_jar>, <class_name>, <data_dir>, <output_dir>, <job-input-params>, <output to edge 0|1>  as parameters\n";
	exit(1);
} else {
  $binary_jar = $ARGV[0]; print "binary_jar: $binary_jar\n";
  $class = $ARGV[1]; $new_job_name = "([a-zA-Z]+$)";

  if ($class =~ /([a-zA-Z]+)$/) {
   $new_job_name = lc($1);
	}

  print "class: $class, new_job_name: $new_job_name\n";

  # extract existing job metrics
  my $existing_job_id = ();
  my $cmd = 'mapred job -list | egrep \'^job\' | awk \'{print $1}\' | xargs -n 1 -I {} sh -c "mapred job -status {} | egrep \'^tracking\' | awk \'{print \$3}\'" | xargs -n 1 -I{} sh -c "echo -n {} | sed \'s/.*jobid=//\'; echo -n \' \';curl -s -XGET {} | grep \'Job Name\' | sed \'s/.* //\' | sed \'s/<br>//\'"';
  my $existing_job_name_str = `$cmd`;
  if ($existing_job_name_str =~ /([_a-zA-Z0-9]+) ([a-zA-Z-]+)$/) {
    $existing_job_id = $1;
    $existing_job_name = $2;
    $existing_job = 1;

    print "\n *** a job is already executing in edge ***\n";
    print "existing_job_id = $existing_job_id\n";
		print "existing_job_name = $existing_job_name\n";

	  $cmd = "mapred job -status $existing_job_id | grep file";
  	my $output = `$cmd`;
	  my $job_staging_file = ();
		if ($output =~  /([:_0-9\-\/\.a-zA-Z]+)$/) {
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
	  $output = `$cmd`;

		if ($output =~ /^([0-9]+)/) {
  		$existing_job_data_size = $1;
		}

  	$existing_job_data_size  = $existing_job_data_size /(1024 * 1024 * 1024); #in GB
	  print "existing_job_data_size = $existing_job_data_size GB\n\n";
  }

  $data_dir = $ARGV[2]; print "input data dir for new job: $data_dir\n";
  $cmd = "hadoop fs -du -s $data_dir";
  my $output = `$cmd`;
	if ($output =~ /^([0-9]+)/) {
  	$input_data_size = $1;
	}
  $input_data_size = $input_data_size /(1024 * 1024 * 1024); #in GB

  print "input_data_size = $input_data_size GB\n";

  $output_dir = $ARGV[3]; print "output data dir: $output_dir\n";
  $job_params = $ARGV [4]; print "parameters for job: $job_params\n";
  $output_to_edge = $ARGV[5]; print "write results on edge?: $output_to_edge\n";
}

if (edgeBusy()) {
	print "cheaper to execute in core, burst out to core\n";
  remoteAccess();
	#distcp();
} else {
	print "cheaper to execute in edge, submit in edge\n";
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

	#print "json object: " . $json . "\n";

# parsing json: http://search.cpan.org/~bkb/JSON-Parse-0.30/lib/JSON/Parse.pod
# http://www.tutorialspoint.com/json/json_perl_example.htm
# method 1  most readable
	my $metrics = decode_json($json);
	#print  Dumper($metrics);

	# default of zero
  my $maps_completed = 0;
  my $pending_maps = 0;

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
	print "**    job level metrics -- local  *******\n";
	my @jobs = $metrics -> { 'fairscheduler' } -> { 'jobs' };

  if($map_jobs_running > 0) {
		my $num_of_jobs = 0;
		if ((@jobs)) {
			$num_of_jobs = @jobs;
  		#print "number of MR jobs (size of @jobs) = $num_of_jobs\n";
  		#print Dumper(@jobs);
		}

		my %current_job = ();

		my $i=0, my $j=0, my $k=0, my $l=0;
		# following loop iterates and extracts from #jobs - array of array of hashes
		foreach my $tasks(@jobs){
			$i++;
			print "tasks = $tasks\n";
			foreach my $task (@{$tasks}) {
    		my $taskType = $metrics -> { 'fairscheduler' } -> { 'jobs' }[$j][0]-> {'taskType'}; # hash-level
	   		$j++;
  	  	if ($taskType eq "MAP" ) {
    			print "map task...collect metrics\n";
	    	} else {
          #print "not a map task but:" . $taskType . "\n";
    	  	next;
    		}
				#print "task = $task\n";
    		my $map_task = 0; # boolean to represent map or reduce task
	  		foreach my $unit (@{$task}) {  # unit is the atomic level hash or leaf node
    			$k++;
					#print "unit = $unit\n";

					my $count = keys %$unit;
  	    	if($count == 3) {
	  	    	$current_job{'name'} = $unit->{'name'};
      		}

	      	if($count == 5) {
  	    		$l++;
    	  		$current_job{'runningTasks'} = $unit->{'runningTasks'};
      	  	$current_job{'demand'} = $unit->{'demand'};

	      	#print out contents of hash
  		  	#while( my ($k, $v) = each %$unit ) {
    			#  print "key: $k, value: $v.\n";
    			#}
	      	}
		  	}
			}
  	}

		my $cmd = "mapred job -counter " . $current_job{'name'} . " org.apache.hadoop.mapreduce.JobCounter TOTAL_LAUNCHED_MAPS";
		#print "executing command: $cmd\n";
		my $output = `$cmd`;
		if ($output =~ /([0-9]+)/) {
  		$maps_completed = $1;
		} else {
    	$maps_completed = 0;
    }

		$pending_maps = $current_job{'demand'};
    $maps_completed = $maps_completed - $current_job{'runningTasks'};

		#print "i=$i, j=$j, k=$k, l=$l\n";
		print "map profile for existing job...\n";
 		print "maps_completed =" . $maps_completed . "\n";
		print "[job id] name=" . $current_job{'name'} . "\n";
		print "runningTasks [maps]=" . $current_job{'runningTasks'} . "\n";
		print "pending_maps or demand=" . $pending_maps . "\n";
	}

  my $slots = $map_slots;

  my $cost_e = 0; # by default, assume no job running and cost 0
  if($map_jobs_running > 0) {
  	if (defined $job_profile{$existing_job_name}) {
	  	$cost_e = ($existing_job_data_size - ($maps_completed * $job_profile{$existing_job_name}{'mc'}))/$slots * $job_profile{$existing_job_name}{'t'};
    } else {
      print "profiling information for \"$existing_job_name\" not present, so setting very high value\n";
      $cost_e = 9999999999;
    }
  }
  my $cost_i = ($input_data_size * $job_profile{$new_job_name}{'t'})/$slots;
  my $cost_l = $cost_e + $cost_i;

  # We begin by assuming the core is idle.
  my $cost_ri = ($input_data_size * $job_profile{$new_job_name}{'t'}) / $slots + ($input_data_size * 1024) / ($bw * 60/8);

  print "execution cost of existing job = $cost_e\n";
  print "execution cost new job in edge = $cost_i\n";
  print "cost of executing new job locally (edge) in presence of old job = $cost_l\n";
  print "cost of executing new job remotely (core) = $cost_ri\n";

  return ($cost_l > $cost_ri );
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