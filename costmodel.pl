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
my $metrics_file = "C:/Users/mian/Documents/research/academic-profile/academic job/pdf/york/MRBurst/MRBurst/metrics_existing_job";
my $browser = WWW::Mechanize->new();

my $bw = 245; #285 Mbps

my %job_profile = ();
$job_profile{'ii'}{'mc'} =  0.122222222; #inverted index
$job_profile{'ii'}{'mt'} =  52.13122222;
$job_profile{'ii'}{'t'} =  7.10880303;

$job_profile{'wc'}{'mc'} =  0.122222222; #word count
$job_profile{'wc'}{'mt'} =  50.519;
$job_profile{'wc'}{'t'} =  6.888954545;

$job_profile{'grep'}{'mc'} =  0.122222222; #grep
$job_profile{'grep'}{'mt'} =  8.960333333;
$job_profile{'grep'}{'t'} =  1.221863636;

my $mc = 0.122222222;
my $mt = 52.13122222;
my $t = 7.10880303;

if (edgeBusy()) {
	print "burst out to core\n";
} else {
	print "submit in edge\n";
}

sub edgeBusy {
	# download the json page:
	# ref: http://beerpla.net/2008/03/27/parsing-json-in-perl-by-example-southparkstudioscom-south-park-episodes/
	print "Getting Job Tracker metrics json $json_url \n or reading $metrics_file...\n";
	$browser->get( $json_url );
	my $content = $browser->content();
	my $json = $content; # from URL

  my $metrics_fn = $metrics_file;
	#open(my $fh, "<", $metrics_fn) or die "cannot open < $metrics_fn: $!";
	#my $json = <$fh>; # from file

	#print "json object: " . $json . "\n";

# parsing json: http://search.cpan.org/~bkb/JSON-Parse-0.30/lib/JSON/Parse.pod
# http://www.tutorialspoint.com/json/json_perl_example.htm
# method 1  most readable
	my $metrics = decode_json($json);
	#print Dumper($metrics);

	# default of zero
  my $maps_completed = 0;
  my $pending_maps = 0;

#job tracker level metrics -- global
	my $jobtracker = $metrics -> { 'mapred' } -> { 'jobtracker' }[0][1];
	print "jobtracker...\n";
	print "***  job tracker level metrics -- global ***\n";
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
  		print "number of MR jobs (size of @jobs) = $num_of_jobs\n";
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
  	  		print "not a map task but:" . $taskType . "\n";
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

		my $cmd = "hadoop job -counter " . $current_job{'name'} . " org.apache.hadoop.mapreduce.JobCounter TOTAL_LAUNCHED_MAPS";
		print "executing command: $cmd\n";
		my $output = `$cmd`;
		print "output: $output\n";

		if ($output =~ /([0-9]+)/) {
  		$maps_completed = $1;
		} else {
    	$maps_completed = 0;
    }

		$pending_maps = $current_job{'demand'};
    $maps_completed = $maps_completed - $current_job{'runningTasks'};

		print "maps_completed =" . $maps_completed . "\n";
		print "i=$i, j=$j, k=$k, l=$l\n";
		print "map profile for a job...\n";
		print "[job] name=" . $current_job{'name'} . "\n";
		print "runningTasks [maps]=" . $current_job{'runningTasks'} . "\n";
		print "pending_maps or demand=" . $pending_maps . "\n";
	}

  my $slots = $map_slots;
  my $data = 50;

  my $cost_e = ($data - ($maps_completed * $job_profile{'ii'}{'mc'}))/$slots * $job_profile{'ii'}{'t'};
  my $cost_i = ($data * $job_profile{'grep'}{'t'})/$slots;
  my $cost_l = $cost_e + $cost_i;

  # We begin by assuming the core is idle.
  my $cost_ri = ($data * $job_profile{'grep'}{'t'}) / $slots + ($data * 1024) / ($bw * 60/8);

  print "execution cost of existing job = $cost_e\n";
  print "execution cost new job = $cost_i\n";
  print "cost of executing new job locally (edge) in presence of old job = $cost_l\n";
  print "cost of executing new job remotely (core) = $cost_ri\n";

  return ($cost_l > $cost_ri );
}

