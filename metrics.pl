#!/usr/bin/perl -w
use strict;
use JSON;
use Data::Dumper;
use WWW::Mechanize;

my $json_url = "http://mianemaster:50030/metrics?format=json";
my $metrics_file = "C:/Users/mian/Documents/research/academic-profile/academic job/pdf/york/MRBurst_folder/MRBurst/metrics";
my $browser = WWW::Mechanize->new();

# References
# manipulating arrays: http://www.cs.mcgill.ca/~abatko/computers/programming/perl/howto/array/

# download the json page:
# ref: http://beerpla.net/2008/03/27/parsing-json-in-perl-by-example-southparkstudioscom-south-park-episodes/
print "Getting Job Tracker metrics json $json_url \n or reading $metrics_file...\n";
$browser->get( $json_url );
my $content = $browser->content();
#my $json = $content; # from URL

my $metrics_fn = $metrics_file;
open(my $fh, "<", $metrics_fn) or die "cannot open < $metrics_fn: $!";
my $json = <$fh>; # from file

#print "json object: " . $json . "\n";

# parsing json: http://search.cpan.org/~bkb/JSON-Parse-0.30/lib/JSON/Parse.pod
# http://www.tutorialspoint.com/json/json_perl_example.htm
# method 1  most readable
my $metrics = decode_json($json);
print Dumper($metrics);

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

my $num_of_jobs = 0;
if ((@jobs)) {
	$num_of_jobs = @jobs;
  print "number of MR jobs (size of @jobs) = $num_of_jobs\n";
  #print Dumper(@jobs);
}

my %current_job = ();
my @job_array = ();
my %job_hash = ();

my $i=0, my $j=0, my $k=0, my $l=0;
# following loop iterates and extracts from #jobs - array of array of hashes
foreach my $tasks(@jobs){
	$i++;
	print "tasks = $tasks\n";
	foreach my $task (@{$tasks}) {
    my $taskType = $metrics -> { 'fairscheduler' } -> { 'jobs' }[$j][0]-> {'taskType'}; # hash-level
   	$j++;
    if ($taskType eq "MAP" ) {
    	#print "map task...collect metrics\n";
    } else {
    	#print "not a map task but:" . $taskType . "\n";
      next;
    }
		#print "task = $task\n";
    my $map_task = 0; # boolean to represent map or reduce task
	  foreach my $unit (@{$task}) {  # unit is the atomic level hash or leaf node
    	$k++;
			#print "unit = $unit\n";

      my $job_id=();
			my $count = keys %$unit;
      if($count == 3) {
	      $current_job{'name'} = $unit->{'name'};
        my $jobid =  $unit->{'name'};
        $job_array[$l] = $jobid;
      }

      if($count == 5) {
        #$current_job{"$jobid"}{'runningTasks'} = $unit->{'runningTasks'};
        #$current_job{'name'} = $unit->{'name'};
      	$current_job{'runningTasks'} = $unit->{'runningTasks'};
        $current_job{'demand'} = $unit->{'demand'};

        $job_hash{$job_array[$l]}{'runningTasks'} =  $unit->{'runningTasks'};
        $job_hash{$job_array[$l]}{'demand'} =  $unit->{'demand'};
        $job_hash{$job_array[$l]}{'fairShare'} =  $unit->{'fairShare'};
      	#print out contents of hash
  	    #while( my ($k, $v) = each %$unit ) {
    	  	#print "key: $k, value: $v.\n";
	    	#}

      	$l++;
      }
	  }
	}
}

#hadoop job -counter job_201405141544_0017 org.apache.hadoop.mapreduce.JobCounter TOTAL_LAUNCHED_MAPS
#my $cmd = "hadoop job -counter " . $current_job{'name'} . " org.apache.hadoop.mapreduce.JobCounter TOTAL_LAUNCHED_MAPS";
#print "executing command: $cmd\n";
#my $output = `$cmd`;
my $output = "140 ";
print "output: $output\n";

my $maps_completed = 0; # default of zero
if ($output =~ /([0-9]+)/) {
   $maps_completed = $1;
}

print "maps_completed =" . ($maps_completed -  $current_job{'runningTasks'}) . "\n";
print "i=$i, j=$j, k=$k, l=$l\n";
print "map profile for a job...\n";
#print "\n\nprinting out details in #hash\n";
#while (my ($k,$v)=each %current_job){print "key:$k=value:$v\n"}

print "\n\nprinting out details in array\n";
#dump(@job_array);
print join(", ", @job_array);
#print join(@job_array, "\n");

print "\n\nprinting out details in #job_hash\n";
#while (my ($k,$v)=each %job_hash){print "key:$k=value:$v\n"}
print Dumper(%job_hash);