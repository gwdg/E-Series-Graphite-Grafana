#!/usr/bin/perl

#
# Copyright 2016 Pablo Luis Zorzoli
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

use strict;
use warnings;
use LWP::UserAgent;
use Data::Dumper;
use MIME::Base64;
use JSON;
use Config::Tiny;
use Getopt::Std;
use Benchmark;
use Scalar::Util qw(looks_like_number);

# Additional perl modules
use Time::HiRes;
use IO::Async::Timer::Periodic;
use IO::Async::Loop;
use Log::Log4perl;

my $DEBUG            = 0;
my $API_VER          = '/devmgr/v2';
my $API_TIMEOUT      = 15;
my $PUSH_TO_GRAPHITE = 1;
my $POLLING_INTERVAL = 20;
my $BATCH_SIZE       = 100;

# How often to retry to connect when failing to connect to NetApp Santricity Webproxy API endpoint
use constant MAX_RETRIES        => 3;

# How long to wait between reconnects in ms
use constant SLEEP_ON_ERROR_MS  => 1000;

# Prevent SSL errors
$ENV{'PERL_LWP_SSL_VERIFY_HOSTNAME'} = 0;

# Selected metrics to collect from any Volume, for ease of reading keep sorted
our %vol_metrics = (
    'averageReadOpSize'          => 0,
    'averageWriteOpSize'         => 0,
    'combinedIOps'               => 0,
    'combinedResponseTime'       => 0,
    'combinedThroughput'         => 0,
    'flashCacheHitPct'           => 0,
    'flashCacheReadHitBytes'     => 0,
    'flashCacheReadHitOps'       => 0,
    'flashCacheReadResponseTime' => 0,
    'flashCacheReadThroughput'   => 0,
    'otherIOps'                  => 0,
    'queueDepthMax'              => 0,
    'queueDepthTotal'            => 0,
    'readCacheUtilization'       => 0,
    'readHitBytes'               => 0,
    'readHitOps'                 => 0,
    'readIOps'                   => 0,
    'readOps'                    => 0,
    'readPhysicalIOps'           => 0,
    'readResponseTime'           => 0,
    'readThroughput'             => 0,
    'writeCacheUtilization'      => 0,
    'writeHitBytes'              => 0,
    'writeHitOps'                => 0,
    'writeIOps'                  => 0,
    'writeOps'                   => 0,
    'writePhysicalIOps'          => 0,
    'writeResponseTime'          => 0,
    'writeThroughput'            => 0,
);

# Selected metrics to collect from any drive, for ease of reading keep sorted.
our %drive_metrics = (
    'averageReadOpSize'         => 0,
    'averageWriteOpSize'        => 0,
    'combinedIOps'              => 0,
    'combinedResponseTime'      => 0,
    'combinedThroughput'        => 0,
    'otherIOps'                 => 0,
    'readIOps'                  => 0,
    'readOps'                   => 0,
    'readPhysicalIOps'          => 0,
    'readResponseTime'          => 0,
    'readThroughput'            => 0,
    'writeIOps'                 => 0,
    'writeOps'                  => 0,
    'writePhysicalIOps'         => 0,
    'writeResponseTime'         => 0,
    'writeThroughput'           => 0,
);

# Metrics to collect from controller
our %controller_metrics = (
    'cpuAvgUtilization'         => 0,
    'averageReadOpSize'         => 0,
    'averageWriteOpSize'        => 0,
    'combinedIOps'              => 0,
    'combinedResponseTime'      => 0,
    'combinedThroughput'        => 0,
    'otherIOps'                 => 0,
    'readIOps'                  => 0,
    'readOps'                   => 0,
    'readPhysicalIOps'          => 0,
    'readResponseTime'          => 0,
    'readThroughput'            => 0,
    'writeIOps'                 => 0,
    'writeOps'                  => 0,
    'writePhysicalIOps'         => 0,
    'writeResponseTime'         => 0,
    'writeThroughput'           => 0,
);


my $metrics_collected;
my $system_id;

# Pattern to detect a valid system ID
my $sys_id_pattern = qr/\w{8}-\w{4}-\w{4}-\w{4}-\w{12}/;

# Flag to detect ID based on System Name.
my $fetch_id_from_name = 0;

# ---------------------------------------------------------------------------------------------------------------------
# Initialize logger

my $log4j_conf = q(

    log4perl.category.GWDG.NetApp = INFO, Screen, Logfile
#    log4perl.category.GWDG.NetApp = DEBUG, Logfile

    log4perl.appender.Logfile = Log::Log4perl::Appender::File
    log4perl.appender.Logfile.filename = /var/log/check_netapp.log
    log4perl.appender.Logfile.layout = Log::Log4perl::Layout::PatternLayout
    log4perl.appender.Logfile.layout.ConversionPattern = [%d %F:%M:%L] %m%n

    log4perl.appender.Screen = Log::Log4perl::Appender::Screen
    log4perl.appender.Screen.stderr = 0
    log4perl.appender.Screen.layout = Log::Log4perl::Layout::PatternLayout
    log4perl.appender.Screen.layout.ConversionPattern = [%d %F:%M:%L] %m%n
);

Log::Log4perl::init(\$log4j_conf);

our $log = Log::Log4perl::get_logger("GWDG::NetApp");

# Command line opts parsing and processing.
my %opts = ( h => undef );
getopts( 'hdnec:t:i:', \%opts );

if ( $opts{'h'} ) {
    print "Usage: $0 [options]\n";
    print "  -h: This help message.\n";
    print "  -n: Don't push to graphite.\n";
    print "  -d: Debug mode, increase verbosity.\n";
    print "  -c: config file with credentials and API End Point.\n";
    print "  -t: Timeout in secs for API Calls (Default=$API_TIMEOUT).\n";
    print "  -i: E-Series ID or System Name to Poll.";
    print " ID is bound to proxy instance. If not defined it will";
    print " use all appliances known by Proxy.\n";
    print "  -e: Embedded mode. Applicable with new HW Generation. More metrics available.\n";

    exit 0;
}
if ( $opts{'d'} ) {
    $DEBUG = 1;
    $log->info("Executing in DEBUG mode, enjoy the verbosity :)");
}
if ( $opts{'c'} ) {
    if ( not -e -f -r $opts{'c'} ) {
        $log->warn("Access problems to $opts{'c'} - check path and permissions.");
        exit 1;
    }
}
else {
    $log->warn("Need to define a webservice proxy config file.");
    exit 1;
}
if ( $opts{'i'} ) {
    $system_id = $opts{'i'};
    $log->info("Will only poll [$system_id]");
}
else {
    if ( $opts{'e'} ) {
        $log->warn("For Embedded mode you need to provide an ID to poll, otherwise we can't collect any metrics.");
        exit 1;
    }
    $log->debug("Will poll all known systems by Webservice proxy.");
}
if ( $opts{'n'} ) {

    # unset this, as user defined to not send metrics to graphite.
    $PUSH_TO_GRAPHITE = 0;
    $log->info("User decided to not push metrics to graphite.");
}
if ( $opts{'t'} ) {
    if ( looks_like_number( $opts{'t'} ) ) {
        $API_TIMEOUT = $opts{'t'};
        $log->info("Redefined timeout to $API_TIMEOUT seconds");
    }
    else {
        $log->warn("Timeout ($opts{'t'}) is not a valid number. Using default.");
    }
}

my $config = Config::Tiny->new;
$config = Config::Tiny->read( $opts{'c'} );

my $username = $config->{_}->{user};
my $password = $config->{_}->{password};

my $endpoint;

if ( $opts{'e'} ) {
    # embedded only supports restapi as directive.
    $endpoint = $config->{_}->{restapi};
}
else {
    # backwards compatible with old config directive _proxy_
    if ( defined $config->{_}->{proxy} ) {
        $endpoint = $config->{_}->{proxy};
    }
    else {
        $endpoint = $config->{_}->{restapi};
    }
}

# Build base url
our $base_url
    = $config->{_}->{proto} . '://'
    . $endpoint . ':'
    . $config->{_}->{port}
    . $API_VER;

$log->debug("Base URL for statistics collection: $base_url");

our $santricity_connection = LWP::UserAgent->new;
$santricity_connection->timeout($API_TIMEOUT);

# TODO make this configurable.
$santricity_connection->ssl_opts(verify_hostname => 0);
$santricity_connection->default_header( 'Content-type',     'application/json' );
$santricity_connection->default_header( 'Accept',           'application/json' );
$santricity_connection->default_header( 'Authorization',    'Basic ' . encode_base64( $username . ':' . $password ) );

# Setup main loop processing
our $iteration = 0;

my $loop = IO::Async::Loop->new;
 
my $timer = IO::Async::Timer::Periodic->new(
   interval => $POLLING_INTERVAL,
   on_tick  => \&main_loop,
);
 
$timer->start;
$loop->add( $timer );
$loop->run;

$log->info('Execution completed');
exit 0;

# ---------------------------------------------------------------------------------------------------------------------
# Main loop

sub main_loop {

    # Get iteration start time
    my $iteration_start_time = Time::HiRes::gettimeofday();

    # Iteration counter
    our $iteration;

    # Some statistics
    my $nr_systems_ok       = 0;
    my $nr_systems_failed   = 0;

    if ($system_id) {
        my $system = call_santricity_api($base_url . '/storage-systems/' . $system_id);

        # When polling only one system we get a Hash.
        my $system_name = $system->{name};
        my $system_id   = $system->{id};

        $log->debug("Processing $system_name [$system_id]");

        if ($system_drivecount > 0) {
            $log->debug("Processing $system_name [$system_id]");

            $metrics_collected->{$system_name} = {};
            $nr_systems_ok = $nr_systems_ok + 1;

            get_volume_stats($system_name, $system_id);
            get_controller_stats($system_name, $system_id);
#            get_drive_stats($system_name, $system_id);
        } else {
            # Skip system (was not reached => other operations will fail)
            $log->warn("Skipping $system_name [$system_id], no drives / not reachable!");
            $nr_systems_failed = $nr_systems_failed + 1;
        }

        if ( $opts{'e'} ) {
            # For embedded systems we get extra metrics to poll.
            # not ready yet.
            #get_live_statistics($system_name, $system_id);
        }
    }
    else {
        my $storage_systems = call_santricity_api($base_url . '/storage-systems' );

        # All systems polled, we get an array of hashes.
        for my $system (@$storage_systems) {

#            print "System hash: \n " . Dumper( \$system );

            my $system_name        = $system->{name};
            my $system_id          = $system->{id};
            my $system_drivecount  = $system->{driveCount};

            if ($system_drivecount > 0) {
                $log->debug("Processing $system_name [$system_id]");

                $metrics_collected->{$system_name} = {};
                $nr_systems_ok = $nr_systems_ok + 1;

                get_volume_stats($system_name, $system_id);
                get_controller_stats($system_name, $system_id);
    #            get_drive_stats($system_name, $system_id);
            } else {
                # Skip system (was not reached => other operations will fail)
                $log->warn("Skipping $system_name [$system_id], no drives / not reachable!");
                $nr_systems_failed = $nr_systems_failed + 1;
            }
        }
    }

#    print "Metrics Collected: \n " . Dumper( \$metrics_collected ) if $DEBUG;

    if ($PUSH_TO_GRAPHITE) {
        post_to_influxdb($metrics_collected);
    }
    else {
        $log->info("No metrics sent to graphite. Remove the -n if this was not intended.");
    }

    # Get iteration end time
    my $iteration_end_time = Time::HiRes::gettimeofday();
    my $iteration_duration_string = sprintf("%.4f", $iteration_end_time - $iteration_start_time);

    # Wait
    $iteration++;
    $log->info("Finished iteration [$iteration] in [$iteration_duration_string] seconds => waiting...");
}

# ---------------------------------------------------------------------------------------------------------------------
sub call_santricity_api {

    my $request_url = shift;

    our $santricity_connection;
    our $base_url;

    $log->debug("API request: " . $request_url);
    
    my $i = 1;
    while ($i <= MAX_RETRIES) {

        my $t0 = Benchmark->new;
 
        my $response = $santricity_connection->get($request_url);
 
        my $t1 = Benchmark->new;
        my $td = timediff( $t1, $t0 );
        $log->debug("Call took: " . timestr($td));

        if ( $response->is_success ) {
            my $data = from_json($response->decoded_content);
 #           print Dumper( \$storage_systems ) if $DEBUG;
            return $data;
        }

        # Request failed: wait + retry
        $log->error("API request failed: " . $response->status_line);
        $log->error("=> Retrying (try $i of " . MAX_RETRIES . ")");
        Time::HiRes::usleep(SLEEP_ON_ERROR_MS);
        $i++;
    }

    # Nothing else we can do
    return;
}

# ---------------------------------------------------------------------------------------------------------------------
# Invoke remote API to get per volume statistics.
sub get_volume_stats {

    my ($sys_name, $sys_id) = (@_);

    $log->debug("Calling analysed-volume-statistics...");

    my $vol_stats = call_santricity_api($base_url . '/storage-systems/' . $sys_id . '/analysed-volume-statistics');
    $log->debug("Number of vols: " . scalar(@$vol_stats));

    # Skip if no vols present on this system
    if ( scalar(@$vol_stats) ) {
        process_vol_metrics($sys_name, $vol_stats);
    }
    else {
        $log->warn("Not processing [$sys_name] because it has no volumes!");
    }
}

# ---------------------------------------------------------------------------------------------------------------------
# Invoke remote API to get controller statistics.
sub get_controller_stats {

    my ($sys_name, $sys_id) = (@_);

    $log->debug("Calling analysed-controller-statistics...");

    my $controller_stats = call_santricity_api($base_url . '/storage-systems/' . $sys_id . '/analysed-controller-statistics');
    $log->debug("Number of controllers: " . scalar(@$controller_stats));

    # Skip if no controller present on this system (this would be rather strange, of course ;)
    if ( scalar(@$controller_stats) ) {
        process_controller_metrics($sys_name, $controller_stats);
    }
    else {
        $log->warn("Not processing [$sys_name] because it has no controllers (WTF?)!");
    }
}

# ---------------------------------------------------------------------------------------------------------------------
# Coalece Collecter metrics into custom structure, to just store the ones
# we care about.
sub process_vol_metrics {
    my ($sys_name, $volume_metrics) = (@_);

    our %vol_metrics;
    our $metrics_collected;

    for my $volume (@$volume_metrics) {
        my $volume_name = $volume->{volumeName};
        $log->debug("Volume name: " . $volume_name );
        $metrics_collected->{$sys_name}->{$volume_name} = {};

        #print Dumper($vol);
        foreach my $metric_name (keys %{$volume}) {

            #print "Met name = $met_name\n";
            if (exists $vol_metrics{$metric_name}) {
                $metrics_collected->{$sys_name}->{$volume_name}->{$metric_name} = $volume->{$metric_name};
            }
        }
    }
}

# ---------------------------------------------------------------------------------------------------------------------
# Coalece Collecter metrics into custom structure, to just store the ones
# we care about.
sub process_controller_metrics {
    my ($sys_name, $controller_metrics) = (@_);

    our %controller_metrics;
    our $metrics_collected;

    my $controller_id = 1;
    for my $controller (@$controller_metrics) {

        # Create simple controller name: real controller ids are too long
        my $controller_name = "controller" . $controller_id;
        $controller_id = $controller_id + 1;

        $log->debug("Controller name: " . $controller_name);
        $metrics_collected->{$sys_name}->{$controller_name} = {};

        #print Dumper($vol);
        foreach my $metric_name ( keys %{$controller} ) {

            #print "Met name = $met_name\n";
            if ( exists $controller_metrics{$metric_name} ) {
                $metrics_collected->{$sys_name}->{$controller_name}->{$metric_name} = $controller->{$metric_name};
            }
        }
    }
}

# ---------------------------------------------------------------------------------------------------------------------
# Manage Sending the metrics to Graphite Instance
sub post_to_graphite {

    my ($met_coll)          = (@_);
    my $local_relay_timeout = $config->{'graphite'}->{'timeout'};
    my $local_relay_server  = $config->{'graphite'}->{'server'};
    my $local_relay_port    = $config->{'graphite'}->{'port'};
    my $metrics_path        = $config->{'graphite'}->{'root'};
    my $local_relay_proto   = $config->{'graphite'}->{'proto'};
    my $epoch               = time();
    my $full_metric;

    my $socket_err;
    $log->debug("Issuing new socket connect.");
    my $connection = IO::Socket::INET->new(
        PeerAddr => $local_relay_server,
        PeerPort => $local_relay_port,
        Timeout  => $local_relay_timeout,
        Proto    => $local_relay_proto,
    );

    if ( !defined $connection ) {
        $socket_err = $! || 'failed without a specific library error';
        $log->error("New socket connect failure with reason: [$socket_err]");
    }

    # Send metrics and upon error fail fast
    foreach my $system ( keys %$met_coll ) {
        $log->debug("Build Metrics for -$system-");
        foreach my $vols ( keys %{ $met_coll->{$system} } ) {
            $log->debug("Build Metrics for vol -$vols-");
            foreach my $mets ( keys %{ $met_coll->{$system}->{$vols} } ) {
                $full_metric
                    = $metrics_path . "."
                    . $system . "."
                    . $vols . "."
                    . $mets . " "
                    . $met_coll->{$system}->{$vols}->{$mets} . " "
                    . $epoch;
                $log->debug( "Metric: " . $full_metric );

                if ( !defined $connection->send("$full_metric\n") ) {
                    $socket_err = $! || 'failed without a specific library error';
                    $log->error("post_to_graphite: Socket failure with reason: [$socket_err]");
                    undef $connection;
                }
            }
        }
    }
}

# ---------------------------------------------------------------------------------------------------------------------
# Manage sending the metrics to InfluxDB
sub post_to_influxdb {
    
    my ($met_coll)              = (@_);
    my $connection_timeout      = $config->{'influxdb'}->{'timeout'};
    my $connection_server       = $config->{'influxdb'}->{'server'};
    my $connection_protocol     = $config->{'influxdb'}->{'protocol'};
    my $connection_port         = $config->{'influxdb'}->{'port'};
    my $connection_user         = $config->{'influxdb'}->{'user'};
    my $connection_password     = $config->{'influxdb'}->{'password'};   
    my $connection_database     = $config->{'influxdb'}->{'database'};   

    my $connection_url          = $connection_protocol . "://" . $connection_server . ":" . $connection_port . "/write?db=" . $connection_database;

    $log->debug("Using URL: " . $connection_url);

    # User higher resolution time
    my ($s, $usec)              = Time::HiRes::gettimeofday();
    my $epoch_ns                = ($s * 1000000 + $usec) * 1000;

    my $volume_measurement      = "eseries_volume";
    my $controller_measurement  = "eseries_controller";
    my $drives_measurement      = "eseries_drives";

    my $metric_line;
    my $metric_lines            = "";
    my $num_lines               = 0;

    # Setup connection
    my $connection = LWP::UserAgent->new;
    $connection->timeout($API_TIMEOUT);
    $connection->ssl_opts(verify_hostname => 0);
    $connection->default_header( 'Content-type',    'application/json' );
    $connection->default_header( 'Accept',          'application/json' );
    $connection->default_header( 'Authorization',   'Basic ' . encode_base64( $connection_user . ':' . $connection_password ) );

    # Send metrics for volumes
    foreach my $system ( keys %$met_coll ) {
        $log->debug("Build metrics for [$system]");
        my $system_tr = $system;
        $system_tr =~ tr/ /_/;

        foreach my $volume ( keys %{ $met_coll->{$system} } ) {
            $log->debug("Build metrics for volume [$volume]");
            my $volume_tr =  $volume;
            $volume_tr =~ tr/ /_/;

            foreach my $metric ( keys %{ $met_coll->{$system}->{$volume} } ) {

                $metric_line
                    = $volume_measurement      . ","
                    . "system=" . $system_tr   . ","
                    . "volume=" . $volume_tr   . " "
                    . $metric . "=" . $met_coll->{$system}->{$volume}->{$metric} . " "
                    . $epoch_ns;

                $metric_lines   = $metric_lines . $metric_line . "\n";
                $num_lines      = $num_lines + 1;

                if ( $num_lines >= $BATCH_SIZE ) {
                    $log->debug("Sending batch of metrics to influxdb...");
                    $num_lines = 0;

                    my $response = $connection->post($connection_url, Content => $metric_lines);

#                    print "InfluxDB request content: " . $metric_lines;

                    if ( $response->is_success ) {
                        $metric_lines = "";
                    }
                    else {
                        $log->error("Request FAILED: " . $response->status_line);
                        return;
                    }
                }
            }
        }
    }

    # Send the rest of the metrics
    $log->debug( "Metrics to send: " . $metric_lines);
    my $response = $connection->post( $connection_url, Content => $metric_lines );

    if ( !$response->is_success ) {
        $log->debug("Request FAILED: " . $response->status_line);
    }
}

# ---------------------------------------------------------------------------------------------------------------------
# Invoke remote API to get per drive statistics.
sub get_drive_stats {

    my ($sys_name, $sys_id) = (@_);

    $log->debug("Calling analysed-drive-statistics...");

    my $drive_stats = call_santricity_api($base_url . '/storage-systems/' . $sys_id . '/analysed-drive-statistics');
    $log->debug("Number of drives: " . scalar(@$drive_stats));

    # skip if no drives present on this system (really possible?)
    if (scalar(@$drive_stats)) {
        process_drive_metrics($sys_name, $drive_stats);
    }
    else {
        $log->warn("Not processing [$sys_name] because it has no drives.");
    }
}

# ---------------------------------------------------------------------------------------------------------------------
# Coalece Drive metrics into custom structure, to just store the ones
# we care about.
sub process_drive_metrics {

    my ($sys_name, $drive_metrics) = (@_);

    our %drive_metrics;
    our $metrics_collected;

    for my $drive (@$drive_metrics) {

        my $disk_id = $drive->{diskId};
        $log->debug("Disk-ID: " . $disk_id);

        $metrics_collected->{$sys_name}->{$disk_id} = {};

        #print Dumper($drive);
        foreach my $met_name ( keys %{$drive} ) {

            if ( exists $drive_metrics{$met_name} ) {
                $metrics_collected->{$sys_name}->{$disk_id}->{$met_name} = $drive->{$met_name};
            }
        }
    }
}

# ---------------------------------------------------------------------------------------------------------------------
# Iterate Through the list of systems know by this proxy instance trying to
# find the provided value with a System Name.
#
sub get_sysid_from_name {
    my ( $response, $id_provided ) = (@_);
    my $sys_id;

    #print Dumper(\$response);
    my $storage_systems = from_json( $response->decoded_content );

    for my $stg_sys (@$storage_systems) {
        my $stg_sys_name = $stg_sys->{name};
        my $stg_sys_id   = $stg_sys->{id};

        if ( $stg_sys_name eq $id_provided ) {
            return $stg_sys_id;
        }
    }

    # No Match Found!
    $log->warn("Not able to match $id_provided with a System Name, giving up!");
    exit 1;
}

# ---------------------------------------------------------------------------------------------------------------------
# Invoke remote API to get live performance statistics.
sub get_live_statistics {

    my ( $sys_name, $sys_id, $met_coll ) = (@_);

    $log->debug("Calling live-statistics...");

    my $live_stats = call_santricity_api($base_url . '/storage-systems/' . $sys_id . '/live-statistics');
    #print  Dumper (\$live_stats) if $DEBUG;
    my $cont_stats  = %{$live_stats->{'controllerStats'}};

    foreach my $controller (@$cont_stats) {
        $log->debug("controller_stats for controllerId: $controller->{'controllerId'} - ". $controller->{'cpuUtilizationStats'}[0]->{'maxCpuUtilization'}. " \n");
        print Dumper(\$controller) if $DEBUG;
    }
    exit 0;
}
