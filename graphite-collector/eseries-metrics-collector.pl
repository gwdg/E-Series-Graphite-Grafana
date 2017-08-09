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
my %drive_metrics = (
    'averageReadOpSize'    => 0,
    'averageWriteOpSize'   => 0,
    'combinedIOps'         => 0,
    'combinedResponseTime' => 0,
    'combinedThroughput'   => 0,
    'otherIOps'            => 0,
    'readIOps'             => 0,
    'readOps'              => 0,
    'readPhysicalIOps'     => 0,
    'readResponseTime'     => 0,
    'readThroughput'       => 0,
    'writeIOps'            => 0,
    'writeOps'             => 0,
    'writePhysicalIOps'    => 0,
    'writeResponseTime'    => 0,
    'writeThroughput'      => 0,
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
   interval => $plugin->opts->wait,
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

    if ($system_id) {
        $storage_systems = call_santricity_api($base_url . '/storage-systems/' . $system_id);

        # When polling only one system we get a Hash.
        my $stg_sys_name = $storage_systems->{name};
        my $stg_sys_id   = $storage_systems->{id};

        $log->debug("Processing $stg_sys_name [$stg_sys_id]");

        $metrics_collected->{$stg_sys_name} = {};

        get_vol_stats( $stg_sys_name, $stg_sys_id, $metrics_collected );
#        get_drive_stats( $stg_sys_name, $stg_sys_id, $metrics_collected );

        if ( $opts{'e'} ) {
            # For embedded systems we get extra metrics to poll.
            # not ready yet.
            #get_live_statistics( $stg_sys_name, $stg_sys_id, $metrics_collected );
        }
    }
    else {
        $storage_systems = call_santricity_api($base_url . '/storage-systems' );

       # All systems polled, we get an array of hashes.
        for my $stg_sys (@$storage_systems) {
            my $stg_sys_name = $stg_sys->{name};
            my $stg_sys_id   = $stg_sys->{id};

            $log->debug("Processing $stg_sys_name ($stg_sys_id)");

            $metrics_collected->{$stg_sys_name} = {};

            get_vol_stats( $stg_sys_name, $stg_sys_id, $metrics_collected );
#            get_drive_stats( $stg_sys_name, $stg_sys_id, $metrics_collected );
        }
    }

    print "Metrics Collected: \n " . Dumper( \$metrics_collected ) if $DEBUG;

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

    $log->info("API request: " . $request_url);
    
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
sub get_vol_stats {
    my ( $sys_name, $sys_id, $met_coll ) = (@_);

    my $t0 = Benchmark->new;
    $log->debug("API: Calling analysed-volume-statistics");
    my $stats_response
        = $ua->get( $base_url
            . $API_VER
            . '/storage-systems/'
            . $sys_id
            . '/analysed-volume-statistics' );
    my $t1 = Benchmark->new;
    my $td = timediff( $t1, $t0 );
    $log->debug( "API: Call took " . timestr($td) );
    if ( $stats_response->is_success ) {
        my $vol_stats = from_json( $stats_response->decoded_content );
        $log->debug( "get_vol_stats: Number of vols: " . scalar(@$vol_stats) );

        # skip if no vols present on this system
        if ( scalar(@$vol_stats) ) {
            process_vol_metrics( $sys_name, $vol_stats, $metrics_collected );
        }
        else {
            $log->warn("Not processing $sys_name because it has no Volumes\n");
        }
    }
    else {
        die $stats_response->status_line;
    }
}

# ---------------------------------------------------------------------------------------------------------------------
# Coalece Collecter metrics into custom structure, to just store the ones
# we care about.
sub process_vol_metrics {
    my ( $sys_name, $vol_mets, $met_coll ) = (@_);
    our %vol_metrics;

    for my $vol (@$vol_mets) {
        my $vol_name = $vol->{volumeName};
        $log->debug( "process_vol_metrics: Volume Name " . $vol_name );
        my $vol_met_key = "$vol_name";
        $metrics_collected->{$sys_name}->{$vol_met_key} = {};

        #print Dumper($vol);
        foreach my $met_name ( keys %{$vol} ) {

            #print "Met name = $met_name\n";
            if ( exists $vol_metrics{$met_name} ) {
                $met_coll->{$sys_name}->{$vol_met_key}->{$met_name}
                    = $vol->{$met_name};
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

    # Maximum number of metrics to send per one call
    my $max_metrics_to_send     = 1000;

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

        foreach my $volume ( keys %{ $met_coll->{$system} } ) {
            $log->debug("Build metrics for volume [$volume]");

            foreach my $metric ( keys %{ $met_coll->{$system}->{$volume} } ) {

                $metric_line
                    = $volume_measurement   . ","
                    . "system=" . $system   . ","
                    . "volume=" . $volume   . " "
                    . $metric . "=" . $met_coll->{$system}->{$volume}->{$metric} . " "
                    . $epoch_ns;

                $metric_lines   = $metric_lines . $metric_line . "\n";
                $num_lines      = $num_lines + 1;

                if ( $num_lines >= $max_metrics_to_send ) {
                    $log->debug("Sending batch of metrics to influxdb...");
                    $num_lines = 0;

#                    my $response = $connection->post( $connection_url, Content => $metric_lines );

#                    if ( $response->is_success ) {
                        $metric_lines = "";
#                    }
#                    else {
#                        $log->error("Request FAILED: " . $response->status_line);
#                        return;
#                    }
                }
            }
        }
    }

    # Send the rest of the metrics
    $log->debug( "Metrics to send: " . $metric_lines);
#    my $response = $connection->post( $connection_url, Content => $metric_lines );

#    if ( !$response->is_success ) {
#        $log->debug("Request FAILED: " . $response->status_line);
#    }
}

# ---------------------------------------------------------------------------------------------------------------------
# Invoke remote API to get per drive statistics.
sub get_drive_stats {
    my ( $sys_name, $sys_id, $met_coll ) = (@_);

    my $t0 = Benchmark->new;
    $log->debug("API: Calling analysed-drive-statistics");
    my $stats_response
        = $ua->get( $base_url
            . $API_VER
            . '/storage-systems/'
            . $sys_id
            . '/analysed-drive-statistics' );
    my $t1 = Benchmark->new;
    my $td = timediff( $t1, $t0 );
    $log->debug("API: Call took " . timestr($td));
    if ( $stats_response->is_success ) {
        my $drive_stats = from_json( $stats_response->decoded_content );
        $log->debug("Number of drives: " . scalar(@$drive_stats));

        # skip if no drives present on this system (really possible?)
        if ( scalar(@$drive_stats) ) {
            process_drive_metrics( $sys_name, $drive_stats,
                $metrics_collected );
        }
        else {
            $log->warn("Not processing $sys_name because it has no Drives");
        }
    }
    else {
        die $stats_response->status_line;
    }
}

# ---------------------------------------------------------------------------------------------------------------------
# Coalece Drive metrics into custom structure, to just store the ones
# we care about.
sub process_drive_metrics {
    my ( $sys_name, $drv_mets, $met_coll ) = (@_);

    for my $drv (@$drv_mets) {
        my $disk_id = $drv->{diskId};
        $log->debug("DiskID " . $disk_id);
        my $drv_met_key = "$disk_id";
        $metrics_collected->{$sys_name}->{$drv_met_key} = {};

        #print Dumper($drv);
        foreach my $met_name ( keys %{$drv} ) {

            if ( exists $drive_metrics{$met_name} ) {
                $met_coll->{$sys_name}->{$drv_met_key}->{$met_name}
                    = $drv->{$met_name};
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
    warn "Not able to match $id_provided with a System Name, giving up!\n";
    exit 1;
}

# ---------------------------------------------------------------------------------------------------------------------
# Invoke remote API to get live performance statistics.
sub get_live_statistics {
    my ( $sys_name, $sys_id, $met_coll ) = (@_);

    my $t0 = Benchmark->new;
    $log->debug("API: Calling live-statistics");
    my $stats_response
        = $ua->get( $base_url
            . $API_VER
            . '/storage-systems/'
            . $sys_id
            . '/live-statistics' );
    my $t1 = Benchmark->new;
    my $td = timediff( $t1, $t0 );
    $log->debug("API: Call took " . timestr($td));
    if ( $stats_response->is_success ) {
        my $live_stats = from_json( $stats_response->decoded_content );

        #print  Dumper (\$live_stats) if $DEBUG;
        my $cont_stats  = %{$live_stats->{'controllerStats'}};

        foreach my $controller (@$cont_stats) {
            $log->debug("controller_stats for controllerId: $controller->{'controllerId'} - ". $controller->{'cpuUtilizationStats'}[0]->{'maxCpuUtilization'}. " \n");
            print Dumper(\$controller) if $DEBUG;
        }
        exit 0;

    }
    else {
        die $stats_response->status_line;
    }
}
