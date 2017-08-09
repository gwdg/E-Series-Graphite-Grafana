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
use Sys::Syslog;
use Scalar::Util qw(looks_like_number);

# Additional perl modules
use Time::HiRes;

my $DEBUG            = 0;
my $API_VER          = '/devmgr/v2';
my $API_TIMEOUT      = 15;
my $PUSH_TO_GRAPHITE = 1;

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

# Send our output to rsyslog.
openlog( 'eseries-metrics-collector', 'pid', 'local0' );

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
    logPrint("Executing in DEBUG mode, enjoy the verbosity :)");
}
if ( $opts{'c'} ) {
    if ( not -e -f -r $opts{'c'} ) {
        warn "Access problems to $opts{'c'} - check path and permissions.\n";
        exit 1;
    }
}
else {
    warn "Need to define a webservice proxy config file.\n";
    exit 1;
}
if ( $opts{'i'} ) {
#    if ( $opts{'i'} =~ $sys_id_pattern ) {
        $system_id = $opts{'i'};
        logPrint("Will only poll -$system_id-");
#    }
#    else {
#        logPrint( "$opts{'i'} does not seem to be a valid System ID,"
#                . " will validate it as System Name." );

        # maybe a system name was provided, lets figure it out.
#        $fetch_id_from_name = 1;
#    }
}
else {
    if ( $opts{'e'} ) {
        warn "For Embedded mode you need to provide an ID to poll, otherwise we can't collect any metrics.\n";
        exit 1;
    }

    logPrint("Will poll all known systems by Webservice proxy.");
}
if ( $opts{'n'} ) {

    # unset this, as user defined to not send metrics to graphite.
    $PUSH_TO_GRAPHITE = 0;
    logPrint("User decided to not push metrics to graphite.");
}
if ( $opts{'t'} ) {
    if ( looks_like_number( $opts{'t'} ) ) {
        $API_TIMEOUT = $opts{'t'};
        logPrint("Redefined timeout to $API_TIMEOUT seconds");
    }
    else {
        warn "Timeout ($opts{'t'}) is not a valid number. Using default.\n";
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

my $base_url
    = $config->{_}->{proto} . '://'
    . $endpoint . ':'
    . $config->{_}->{port};

logPrint("Base URL for Statistics Collection=$base_url") if $DEBUG;

my $ua = LWP::UserAgent->new;
$ua->timeout($API_TIMEOUT);

# TODO make this configurable.
$ua->ssl_opts(verify_hostname => 0);
$ua->default_header( 'Content-type', 'application/json' );
$ua->default_header( 'Accept',       'application/json' );
$ua->default_header( 'Authorization',
    'Basic ' . encode_base64( $username . ':' . $password ) );
my $t0 = Benchmark->new;

my $response;
if ($fetch_id_from_name) {

    # Try to find out ID based on name.
    $response = $ua->get( $base_url . $API_VER . '/storage-systems' );

    if ( $response->is_success ) {
        if ($fetch_id_from_name) {
            $system_id = get_sysid_from_name( $response, $opts{'i'} );
            logPrint("Found SystemID $system_id for $opts{'i'}");
        }
    }
    else {
        logPrint("Request FAILED: " . $response->status_line, 'ERR');
        die $response->status_line;
    }
}

if ($system_id) {
    logPrint("API: Calling storage-systems/{system-id} ...");
    $response
        = $ua->get( $base_url . $API_VER . '/storage-systems/' . $system_id );
}
else {
    logPrint("API: Calling storage-systems...");
    $response = $ua->get( $base_url . $API_VER . '/storage-systems' );
}
my $t1 = Benchmark->new;
my $td = timediff( $t1, $t0 );
logPrint( "API: Call took " . timestr($td) );

if ( $response->is_success ) {
    my $storage_systems = from_json( $response->decoded_content );

    print Dumper( \$storage_systems ) if $DEBUG;
    if ($system_id) {

        # When polling only one system we get a Hash.
        my $stg_sys_name = $storage_systems->{name};
        my $stg_sys_id   = $storage_systems->{id};

        logPrint("Processing $stg_sys_name ($stg_sys_id)");

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

        # All systems polled, we get an array of hashes.
        for my $stg_sys (@$storage_systems) {
            my $stg_sys_name = $stg_sys->{name};
            my $stg_sys_id   = $stg_sys->{id};

            logPrint("Processing $stg_sys_name ($stg_sys_id)");

            $metrics_collected->{$stg_sys_name} = {};

            get_vol_stats( $stg_sys_name, $stg_sys_id, $metrics_collected );
#            get_drive_stats( $stg_sys_name, $stg_sys_id, $metrics_collected );
        }
    }
}
else {
    if ( $response->code eq '404' ) {
        warn "The SystemID: " . $system_id . ", is unknown by the proxy.\n";
        warn "Please check the documentation on how to search for it."
            . " Or try providing the System Name.\n";
        exit 1;
    }
    else {
        die $response->status_line;
    }
}

print "Metrics Collected: \n " . Dumper( \$metrics_collected ) if $DEBUG;
if ($PUSH_TO_GRAPHITE) {
    post_to_influxdb($metrics_collected);
}
else {
    logPrint(
        "No metrics sent to graphite. Remove the -n if this was not intended."
    );
}

logPrint('Execution completed');
exit 0;

# Utility sub to send info to rsyslog and STDERR if in debug mode.
sub logPrint {
    my $text = shift;
    my $level = shift || 'info';

    if ($text) {
        syslog( $level, $text );
        $DEBUG && warn $level . ' ' . $text . "\n";
    }
}

# Invoke remote API to get per volume statistics.
sub get_vol_stats {
    my ( $sys_name, $sys_id, $met_coll ) = (@_);

    my $t0 = Benchmark->new;
    logPrint("API: Calling analysed-volume-statistics");
    my $stats_response
        = $ua->get( $base_url
            . $API_VER
            . '/storage-systems/'
            . $sys_id
            . '/analysed-volume-statistics' );
    my $t1 = Benchmark->new;
    my $td = timediff( $t1, $t0 );
    logPrint( "API: Call took " . timestr($td) );
    if ( $stats_response->is_success ) {
        my $vol_stats = from_json( $stats_response->decoded_content );
        logPrint( "get_vol_stats: Number of vols: " . scalar(@$vol_stats) );

        # skip if no vols present on this system
        if ( scalar(@$vol_stats) ) {
            process_vol_metrics( $sys_name, $vol_stats, $metrics_collected );
        }
        else {
            warn "Not processing $sys_name because it has no Volumes\n"
                if $DEBUG;
        }
    }
    else {
        die $stats_response->status_line;
    }
}

# Coalece Collecter metrics into custom structure, to just store the ones
# we care about.
sub process_vol_metrics {
    my ( $sys_name, $vol_mets, $met_coll ) = (@_);
    our %vol_metrics;

    for my $vol (@$vol_mets) {
        my $vol_name = $vol->{volumeName};
        logPrint( "process_vol_metrics: Volume Name " . $vol_name ) if $DEBUG;
        my $vol_met_key = "volume_statistics.$vol_name";
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
    logPrint("post_to_graphite: Issuing new socket connect.") if $DEBUG;
    my $connection = IO::Socket::INET->new(
        PeerAddr => $local_relay_server,
        PeerPort => $local_relay_port,
        Timeout  => $local_relay_timeout,
        Proto    => $local_relay_proto,
    );

    if ( !defined $connection ) {
        $socket_err = $! || 'failed without a specific library error';
        logPrint(
            "post_to_graphite: New socket connect failure with reason: [$socket_err]",
            "err"
        );
    }

    # Send metrics and upon error fail fast
    foreach my $system ( keys %$met_coll ) {
        logPrint("post_to_graphite: Build Metrics for -$system-") if $DEBUG;
        foreach my $vols ( keys %{ $met_coll->{$system} } ) {
            logPrint("post_to_graphite: Build Metrics for vol -$vols-")
                if $DEBUG;
            foreach my $mets ( keys %{ $met_coll->{$system}->{$vols} } ) {
                $full_metric
                    = $metrics_path . "."
                    . $system . "."
                    . $vols . "."
                    . $mets . " "
                    . $met_coll->{$system}->{$vols}->{$mets} . " "
                    . $epoch;
                logPrint( "post_to_graphite: Metric: " . $full_metric )
                    if $DEBUG;

                if ( !defined $connection->send("$full_metric\n") ) {
                    $socket_err = $!
                        || 'failed without a specific library error';
                    logPrint(
                        "post_to_graphite: Socket failure with reason: [$socket_err]",
                        "err"
                    );
                    undef $connection;
                }
            }
        }
    }
}

# Manage sending the metrics to InfluxDB
sub post_to_influxdb {
    my ($met_coll)              = (@_);
    my $connection_timeout      = $config->{'influxdb'}->{'timeout'};
    my $connection_server       = $config->{'influxdb'}->{'server'};
    my $connection_protocol     = $config->{'influxdb'}->{'https'};
    my $connection_port         = $config->{'influxdb'}->{'port'};
    my $connection_user         = $config->{'influxdb'}->{'user'};
    my $connection_password     = $config->{'influxdb'}->{'password'};   
    my $connection_database     = $config->{'influxdb'}->{'database'};   

    my $connection_url          = $connection_protocol . "://" . $connection_server . ":" . $connection_port . "/write?db=" . $connection_database;

    logPrint("post_to_influxdb: using url: " . $connection_url) if $DEBUG;

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
        logPrint("post_to_influxdb: build metrics for [$system]") if $DEBUG;

        foreach my $volume ( keys %{ $met_coll->{$system} } ) {
            logPrint("post_to_graphite: Build Metrics for volume [$volume]") if $DEBUG;

            foreach my $metric ( keys %{ $met_coll->{$system}->{$volume} } ) {

                $metric_line
                    = $volume_measurement   . ","
                    . "system=" . $system   . ","
                    . "volume=" . $volume   . " "
                    . $metric . "=" . $met_coll->{$system}->{$volume}->{$metric} . " "
                    . $epoch_ns;

                $metric_lines   = $metric_line . "\n";
                $num_lines      = $num_lines + 1;

                if ( $num_lines >= $max_metrics_to_send ) {
                    logPrint( "post_to_influxdb: sending batch of metrics to influxdb...") if $DEBUG;
                    $num_lines = 0;

                    my $response = $connection->post( $connection_url, Content => $metric_lines );

                    if ( $response->is_success ) {
                        $metric_lines = "";
                    }
                    else {
                        logPrint("post_to_influxdb: request FAILED: " . $response->status_line, 'ERR');
                        return;
                    }
                }
            }
        }
    }

    # Send the rest of the metrics
    my $response = $connection->post( $connection_url, Content => $metric_lines );

    if ( !$response->is_success ) {
        logPrint("post_to_influxdb: request FAILED: " . $response->status_line, 'ERR');
    }
}

# Invoke remote API to get per drive statistics.
sub get_drive_stats {
    my ( $sys_name, $sys_id, $met_coll ) = (@_);

    my $t0 = Benchmark->new;
    logPrint("API: Calling analysed-drive-statistics");
    my $stats_response
        = $ua->get( $base_url
            . $API_VER
            . '/storage-systems/'
            . $sys_id
            . '/analysed-drive-statistics' );
    my $t1 = Benchmark->new;
    my $td = timediff( $t1, $t0 );
    logPrint( "API: Call took " . timestr($td) );
    if ( $stats_response->is_success ) {
        my $drive_stats = from_json( $stats_response->decoded_content );
        logPrint(
            "get_drive_stats: Number of drives: " . scalar(@$drive_stats) );

        # skip if no drives present on this system (really possible?)
        if ( scalar(@$drive_stats) ) {
            process_drive_metrics( $sys_name, $drive_stats,
                $metrics_collected );
        }
        else {
            warn "Not processing $sys_name because it has no Drives\n"
                if $DEBUG;
        }
    }
    else {
        die $stats_response->status_line;
    }
}

# Coalece Drive metrics into custom structure, to just store the ones
# we care about.
sub process_drive_metrics {
    my ( $sys_name, $drv_mets, $met_coll ) = (@_);

    for my $drv (@$drv_mets) {
        my $disk_id = $drv->{diskId};
        logPrint( "process_drive_metrics: DiskID " . $disk_id ) if $DEBUG;
        my $drv_met_key = "drive_statistics.$disk_id";
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

# Invoke remote API to get live performance statistics.
sub get_live_statistics {
    my ( $sys_name, $sys_id, $met_coll ) = (@_);

    my $t0 = Benchmark->new;
    logPrint("API: Calling live-statistics");
    my $stats_response
        = $ua->get( $base_url
            . $API_VER
            . '/storage-systems/'
            . $sys_id
            . '/live-statistics' );
    my $t1 = Benchmark->new;
    my $td = timediff( $t1, $t0 );
    logPrint( "API: Call took " . timestr($td) );
    if ( $stats_response->is_success ) {
        my $live_stats = from_json( $stats_response->decoded_content );

        #print  Dumper (\$live_stats) if $DEBUG;
        my $cont_stats  = %{$live_stats->{'controllerStats'}};

        foreach my $controller (@$cont_stats) {
            print "controller_stats for controllerId: $controller->{'controllerId'} - ". $controller->{'cpuUtilizationStats'}[0]->{'maxCpuUtilization'}. " \n" if $DEBUG;
            print Dumper(\$controller) if $DEBUG;
        }
        exit 0;

    }
    else {
        die $stats_response->status_line;
    }
}
