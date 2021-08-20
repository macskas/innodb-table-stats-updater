#!/usr/bin/perl

use strict;
use warnings;

use Getopt::Std;
use DBI;
use Digest::MD5 qw/md5_hex/;

my $dry_run = 0;
my $dbh;
$ENV{'PATH'} = '/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games';

sub do_error()
{
    my ($package, $filename, $line, $function_name) = caller(1);
    my $msg = shift || "-";
    my $outmsg = sprintf("%s ERROR > [%s:%d] %s() - %s\n", scalar localtime, $filename, $line, $function_name, $msg);
    print STDERR $outmsg;
    exit(1);
}

sub do_warn() {
    my ($package, $filename, $line, $function_name) = caller(1);
    my $msg = shift || "-";
    my $outmsg = sprintf("%s WARN  > [%s:%d] %s()  - %s\n", scalar localtime, $filename, $line, $function_name, $msg);
    print $outmsg;
}

sub do_info()
{
    my $msg = shift || "-";
    my $outmsg = sprintf("%s INFO  > %s\n", scalar localtime, $msg);
    print $outmsg;
}

sub do_help()
{
    print ("$0\n");
    print ("      -d       - database_name\n");
    print ("      -t       - table_name\n");
    print ("      -n       - dry_run(no updates)\n");
    print ("      -b <0|1> - binlog enable/disable (default: disabled)");
    exit(1);
}

sub table_distinct_data()
{
    my $database_name = shift;
    my $table_name = shift;
    my $uniq_stats = shift;
    my $results = {
        'total_cnt' => 0
    };

    &do_info("table_distinct_data($database_name, $table_name) - started");
    $dbh->do("USE `$database_name`") or &do_error($DBI::errstr);
    my @fields = ();
    foreach my $hash (keys %{$uniq_stats}) {
        my $data = $uniq_stats->{"$hash"};
        push(@fields, "COUNT(DISTINCT $data) AS `$hash`");
        $results->{$hash} = 0;
    }
    push(@fields, "COUNT(*) AS total_cnt");
    my $query = sprintf('SELECT %s FROM `%s`', join(", ", @fields), $table_name);
    my $sth = $dbh->prepare($query);
    $sth->execute() or &do_error($DBI::errstr);
    while (my $ref = $sth->fetchrow_hashref()) {
        foreach my $k_hash (keys %{$ref}) {
            $results->{"$k_hash"} = $ref->{"$k_hash"};
        }
    }
    $sth->finish;
    &do_info("table_distinct_data() - finished");
    return $results;
}

sub table_stat_update()
{
    my $now = time();
    my $index_stats = {};
    my $uniq_stats = {};

    my $database_name = shift;
    my $table_name = shift;

    &do_info("table_stat_update($database_name, $table_name) - started.");

    $dbh->do("USE `$database_name`") or &do_error($DBI::errstr);
    my $sth = $dbh->prepare("SELECT *,UNIX_TIMESTAMP(last_update) AS last_update_ts FROM mysql.innodb_index_stats where database_name = ? and table_name = ?");
    $sth->execute($database_name, $table_name) or &do_error($DBI::errstr);

    while (my $ref = $sth->fetchrow_hashref()) {
        # stat_value, stat_name, index_name, stat_description, last_update_ts
        #print "Found a row:  $ref->{'stat_description'}\n";
        #printf("\n", $ref->{'index_name'});
        my $stat_description = $ref->{'stat_description'};
        my $index_name = $ref->{'index_name'};
        if ($stat_description !~ /^([^\s]+,?)+$/) {
            next;
        }

        my $hash = md5_hex("$database_name|$table_name|$index_name|$stat_description");
        my $hash_desc = md5_hex("$database_name|$table_name|$stat_description");
        $index_stats->{"$hash"} = $ref;
        $index_stats->{"$hash"}->{'hash_desc'} = $hash_desc;
        $index_stats->{"$hash"}->{'tsdiff'} = $now - int($ref->{'last_update_ts'});
        $uniq_stats->{"$hash_desc"} = $stat_description;
    }
    $sth->finish;

    my $results = &table_distinct_data($database_name, $table_name, $uniq_stats);
    my $n_rows = $results->{'total_cnt'};
    if (!$n_rows) {
        &do_warn("Result is empty. No update.");
        return 0;
    }

    if (!$dry_run) {
        $sth = $dbh->prepare("UPDATE mysql.innodb_table_stats SET n_rows = ? WHERE database_name = ? AND table_name = ? LIMIT 1");
        $sth->execute($n_rows, $database_name, $table_name) or &do_error($DBI::errstr);
        my $rc = $sth->rows;
        if ($rc) {
            &do_info("innodb_table_stats - updated($database_name, $table_name): n_rows = $n_rows");
        } else {
            &do_info("innodb_table_stats - not updated($database_name, $table_name): n_rows = $n_rows");
        }
        $sth->finish;
    }

    foreach my $hash (keys %{$index_stats}) {
        my $cur = $index_stats->{"$hash"};
        my $hash_desc = $cur->{'hash_desc'};
        my $new_stat_value = defined($results->{"$hash_desc"}) ? $results->{"$hash_desc"} : -1;
        my $tsdiff = $cur->{'tsdiff'};
        my $stat_desc = $cur->{'stat_description'};
        if ($new_stat_value == -1) {
            &do_warn("Missing new stat value for: $database_name/$table_name/$stat_desc");
            next;
        }
        &do_info(sprintf('UPDATE innodb_index_stats, %s: %s -> %s'."\n", $cur->{'stat_description'}, $cur->{'stat_value'}, $new_stat_value));
        if (!$dry_run) {
            my $sub_sth = $dbh->prepare("UPDATE mysql.innodb_index_stats SET stat_value = ? WHERE database_name = ? AND table_name = ? AND stat_description = ?");
            $sub_sth->execute($new_stat_value, $database_name, $table_name, $stat_desc) or &do_error($DBI::errstr);
            my $rc = $sub_sth->rows;
            $sub_sth->finish;
            if ($rc) {
                &do_info("innodb_index_stats - updated($database_name, $table_name, $stat_desc): $new_stat_value");
            } else {
                &do_info("innodb_index_stats - not updated($database_name, $table_name, $stat_desc): $new_stat_value");
            }
        } else {
            &do_info("innodb_index_stats - would update($database_name, $table_name, $stat_desc): $new_stat_value");
        }
    }
    #print Dumper(@updates);
}

sub flush_table()
{
    my $database_name = shift;
    my $table_name = shift;

    &do_info("flush_table($database_name, $table_name) - started");
    my $success = 0;
    $dbh->do("USE `$database_name`") or &do_error($DBI::errstr);
    for (my $i=0; $i<15; $i++) {
        my $rc = $dbh->do("SET lock_wait_timeout=1;");
        if (!$rc) {
            $success = 0;
            last;
        }
        $rc = $dbh->do("FLUSH TABLE `$table_name`");
        if ($rc) {
            $success = 1;
            last;
        }
        sleep(1);
    }
    &do_info("flush_table($database_name, $table_name) - finished (success: $success)");
}

sub main()
{
    my %opts = ();
    getopts("d:t:nb:", \%opts);
    my $binlog_enabled = 0;
    
    if (!defined($opts{d})) {
        &do_help();
    }

    if (!defined($opts{t})) {
        &do_help();
    }

    if (defined($opts{n})) {
        $dry_run = 1;
        &do_info("dry_run = 1");
    }
    if (defined($opts{b}) && $opt{b} =~ /^[0-9]+$/) {
        $binlog_enabled = int($opts{b});
        &do_info("binlog_enabled = $binlog_enabled");
    }

    $dbh = DBI->connect("DBI:mysql:database=mysql;host=localhost;mysql_read_default_group=mysql", "", "",
        {
            'mysql_connect_timeout'     => 3,
            'mysql_auto_reconnect'      => 1,
            'PrintError'                => 1,
            'RaiseError'                => 0,
            'AutoCommit'                => 1
        }
    );
    $dbh->do("SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED");
    if (!$binlog_enabled) {
        $dbh->do("SET sql_log_bin=0");
    }
    &table_stat_update($opts{d}, $opts{t});
    if (!$dry_run) {
        &flush_table($opts{d}, $opts{t});
    }

}

&main();
