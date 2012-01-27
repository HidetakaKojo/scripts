#!/usr/bin/perl

use strict;
use warnings;

use Time::HiRes qw/sleep/;
use DBI;
use Getopt::Long;

my $port = 3306;
my $num  = 1000;
my $results = Getopt::Long::GetOptions(
        "host=s" => \my $host,
        "port=i" => \$port,
        "num=i"  => \$num,
);
sub help {
  print "[USAGE] perl list_sql_commands.pl --host=[hostname] (--port=[port number] --num=[number of trials])\n";
  exit;
}

help() unless $host;

my $dsn = "DBI:mysql:information_schema;host=$host;port=$port";
my $dbh = DBI->connect($dsn, "root", "", { RaiseError => 1 });
my $sth = $dbh->prepare("select INFO from processlist");
my %appearance_count = ();
foreach my $cnt ( 0 .. ($num-1) ) {
  $sth->execute();
  ROW_LOOP: while (my $row = $sth->fetchrow_arrayref() ) {
    my $query = $row->[0];
    next ROW_LOOP if !$query;
    next ROW_LOOP if $query eq 'select INFO from processlist';
    $appearance_count{query_normalize($query)}++;
  }
  sleep(0.1);
}
$dbh->disconnect;

my @appearance_ranking = sort { $appearance_count{$b} <=> $appearance_count{$a} } keys %appearance_count;
foreach my $query ( @appearance_ranking ) {
  print sprintf("%d\t%s\n", $appearance_count{$query}, $query);
}

sub query_normalize {
  my $query = shift;
  $query = lc $query;
  $query =~ s/[\s\n]+/ /g;
  $query =~ s/([\'\"]).+?([\'\"])/$1?$2/g;
  $query =~ s/in\s?\([^\)]*\)/in (?,...,?)/g;
  $query =~ s/([^a-z])\d+$/$1?/g;
  $query =~ s/([^a-z])\d+([^a-z])/$1?$2/g;
  return $query;
}
