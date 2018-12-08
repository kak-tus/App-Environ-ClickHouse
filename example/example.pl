#!/usr/bin/env perl

use strict;
use warnings;
use utf8;
use v5.10;

use lib 'lib';
$ENV{APPCONF_DIRS} = 'example';

use AE;
use App::Environ;
use App::Environ::ClickHouse;
use Data::Dumper;

App::Environ->send_event('initialize');

my $CH = App::Environ::ClickHouse->instance;

my $data = $CH->selectall_hash('SELECT 1');
say Dumper $data;

my $async = App::Environ::ClickHouse->async;

my $cv = AE::cv;

$async->selectall_hash(
  'SELECT 1',
  sub {
    my ( $data, $err ) = @_;

    if ($err) {
      say $err;
      $cv->send;
      return;
    }

    say Dumper $data;
    $cv->send;
  }
);

$cv->recv;

App::Environ->send_event('finalize:r');
