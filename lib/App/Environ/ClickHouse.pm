package App::Environ::ClickHouse;

our $VERSION = '0.3.2';

use strict;
use warnings;
use v5.10;
use utf8;

use AnyEvent::ClickHouse qw();
use App::Environ;
use App::Environ::Config;
use Carp qw(croak);
use HTTP::ClickHouse;
use Params::Validate qw(validate_pos);

my $INSTANCE;
my $ASYNC;

App::Environ->register( __PACKAGE__, postfork => sub { undef $INSTANCE } );

App::Environ::Config->register(qw(clickhouse.yml));

sub instance {
  my $class = shift;

  unless ($INSTANCE) {
    my $config = App::Environ::Config->instance;

    $INSTANCE = HTTP::ClickHouse->new(
      host       => $config->{clickhouse}{host},
      port       => $config->{clickhouse}{port},
      nb_timeout => $config->{clickhouse}{timeout},
      database   => '',
    );
  }

  return $INSTANCE;
}

sub async {
  my $class = shift;

  unless ($ASYNC) {
    my $config = App::Environ::Config->instance;

    my %opt = (
      host     => $config->{clickhouse}{host},
      port     => $config->{clickhouse}{port},
      database => '',
    );

    $ASYNC = bless { opt => \%opt };
  }

  return $ASYNC;
}

sub selectall_hash {
  my __PACKAGE__ $self = shift;

  my $cb = pop;
  croak 'No cb' unless $cb;

  my ($sql) = validate_pos( @_, 1 );

  AnyEvent::ClickHouse::clickhouse_select_hash( $self->{opt}, $sql, $cb,
    sub { $cb->( undef, @_ ) } );

  return;
}

1;

__END__

=head1 NAME

App::Environ::ClickHouse - get instance of HTTP::ClickHouse or AnyEvent::ClickHouse in App::Environ environment

=head1 SYNOPSIS

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

=head1 DESCRIPTION

App::Environ::ClickHouse used to get instance of HTTP::ClickHouse in App::Environ environment

=head1 AUTHOR

Andrey Kuzmin, E<lt>kak-tus@mail.ruE<gt>

=head1 SEE ALSO

L<https://github.com/kak-tus/App-Environ-ClickHouse>.

L<https://metacpan.org/pod/HTTP::ClickHouse>.

=cut
