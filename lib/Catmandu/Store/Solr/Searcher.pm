package Catmandu::Store::Solr::Searcher;

use Catmandu::Sane;
use Catmandu::Util qw(:is);
use Moo;

our $VERSION = "0.0302";

with 'Catmandu::Iterable';

has bag   => (is => 'ro', required => 1);
has query => (is => 'ro', required => 1);
has start => (is => 'ro', required => 1);
has limit => (is => 'ro', required => 1);
has sort  => (is => 'ro', required => 0);
has total => (is => 'ro');
has fl => (is => 'ro', lazy => 1, default => sub {"*"});
has fq => (is => 'ro');

sub generator {
    my ($self)    = @_;
    my $store     = $self->bag->store;
    my $name      = $self->bag->name;
    my $limit     = $self->limit;
    my $query     = $self->query;
    my $bag_field = $self->bag->bag_field;
    my $fq        = [];
    push @$fq, qq/{!type=lucene}$bag_field:"$name"/;
    if ( is_string( $self->fq ) ) {
        push @$fq, $self->fq;
    }
    elsif ( is_array_ref( $self->fq ) ) {
        push @$fq, @{ $self->fq };
    }
    sub {
        state $start = $self->start;
        state $total = $self->total;
        state $hits;
        if (defined $total) {
            return unless $total;
        }
        unless ($hits && @$hits) {
            if ($total && $limit > $total) {
                $limit = $total;
            }
            $hits = $store->solr->search(
                $query,
                {
                    start      => $start,
                    rows       => $limit,
                    fq         => $fq,
                    sort       => $self->sort,
                    fl         => $self->fl,
                    facet      => "false",
                    spellcheck => "false"
                }
            )->content->{response}{docs};
            $start += $limit;
        }
        if ($total) {
            $total--;
        }
        my $hit = shift(@$hits) || return;
        $self->bag->map_fields($hit);
        $hit;
    };
}

sub slice {
    my ($self, $start, $total) = @_;
    $start //= 0;

    my $old_total   = $self->total();
    my $old_start   = $self->start();
    my $new_start   = $old_start + $start;
    my $new_total   = $total;

    if ( is_natural($old_total) ) {
        my $old_end = $old_start + $old_total;

        if ( $new_start > $old_end ) {
            $new_total = 0;
        }

        elsif ( defined($new_total) ) {

            $new_total = ( $new_start + $new_total > $old_end ) ? $old_end - $new_start : $new_total;

        }
        else {

            $new_total = $old_end - $new_start;

        }
        $new_total = $new_total < 0 ? 0 : $new_total;

    }

    $self->new(
        bag   => $self->bag,
        query => $self->query,
        start => $new_start,
        limit => $self->limit,
        sort  => $self->sort,
        total => $new_total,
        fq    => $self->fq
    );
}

sub count {
    my ($self)    = @_;
    my $name      = $self->bag->name;
    my $bag_field = $self->bag->bag_field;
    my $fq        = [];
    push @$fq, qq/{!type=lucene}$bag_field:"$name"/;
    if ( is_string( $self->fq ) ) {
        push @$fq, $self->fq;
    }
    elsif ( is_array_ref( $self->fq ) ) {
        push @$fq, @{ $self->fq };
    }
    my $res       = $self->bag->store->solr->search(
        $self->query,
        {
            rows       => 0,
            fq         => $fq,
            facet      => "false",
            spellcheck => "false"
        }
    );
    my $total_count = $res->content->{response}{numFound};
    my $start       = $self->start() // 0;
    my $count       = $total_count - $start;
    $count          = $count < 0 ? 0 : $count;
    my $total       = $self->total();

    if ( is_natural($total) && $total < $count ) {
        $count      = $total;
    }

    $count;
}

around select => sub {

    my ($orig, $self, $arg1, $arg2) = @_;

    if ( is_string($arg1) && (is_value($arg2) || is_array_ref($arg2)) ) {

        my $fq = $self->fq;
        $fq = is_string($fq) ? [ $fq ] : is_array_ref( $fq ) ? $fq : [];

        if ( is_value($arg2) ) {

            push @$fq, qq({!type=lucene}$arg1:"$arg2");

        }
        elsif ( is_array_ref($arg2) ) {

            push @$fq, "{!type=lucene}".join(' OR ', map {
                qq($arg1:"$_")
            } @$arg2);

        }
        return $self->new(
            bag     => $self->bag,
            query   => $self->query,
            start   => $self->start,
            limit   => $self->limit,
            sort    => $self->sort,
            total   => $self->total,
            fq      => $fq
        );

    }

    $self->$orig($arg1, $arg2);

};

around detect => sub {

    my ($orig, $self, $arg1, $arg2) = @_;

    $self->select( $arg1 , $arg2 )->first();

};

sub first {

    my $self = $_[0];

    $self->new(
        bag     => $self->bag,
        query   => $self->query,
        start   => $self->start,
        limit   => $self->limit,
        sort    => $self->sort,
        total   => 1,
        fq      => $self->fq
    )->generator()->();

}

1;
