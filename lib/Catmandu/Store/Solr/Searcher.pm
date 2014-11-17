package Catmandu::Store::Solr::Searcher;

use Catmandu::Sane;
use Moo;

with 'Catmandu::Iterable';

has bag   => (is => 'ro', required => 1);
has query => (is => 'ro', required => 1);
has start => (is => 'ro', required => 1);
has limit => (is => 'ro', required => 1);
has sort  => (is => 'ro', required => 0);
has total => (is => 'ro');

sub generator {
    my ($self) = @_;
    my $store  = $self->bag->store;
    my $name   = $self->bag->name;
    my $limit  = $self->limit;
    my $query  = $self->query;
    my $fq     = qq/_bag:"$name"/;
    sub {
        state $start = $self->start;
        state $total = $self->total;
        state $hits;
        if (defined $total) {
            return unless $total;
        }
        unless ($hits && @$hits) {
            if ( $total && $limit > $total ) {
                $limit = $total;
            }
            $hits = $store->solr->search($query, {start => $start, rows => $limit, fq => $fq,sort => $self->sort})
              ->content->{response}{docs};
            $start += $limit;
        }
        if ($total) {
            $total--;
        }
        my $hit = shift(@$hits) || return;
        delete $hit->{_bag};
        $hit;
    };
}

sub slice { # TODO constrain total?
    my ($self, $start, $total) = @_;
    $start //= 0;
    $self->new(
        bag   => $self->bag,
        query => $self->query,
        start => $self->start + $start,
        limit => $self->limit,
        sort => $self->sort,
        total => $total,
    );
}

sub count {
    my ($self) = @_;
    my $name   = $self->bag->name;
    my $res    = $self->bag->store->solr->search(
        $self->query,
        {
            rows       => 0,
            fq         => qq/_bag:"$name"/,
            facet      => "false",
            spellcheck => "false",
            defType    => "lucene",
        }
    );
    $res->content->{response}{numFound};
}

1;
