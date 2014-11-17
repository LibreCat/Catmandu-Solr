package Catmandu::Store::Solr::Bag;

use Catmandu::Sane;
use Catmandu::Util qw(:is);
use Carp qw(confess);
use Catmandu::Hits;
use Catmandu::Store::Solr::Searcher;
use Catmandu::Store::Solr::CQL;
use Moo;

with 'Catmandu::Bag';
with 'Catmandu::Searchable';
with 'Catmandu::Buffer';

sub generator {
    my ($self) = @_;
    my $store  = $self->store;
    my $name   = $self->name;
    my $limit  = $self->buffer_size;
    my $query  = qq/_bag:"$name"/;
    sub {
        state $start = 0;
        state $hits;
        unless ($hits && @$hits) {
            $hits =
              $store->solr->search($query, {start => $start, rows => $limit})
              ->content->{response}{docs};
            $start += $limit;
        }
        my $hit = shift(@$hits) || return;
        delete $hit->{_bag};
        $hit;
    };
}

sub count {
    my ($self) = @_;
    my $name   = $self->name;
    my $res    = $self->store->solr->search(
        qq/_bag:"$name"/,
        {
            rows       => 0,
            facet      => "false",
            spellcheck => "false",
            defType    => "lucene",
        }
    );
    $res->content->{response}{numFound};
}

sub get {
    my ($self, $id) = @_;
    my $name = $self->name;
    my $res  = $self->store->solr->search(
        qq/_bag:"$name" AND _id:"$id"/,
        {
            rows       => 1,
            facet      => "false",
            spellcheck => "false",
            defType    => "lucene",
        }
    );
    my $hit = $res->content->{response}{docs}->[0] || return;
    delete $hit->{_bag};
    $hit;
}

sub add {
    my ($self, $data) = @_;

    my @fields = (WebService::Solr::Field->new(_bag => $self->name));

    for my $key (keys %$data) {
        next if $key eq '_bag';
        my $val = $data->{$key};
        if (is_array_ref($val)) {
            is_value($_) && push @fields,
              WebService::Solr::Field->new($key => $_)
              foreach @$val;
        }
        elsif (is_value($val)) {
            push @fields, WebService::Solr::Field->new($key => $val);
        }
    }

    $self->buffer_add(WebService::Solr::Document->new(@fields));

    if ($self->buffer_is_full) {
        $self->commit;
    }
}

sub delete {
    my ($self, $id) = @_;
    my $name = $self->name;
    $self->store->solr->delete_by_query(qq/_bag:"$name" AND _id:"$id"/);
}

sub delete_all {
    my ($self) = @_;
    my $name = $self->name;
    $self->store->solr->delete_by_query(qq/_bag:"$name"/);
}

sub delete_by_query {
    my ($self, %args) = @_;
    my $name = $self->name;
    $self->store->solr->delete_by_query(qq/_bag:"$name" AND ($args{query})/);
}

sub commit { # TODO better error handling
    my ($self) = @_;
    my $solr = $self->store->solr;
    my $err;
    if ($self->buffer_used) {
        eval { $solr->add($self->buffer) } or push @{ $err ||= [] }, $@;
        $self->clear_buffer;
    }
    eval { $solr->commit } or push @{ $err ||= [] }, $@;
    !defined $err, $err;
}

sub search {
    my ($self, %args) = @_;

    my $query = delete $args{query};
    my $start = delete $args{start};
    my $limit = delete $args{limit};
    my $bag   = delete $args{reify};

    my $name = $self->name;

    my $bag_fq = qq/_bag:"$name"/;

    if ( $args{fq} ) {
        if (is_array_ref( $args{fq})) {
            $args{fq} = [ $bag_fq , @{ $args{fq} } ];
        }
        else {
            $args{fq} = [$bag_fq, $args{fq}];
        }
    } else {
        $args{fq} = $bag_fq;
    }

    my $res = $self->store->solr->search($query, {%args, start => $start, rows => $limit});

    my $set = $res->content->{response}{docs};

    if ($bag) {
        $set = [map { $bag->get($_->{_id}) } @$set];
    } else {
        delete $_->{_bag} for @$set;
    }

    my $hits = Catmandu::Hits->new({
        limit => $limit,
        start => $start,
        total => $res->content->{response}{numFound},
        hits  => $set,
    });

    if ($res->facet_counts) {
        $hits->{facets} = $res->facet_counts;
    }

    if ($res->spellcheck) {
        $hits->{spellcheck} = $res->spellcheck;
    }

    $hits;
}

sub searcher {
    my ($self, %args) = @_;
    Catmandu::Store::Solr::Searcher->new(%args, bag => $self);
}

sub translate_sru_sortkeys {
    confess 'TODO';
}

sub translate_cql_query {
    Catmandu::Store::Solr::CQL->parse($_[1]);
}

sub normalize_query {
    $_[1] || "*:*";
}

=head1 SEE ALSO

L<Catmandu::Bag>, L<Catmandu::Searchable>

=cut

1;
