package Catmandu::Store::Solr::Bag;

use Catmandu::Sane;
use Catmandu::Util qw(:is);
use Carp qw(confess);
use Catmandu::Hits;
use Catmandu::Store::Solr::Searcher;
use Catmandu::Store::Solr::CQL;
use Moo;

our $VERSION = "0.0207";

with 'Catmandu::Bag';
with 'Catmandu::Searchable';
with 'Catmandu::Buffer';

# not defined as Moo attributes because it may be moved to Catmandu::Bag
sub bag_field { $_[0]->store->bag_field // '_bag' }
sub id_field { $_[0]->store->id_field // '_id' }

sub generator {
    my ($self) = @_;
    my $store     = $self->store;
    my $name      = $self->name;
    my $limit     = $self->buffer_size;
    my $bag_field = $self->bag_field;
    my $query  = qq/$bag_field:"$name"/;
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
        $self->map_fields($hit);
        $hit;
    };
}

sub count {
    my ($self) = @_;
    my $name      = $self->name;
    my $bag_field = $self->bag_field;
    my $res = $self->store->solr->search(
        qq/$bag_field:"$name"/,
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
    my $name      = $self->name;
    my $id_field  = $self->id_field;
    my $bag_field = $self->bag_field;
    my $res  = $self->store->solr->search(
        qq/$bag_field:"$name" AND $id_field:"$id"/,
        {
            rows       => 1,
            facet      => "false",
            spellcheck => "false",
            defType    => "lucene",
        }
    );
    my $hit = $res->content->{response}{docs}->[0] || return;
    $self->map_fields($hit);
    $hit;
}

sub add {
    my ($self, $data) = @_;

    my $id_field  = $self->id_field;
    my $bag_field = $self->bag_field;

    my @fields = (WebService::Solr::Field->new($bag_field => $self->name));

    if (defined $data->{_id}) {
        push @fields, WebService::Solr::Field->new($id_field => $data->{_id});
    }

    for my $key (keys %$data) {
        next if $key eq $bag_field or $key eq '_id';
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
    my $id_field  = $self->id_field;
    my $bag_field = $self->bag_field;
    $self->store->solr->delete_by_query(qq/$bag_field:"$name" AND $id_field:"$id"/);
}

sub delete_all {
    my ($self) = @_;
    my $name = $self->name;
    my $bag_field = $self->bag_field;
    $self->store->solr->delete_by_query(qq/$bag_field:"$name"/);
}
sub delete_by_query {
    my ($self, %args) = @_;
    my $name      = $self->name;
    my $bag_field = $self->bag_field;
    $self->store->solr->delete_by_query(qq/$bag_field:"$name" AND ($args{query})/);
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

    my $name      = $self->name;
    my $id_field  = $self->id_field;
    my $bag_field = $self->bag_field;

    my $bag_fq = qq/$bag_field:"$name"/;

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
        $set = [map { $bag->get($_->{$id_field}) } @$set];
    } else {
        $self->map_fields($_) for (@$set);
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

sub map_fields {
    my ($self, $item) = @_;
    my $id_field = $self->id_field;
    if ($id_field ne '_id') {
        $item->{_id} = delete $_->{$id_field};
    }
    delete $item->{$self->bag_field};
}

=head1 SEE ALSO

L<Catmandu::Bag>, L<Catmandu::Searchable>

=cut

1;
