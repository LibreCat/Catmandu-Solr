package Catmandu::Store::Solr;

use Catmandu::Sane;
use Catmandu::Util qw(:is :array);
use Moo;
use WebService::Solr;
use Catmandu::Store::Solr::Bag;
use Catmandu::Error;

with 'Catmandu::Store';
with 'Catmandu::Transactional';

=head1 NAME

Catmandu::Store::Solr - A searchable store backed by Solr

=cut

our $VERSION = '0.0214';

=head1 SYNOPSIS

    use Catmandu::Store::Solr;

    my $store = Catmandu::Store::Solr->new(url => 'http://localhost:8983/solr' );

    my $obj1 = $store->bag->add({ name => 'Patrick' });

    printf "obj1 stored as %s\n" , $obj1->{_id};

    # Force an id in the store
    my $obj2 = $store->bag->add({ _id => 'test123' , name => 'Nicolas' });

    # send all changes to solr (committed automatically)
    $store->bag->commit;

    #transaction: rollback issued after 'die'
    $store->transaction(sub{
        $bag->delete_all();
        die("oops, didn't want to do that!");
    });

    my $obj3 = $store->bag->get('test123');

    $store->bag->delete('test123');

    $store->bag->delete_all;

    # All bags are iterators
    $store->bag->each(sub { ... });
    $store->bag->take(10)->each(sub { ... });

    # Some stores can be searched
    my $hits = $store->bag->search(query => 'name:Patrick');

=cut

has url => (is => 'ro', default => sub { 'http://localhost:8983/solr' });

has solr => (
    is      => 'ro',
    lazy    => 1,
    builder => '_build_solr',
);

has id_field  => (is => 'ro', default => sub { '_id' });
has bag_field => (is => 'ro', default => sub { '_bag' });
has on_error => (
    is => 'ro',
    isa => sub {
        array_includes([qw(throw ignore)],$_[0]) or die("on_error must be 'throw' or 'ignore'");
    },
    lazy => 1,
    default => sub { "throw" }
);

has _bags_used => (
    is => 'ro',
    lazy => 1,
    default => sub { []; }
);
around 'bag' => sub {

    my $orig = shift;
    my $self = shift;

    my $bags_used = $self->_bags_used;
    unless(array_includes($bags_used,$_[0])){
        push @$bags_used,$_[0];
    }

    $orig->($self,@_);
};

sub _build_solr {
    WebService::Solr->new($_[0]->url, {autocommit => 0, default_params => {wt => 'json'}});
}

sub transaction {
    my($self,$sub)=@_;

    if($self->{_tx}){
        return $sub->();
    }
    my $solr = $self->solr;
    my @res;

    eval {
        #mark store as 'in transaction'. All subsequent calls to commit only flushes buffers without setting 'commit' to 'true' in solr
        $self->{_tx} = 1;

        #transaction
        @res = $sub->();

        #flushing buffers of all bags
        for my $bag_name(@{ $self->_bags_used() }){
            $self->bag($bag_name)->commit();
        }

        #commit in solr
        $solr->commit;

        #remove mark 'in transaction'
        $self->{_tx} = 0;
        1;
    } or do {
        my $err = $@;
        eval { $solr->rollback };
        $self->{_tx} = 0;
        Catmandu::Error->throw($err);
    };

    @res;
}

=head1 SUPPORT

Solr schemas need to support an identifier field (C<_id> by default) and a bag
field (C<_bag> by default) to be able to store Catmandu items.

=head1 CONFIGURATION

=over

=item url

URL of Solr core

Default: C<http://localhost:8983/solr>

=item id_field

Name of unique field in Solr core.

Default: C<_id>

This Solr field is mapped to C<_id> when retrieved

=item bag_field

Name of field in Solr we can use to split the core into 'bags'.

Default: C<_bag>

This Solr field is mapped to C<_bag> when retrieved

=item on_error

Action to take when records cannot be saved to Solr. Default: throw. Available: ignore.

=back

=head1 METHODS
=head2 new( url => $url )
=head2 new( url => $url, id_field => '_id', bag_field => '_bag' )
=head2 new( url => $url, bags => { data => { cql_mapping => \%mapping } } )

Creates a new Catmandu::Store::Solr store connected to a Solr core, specificied by $url.

The store supports CQL searches when a cql_mapping is provided. This hash
contains a translation of CQL fields into Solr searchable fields.

 # Example mapping
 $cql_mapping = {
      title => {
        op => {
          'any'   => 1 ,
          'all'   => 1 ,
          '='     => 1 ,
          '<>'    => 1 ,
          'exact' => {field => 'mytitle.exact' }
        } ,
        sort  => 1,
        field => 'mytitle',
        cb    => ['Biblio::Search', 'normalize_title']
      }
 }

The CQL mapping above will support for the 'title' field the CQL operators: any, all, =, <> and exact.

For all the operators the 'title' field will be mapping into the Solr field 'mytitle', except
for the 'exact' operator. In case of 'exact' we will search the field 'mytitle.exact'.

The CQL has an optional callback field 'cb' which contains a reference to subroutines to rewrite or
augment the search query. In this case, in the Biblio::Search package there is a normalize_title
subroutine which returns a string or an ARRAY of string with augmented title(s). E.g.

    package Biblio::Search;

    sub normalize_title {
       my ($self,$title) = @_;
       my $new_title =~ s{[^A-Z0-9]+}{}g;
       $new_title;
    }

    1;

=head2 transaction

When you issue $bag->commit, all changes made in the buffer are sent to solr, along with a commit.
So committing in Catmandu merely means flushing changes;-).

When you wrap your subroutine within 'transaction', this behaviour is disabled temporarily.
When you call 'die' within the subroutine, a rollback is sent to solr.

Remember that transactions happen at store level: after the transaction, all buffers of all bags are flushed to solr,
and a commit is issued in solr.

#record 'test' added
$bag->add({ _id => "test" });
#buffer flushed, and 'commit' sent to solr
$bag->commit();

$bag->store->transaction(sub{
    $bag->add({ _id => "test",title => "test" });
    #call to die: rollback sent to solr
    die("oops, didn't want to do that!");
});

#record is still { _id => "test" }

=head1 SEE ALSO

L<Catmandu::Store>, L<WebService::Solr>

=head1 AUTHOR

Nicolas Steenlant, C<< nicolas.steenlant at ugent.be >>

Patrick Hochstenbach, C<< patrick.hochstenbach at ugent.be >>

Nicolas Franck, C<< nicolas.franck at ugent.be >>

=head1 LICENSE AND COPYRIGHT

This program is free software; you can redistribute it and/or modify it
under the terms of either: the GNU General Public License as published
by the Free Software Foundation; or the Artistic License.

See http://dev.perl.org/licenses/ for more information.

=cut

1;
