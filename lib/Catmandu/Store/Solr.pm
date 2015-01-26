package Catmandu::Store::Solr;

use Catmandu::Sane;
use Moo;
use WebService::Solr;
use Catmandu::Store::Solr::Bag;

with 'Catmandu::Store';
#with 'Catmandu::Transactionable';

=head1 NAME

Catmandu::Store::Solr - A searchable store backed by Solr

=cut

our $VERSION = '0.0208';

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
        $self->{_tx} = 1;
        @res = $sub->();
        $solr->commit;
        $self->{_tx} = 0;
        1;
    } or do {
        my $err = $@;
        eval { $solr->rollback };
        $self->{_tx} = 0;
        die $err;
    };

    @res;
}

=head1 SUPPORT

Solr schemas need to support an identifier field (C<_id> by default) and a bag
field (C<_bag> by default) to be able to store Catmandu items.

=head1 CONFIGURATION

=over

=item url

Solr URL (C<http://localhost:8983/solr> by default)

=item id_field

Field that C<_id> is mapped to in Solr

=item bag_field

Field that C<_bag> is mapped to in Solr

=back

=head1 METHODS

=head2 transaction

When you issue $bag->commit, all changes made in the buffer are sent to solr, along with a commit.
So committing in Catmandu merely means flushing changes;-).

When you wrap your subroutine within 'transaction', this behaviour is disabled temporarily.
You still need to call 'commit', but this only sents the documents to Solr without commit.
When you call 'die' within the subroutine, a rollback is sent to solr.

#record 'test' added
$bag->add({ _id => "test" });
#buffer flushed, and 'commit' sent to solr
$bag->commit();

$bag->store->transaction(sub{
    $bag->add({ _id => "test",title => "test" });
    $bag->commit;
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
