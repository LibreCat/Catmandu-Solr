# NAME

Catmandu::Store::Solr - A searchable store backed by Solr

# SYNOPSIS

    use Catmandu::Store::Solr;

    my $store = Catmandu::Store::Solr->new(url => 'http://localhost:8983/solr' );

    my $obj1 = $store->bag->add({ name => 'Patrick' });

    printf "obj1 stored as %s\n" , $obj1->{_id};

    # Force an id in the store
    my $obj2 = $store->bag->add({ _id => 'test123' , name => 'Nicolas' });

    # Commit all changes
    $store->bag->commit;

    my $obj3 = $store->bag->get('test123');

    $store->bag->delete('test123');

    $store->bag->delete_all;

    # All bags are iterators
    $store->bag->each(sub { ... });
    $store->bag->take(10)->each(sub { ... });

    # Some stores can be searched
    my $hits = $store->bag->search(query => 'name:Patrick');

# SUPPORT

Solr schemas need to support '\_id' and '\_bag' record fields to be able to
store Catmandu items.

# METHODS

## new(url => $solr\_url)

Create a new Catmandu::Store::Solr store.

# SEE ALSO

[Catmandu::Store](https://metacpan.org/pod/Catmandu::Store)

# AUTHOR

Nicolas Franck, `<nicolas.franck at ugent.be>`
Nicolas Steenlant, `<nicolas.steenlant at ugent.be>`
Patrick Hochstenbach, `<patrick.hochstenbach at ugent.be>`

# LICENSE AND COPYRIGHT

This program is free software; you can redistribute it and/or modify it
under the terms of either: the GNU General Public License as published
by the Free Software Foundation; or the Artistic License.

See http://dev.perl.org/licenses/ for more information.
