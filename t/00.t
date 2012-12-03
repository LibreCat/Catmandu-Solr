#!/usr/bin/env perl

use strict;
use warnings;
use Test::More;

my @pkgs = qw(
    Catmandu::Store::Solr
    Catmandu::Store::Solr::Bag
    Catmandu::Store::Solr::Searcher
    Catmandu::Store::Solr::CQL
);

require_ok $_ for @pkgs;

done_testing 4;
