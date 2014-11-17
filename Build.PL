use strict;
use warnings;
use Module::Build;

my $builder = Module::Build->new(
    module_name => 'Catmandu::Store::Solr',
    license => 'perl',
    dist_author => [
        'Patrick Hochstenbach <patrick.hochstenbach@ugent.be>',
        'Nicolas Steenlant <nicolas.steenlant@ugent.be>',
        'Nicolas Franck <nicolas.franck@ugent.be>'
    ],
    build_requires => {
        'Software::License' => 0,
        'Test::Exception' => 0,
        'Test::More' => 0,
    },
    requires => {
        'perl' => '5.10.1',
        'Catmandu' => '0.7',
        'CQL::Parser' => '1.12',
        'Moo' => '1.00',
        'WebService::Solr' => '0.16',
    },
    add_to_cleanup => [qw(
        Catmandu-Store-Solr-*
    )],
    create_makefile_pl => 'traditional',
    create_license => 1,
);

$builder->create_build_script;
