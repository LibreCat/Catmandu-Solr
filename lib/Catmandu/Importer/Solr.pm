package Catmandu::Importer::Solr;

use Catmandu::Sane;
use Catmandu::Store::Solr;
use Catmandu;
use Moo;

our $VERSION = '0.01';

with 'Catmandu::Importer';

has fq => (is => 'ro');
has query    => (is => 'ro');
has url => ( is => 'ro' );
has bag => ( is => 'ro' );
has _bag  => (
    is       => 'ro',
    lazy     => 1,
    builder  => '_build_bag',
);

sub _build_bag {
    my $self = $_[0];
    Catmandu::Store::Solr->new( url => $self->url )->bag($self->bag());
}

sub generator {
	my ($self) = @_;

	return sub {
        state $start = 0;
        state $limit = 100;
        state $total = 100;
        state $hits = [];

        return if $start >= $total;

        unless(scalar(@$hits)){

            my $res = $self->_bag()->search(
                query => $self->query,
                fq => $self->fq,
                start => $start,
                limit => $limit,
                facet => "false",
                spellcheck => "false"
            );
            $total = $res->total;
            $hits = $res->hits();

            $start += $limit;

        }

        shift(@$hits);

	}
}

#TODO: count
sub count {
    my ( $self ) = @_;
    $self->bag()->search( query => $self->query, fq => $self->fq, limit => 0 )->total();
}


=head1 NAME

Catmandu::Importer::DBI - Catmandu module to import data from any DBI source

=head1 SYNOPSIS

 use Catmandu::Importer::DBI;

 my %attrs = (
        dsn => 'dbi:mysql:foobar' ,
        user => 'foo' ,
        password => 'bar' ,
        query => 'select * from table'
 );

 my $importer = Catmandu::Importer::DBI->new(%attrs);

 # Optional set extra parameters on the database handle
 # $importer->dbh->{LongReadLen} = 1024 * 64;

 $importer->each(sub {
	my $row_hash = shift;
	...
 });


 # or

 $ catmandu convert DBI --dsn dbi:mysql:foobar --user foo --password bar --query "select * from table"

=head1 AUTHORS

 Patrick Hochstenbach, C<< <patrick.hochstenbach at ugent.be> >>

=head1 SEE ALSO

L<Catmandu>, L<Catmandu::Importer> , L<Catmandu::Store::DBI>

=cut

1;
