package App::MysqlUtils;

# DATE
# VERSION

use 5.010001;
use strict;
use warnings;
use Log::Any::IfLOG '$log';

use Perinci::Object;

our %SPEC;

my %args_common = (
    host => {
        schema => 'str*', # XXX hostname
        default => 'localhost',
        tags => ['category:connection'],
    },
    port => {
        schema => ['int*', min=>1, max=>65535], # XXX port
        default => '3306',
        tags => ['category:connection'],
    },
    username => {
        schema => 'str*',
        tags => ['category:connection'],
    },
    password => {
        schema => 'str*',
        tags => ['category:connection'],
    },
);

my %args_database = (
    database => {
        schema => 'str*',
        req => 1,
        pos => 0,
        completion => \&_complete_database,
    },
);

$SPEC{':package'} = {
    v => 1.1,
    summary => 'CLI utilities related to MySQL',
};

sub _connect {
    my %args = @_;

    unless (defined $args{username} && defined $args{password}) {
        if (-f (my $path = "$ENV{HOME}/.my.cnf")) {
            require Config::IOD::Reader;
            my $iod = Config::IOD::Reader->new();
            my $hoh = $iod->read_file($path);
            $args{username} //= $hoh->{client}{user};
            $args{password} //= $hoh->{client}{password};
        }
    }

    require DBI;
    my $dbh = DBI->connect(
        "DBI:mysql:".
            join(";",
                 (defined $args{database} ? ("database=$args{database}") : ()),
                 (defined $args{host} ? ("host=$args{host}") : ()),
                 (defined $args{port} ? ("port=$args{port}") : ()),
             ),
        $args{username},
        $args{password},
        {RaiseError => $args{_raise_error} // 1},
    );
}

sub _complete_database {
    require Complete::Util;
    my %args = @_;

    # only run under pericmd
    my $cmdline = $args{cmdline} or return undef;
    my $r = $args{r};

    # force read config file, because by default it is turned off when in
    # completion
    $r->{read_config} = 1;
    my $res = $cmdline->parse_argv($r);

    my $dbh = _connect(%{ $res->[2] }, database=>undef) or return undef;

    my @dbs;
    my $sth = $dbh->prepare("SHOW DATABASES");
    $sth->execute;
    while (my @row = $sth->fetchrow_array) {
        push @dbs, $row[0];
    }
    Complete::Util::complete_array_elem(
        word  => $args{word},
        array => \@dbs,
    );
}

sub _complete_table {
    require Complete::Util;
    my %args = @_;

    # only run under pericmd
    my $cmdline = $args{cmdline} or return undef;
    my $r = $args{r};

    # force read config file, because by default it is turned off when in
    # completion
    $r->{read_config} = 1;
    my $res = $cmdline->parse_argv($r);

    my $dbh = _connect(%{ $res->[2] }) or return undef;

    my @names = $dbh->tables(undef, undef, undef, undef);
    my @tables;
    for (@names) {
        /\A`(.+)`\.`(.+)`\z/ or next;
        push @tables, $2;
    }
    Complete::Util::complete_array_elem(
        word  => $args{word},
        array => \@tables,
    );
}

$SPEC{mysql_drop_all_tables} = {
    v => 1.1,
    summary => 'Drop all tables in a MySQL database',
    description => <<'_',

For safety, the default is dry-run mode. To actually drop the tables, you must
supply `--no-dry-run` or DRY_RUN=0.

_
    args => {
        %args_common,
        %args_database,
    },
    features => {
        dry_run => {default=>1},
    },
};
sub mysql_drop_all_tables {
    my %args = @_;

    my $dbh = _connect(%args);

    my @names = $dbh->tables(undef, undef, undef, undef);

    my $res = envresmulti();
    for (@names) {
        if ($args{-dry_run}) {
            $log->infof("[DRY_RUN] Dropping table %s ...", $_);
            $res->add_result(304, "OK (dry-run)", {item_id=>$_});
        } else {
            $log->infof("Dropping table %s ...", $_);
            $dbh->do("DROP TABLE $_");
            $res->add_result(200, "OK", {item_id=>$_});
        }
    }
    $res->as_struct;
}

$SPEC{mysql_drop_tables} = {
    v => 1.1,
    summary => 'Drop tables in a MySQL database',
    description => <<'_',

For safety, the default is dry-run mode. To actually drop the tables, you must
supply `--no-dry-run` or DRY_RUN=0.

Examples:

    # Drop table T1, T2, T3 (dry-run mode)
    % mysql-drop-tables DB T1 T2 T3

    # Drop all tables with names matching /foo/ (dry-run mode)
    % mysql-drop-tables DB --table-pattern foo

    # Actually drop all tables with names matching /foo/, don't delete more than 5 tables
    % mysql-drop-tables DB --table-pattern foo --limit 5 --no-dry-run

_
    args => {
        %args_common,
        %args_database,
        tables => {
            'x.name.is_plural' => 1,
            'x.name.singular' => 'table',
            schema => ['array*', of=>'str*'],
            element_completion => \&_complete_table,
            pos => 1,
            greedy => 1,
        },
        table_pattern => {
            schema => 're*',
        },
        limit => {
            summary => "Don't delete more than this number of tables",
            schema => 'posint*',
        },
    },
    args_rels => {
        req_one => [qw/tables table_pattern/],
    },
    features => {
        dry_run => {default=>1},
    },
};
sub mysql_drop_tables {
    my %args = @_;

    my $dbh = _connect(%args);

    my @names = $dbh->tables(undef, undef, undef, undef);

    my $res = envresmulti();
    my $n = 0;
  TABLE:
    for my $name (@names) {
        my ($schema, $table) = $name =~ /\A`(.+)`\.`(.+)`\z/
            or die "Invalid table name returned by \$dbh->tables() ($name), expecting `schema`.`table`";

        if ($args{tables}) {
            my $found;
            for (@{ $args{tables} }) {
                if ($_ eq $table) {
                    $found++; last;
                }
            }
            next TABLE unless $found;
        }
        if ($args{table_pattern}) {
            next TABLE unless $table =~ /$args{table_pattern}/;
        }
        $n++;
        if (defined $args{limit} && $n > $args{limit}) {
            last;
        }

        if ($args{-dry_run}) {
            $log->infof("[DRY_RUN] Dropping table %s ...", $name);
            $res->add_result(304, "OK (dry-run)", {item_id=>$name});
        } else {
            $log->infof("Dropping table %s ...", $name);
            $dbh->do("DROP TABLE $name");
            $res->add_result(200, "OK", {item_id=>$name});
        }
    }
    $res->as_struct;
}

1;
#ABSTRACT:

=head1 SYNOPSIS

This distribution includes the following CLI utilities:

#INSERT_EXECS_LIST


=head1 SEE ALSO
