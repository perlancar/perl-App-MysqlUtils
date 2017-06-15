package App::MysqlUtils;

# DATE
# VERSION

use 5.010001;
use strict;
use warnings;
use Log::Any::IfLOG '$log';

use IPC::System::Options qw(system);
use Perinci::Object;
use String::ShellQuote;

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
        description => <<'_',

Will try to get default from `~/.my.cnf`.

_
        tags => ['category:connection'],
    },
    password => {
        schema => 'str*',
        description => <<'_',

Will try to get default from `~/.my.cnf`.

_
        tags => ['category:connection'],
    },
);

my %args_database0 = (
    database => {
        schema => 'str*',
        req => 1,
        pos => 0,
        completion => \&_complete_database,
    },
);

my %args_database = (
    database => {
        schema => 'str*',
        req => 1,
        cmdline_aliases => { db=>{} },
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
        %args_database0,
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
        %args_database0,
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

$SPEC{mysql_query} = {
    v => 1.1,
    summary => 'Run query and return table result',
    description => <<'_',

This is like just regular querying, but the result will be returned as table
data (formattable using different backends). Or, you can output as JSON.

Examples:

    # by default, show as pretty text table, like in interactive mysql client
    % mysql-query DBNAME "SELECT * FROM t1"

    # show as JSON (array of hashes)
    % mysql-query DBNAME "QUERY..." --json ;# or, --format json

    # show as CSV
    % mysql-query DBNAME "QUERY..." --format csv

    # show as CSV table using Text::Table::CSV
    % FORMAT_PRETTY_TABLE_BACKEND=Text::Table::Org mysql-query DBNAME "QUERY..."

_
    args => {
        %args_common,
        %args_database0,
        query => {
            schema => 'str*',
            req => 1,
            pos => 0,
            cmdline_src => 'stdin_or_args',
        },
        add_row_numbers => {
            summary => 'Add first field containing number from 1, 2, ...',
            schema => ['bool*', is=>1],
        },
    },
};
sub mysql_query {
    my %args = @_;

    my $dbh = _connect(%args);

    my $sth = $dbh->prepare($args{query});
    $sth->execute;

    my @columns = @{ $sth->{NAME_lc} };
    if ($args{add_row_numbers}) {
        unshift @columns, "_row"; # XXX what if columns contains '_row' already, we need to supply a unique name e.g. '_row2', ...
    };
    my @rows;
    my $i = 0;
    while (my $row = $sth->fetchrow_hashref) {
        $i++;
        $row->{_row} = $i if $args{add_row_numbers};
        push @rows, $row;
    }

    [200, "OK", \@rows, {'table.fields'=>\@columns}];
}

$SPEC{mysql_split_sql_dump_per_table} = {
    v => 1.1,
    summary => 'Parse SQL dump and spit out tables to separate files',
    args => {
        # XXX include_table
        # XXX include_table_pattern
        # XXX exclude_table
        # XXX exclude_table_pattern
        stop_after_table => {
            schema => 'str*',
        },
        stop_after_table_pattern => {
            schema => 're*',
        },
        # XXX output_file_pattern
        # XXX overwrite
    },
};
sub mysql_split_sql_dump_per_table {
    my %args = @_;

    my ($prevtbl, $curtbl, $pertblfile, $pertblfh);

    # we use direct <>, instead of cmdline_src for speed
    while (<>) {
        if (/^(?:CREATE TABLE) `(.+)`/) {
            $prevtbl = $curtbl;
            if (defined $prevtbl && $args{stop_after_table} && $prevtbl eq $args{stop_after_table}) {
                last;
            } elsif (defined $prevtbl && $args{stop_after_table_pattern} && $prevtbl =~ $args{stop_after_table_pattern}) {
                last;
            }
            $curtbl = $1;
            $pertblfile = "$curtbl";
            if (defined $prevtbl) {
                close $pertblfh;
                #say "Finished writing $pertblfile";
            }
            warn "Writing $pertblfile ...\n";
            open $pertblfh, ">", $pertblfile or die "Can't open $pertblfile: $!";
        }
        next unless $curtbl;
        print $pertblfh $_;
    }

    [200, "OK"];
}

$SPEC{mysql_run_sql_files} = {
    v => 1.1,
    summary => 'Feed each .sql file to `mysql` command and '.
        'write result to .txt file',
    args => {
        sql_files => {
            schema => ['array*', of=>'filename*'],
            req => 1,
            pos => 0,
            greedy => 1,
        },
        %args_database,
        # XXX output_file_pattern
        overwrite_when => {
            summary => 'Specify when to overwrite existing .txt file',
            schema => ['str*', in=>[qw/none older always/]],
            default => 'none',
            description => <<'_',

`none` means to never overwrite existing .txt file. `older` overwrites existing
.txt file if it's older than the corresponding .sql file. `always` means to
always overwrite existing .txt file.

_
            cmdline_aliases => {
                o         => {summary=>'Shortcut for --overwrite_when=older' , is_flag=>1, code=>sub {$_[0]{overwrite_when} = 'older' }},
                O         => {summary=>'Shortcut for --overwrite_when=always', is_flag=>1, code=>sub {$_[0]{overwrite_when} = 'always'}},
            },
        },
    },
    deps => {
        prog => 'mysql',
    },
};
sub mysql_run_sql_files {
    my %args = @_;

    my $ov_when = $args{overwrite_when} // 'none';

    for my $sqlfile (@{ $args{sql_files} }) {

        my $txtfile = $sqlfile;
        $txtfile =~ s/\.sql$/.txt/i;
        if ($sqlfile eq $txtfile) { $txtfile .= ".txt" }

        if (-f $txtfile) {
            if ($ov_when eq 'always') {
                $log->debugf("Overwriting existing %s ...", $txtfile);
            } elsif ($ov_when eq 'older') {
                if ((-M $txtfile) > (-M $sqlfile)) {
                    $log->debugf("Overwriting existing %s because it is older than the corresponding %s ...", $txtfile, $sqlfile);
                } else {
                    $log->infof("%s already exists and newer than corresponding %s, skipped", $txtfile, $sqlfile);
                    next;
                }
            } else {
                $log->infof("%s already exists, we never overwrite existing .txt file, skipped", $txtfile);
                next;
            }
        }

        $log->infof("Running SQL file '%s' and putting result to '%s' ...",
                    $sqlfile, $txtfile);
        my $cmd = join(
            " ",
            "mysql",
            shell_quote($args{database}),
            "<", shell_quote($sqlfile),
            ">", shell_quote($txtfile),
        );
        system({log=>1}, $cmd);
    }

    [200, "OK"];
}

1;
#ABSTRACT:

=head1 SYNOPSIS

This distribution includes the following CLI utilities:

#INSERT_EXECS_LIST


=head1 SEE ALSO
