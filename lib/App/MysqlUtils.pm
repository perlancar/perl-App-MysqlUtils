package App::MysqlUtils;

## no critic (InputOutput::RequireBriefOpen)

# DATE
# VERSION

use 5.010001;
use strict;
use warnings;
use Log::ger;

use IPC::System::Options qw(system);
use List::MoreUtils qw(firstidx);
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

my %args_overwrite_when = (
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
);

my %args_output = (
    directory => {
        summary => 'Specify directory for the resulting *.txt files',
        schema => 'dirname*',
        default => '.',
        cmdline_aliases => {
            d => {},
        },
    },
    mkdir => {
        summary => 'Create output directory if not exists',
        schema => ['true*'],
        default => 1,
        cmdline_aliases => {
            p => {},
        },
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
            log_info("[DRY_RUN] Dropping table %s ...", $_);
            $res->add_result(304, "OK (dry-run)", {item_id=>$_});
        } else {
            log_info("Dropping table %s ...", $_);
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
            log_info("[DRY_RUN] Dropping table %s ...", $name);
            $res->add_result(304, "OK (dry-run)", {item_id=>$name});
        } else {
            log_info("Dropping table %s ...", $name);
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

$SPEC{mysql_sql_dump_extract_tables} = {
    v => 1.1,
    summary => 'Parse SQL dump and spit out tables to separate files',
    args => {
        include_tables => {
            'x.name.is_plural' => 1,
            'x.name.singular' => 'include_table',
            schema => ['array*', of=>'str*'],
            tags => ['category:filtering'],
            cmdline_aliases => {I=>{}},
        },
        exclude_tables => {
            'x.name.is_plural' => 1,
            'x.name.singular' => 'exclude_table',
            schema => ['array*', of=>'str*'],
            tags => ['category:filtering'],
            cmdline_aliases => {X=>{}},
        },
        include_table_patterns => {
            'x.name.is_plural' => 1,
            'x.name.singular' => 'include_table_pattern',
            schema => ['array*', of=>'re*'],
            tags => ['category:filtering'],
            cmdline_aliases => {pat=>{}},
        },
        exclude_table_patterns => {
            'x.name.is_plural' => 1,
            'x.name.singular' => 'exclude_table_pattern',
            schema => ['array*', of=>'re*'],
            tags => ['category:filtering'],
            cmdline_aliases => {xpat=>{}},
        },
        stop_after_table => {
            schema => 'str*',
        },
        stop_after_table_pattern => {
            schema => 're*',
        },
        overwrite => {
            schema => ['bool*', is=>1],
            cmdline_aliases => {O=>{}},
            tags => ['category:output'],
        },
        dir => {
            summary => 'Directory to put the SQL files into',
            schema => 'dirname*',
            tags => ['category:output'],
        },
        # XXX output_file_pattern
    },
};
sub mysql_sql_dump_extract_tables {
    my %args = @_;

    my $stop_after_tbl  = $args{stop_after_table};
    my $stop_after_tpat = $args{stop_after_table_pattern};
    my $inc_tbl  = $args{include_tables};
    $inc_tbl  = undef unless $inc_tbl  && @$inc_tbl;
    my $inc_tpat = $args{include_table_patterns};
    $inc_tpat = undef unless $inc_tpat && @$inc_tpat;
    my $exc_tbl  = $args{exclude_tables};
    $exc_tbl  = undef unless $exc_tbl  && @$exc_tbl;
    my $exc_tpat = $args{exclude_table_patterns};
    $exc_tpat = undef unless $exc_tpat && @$exc_tpat;
    my $has_tbl_filters = $inc_tbl || $inc_tpat || $exc_tbl || $exc_tpat;

    my ($prevtbl, $curtbl, $pertblfile, $pertblfh);

    my $code_tbl_is_included = sub {
        my $tbl = shift;
        return 0 if $exc_tbl  && (grep { $tbl eq $_ } @$exc_tbl );
        return 0 if $exc_tpat && (grep { $tbl =~ $_ } @$exc_tpat);
        return 1 if $inc_tbl  && (grep { $tbl eq $_ } @$inc_tbl );
        return 1 if $inc_tpat && (grep { $tbl =~ $_ } @$inc_tpat);
        if ($inc_tbl || $inc_tpat) { return 0 } else { return 1 }
    };

    if (defined $args{dir}) {
        unless (-d $args{dir}) {
            log_info "Creating directory '%s' ...", $args{dir};
            mkdir $args{dir}, 0755 or return [500, "Can't create directory '$args{dir}': $!"];
        }
    }

    # we use direct <>, instead of cmdline_src for speed
    my %seentables;
    while (<>) {
        if (/^(?:-- Table structure for table|-- Dumping data for table|CREATE TABLE IF NOT EXISTS|CREATE TABLE|DROP TABLE IF EXISTS) `(.+)`/) {
            goto L1 if $seentables{$1}++;
            $prevtbl = $curtbl;
            if (defined $prevtbl && $args{stop_after_table} && $prevtbl eq $args{stop_after_table}) {
                last;
            } elsif (defined $prevtbl && $args{stop_after_table_pattern} && $prevtbl =~ $args{stop_after_table_pattern}) {
                last;
            }
            $curtbl = $1;
            $pertblfile = (defined $args{dir} ? "$args{dir}/" : "") . "$curtbl";
            if ($has_tbl_filters && !$code_tbl_is_included->($curtbl)) {
                log_warn "SKIPPING table $curtbl because it is not included";
                undef $pertblfh;
            } elsif ((-e $pertblfile) && !$args{overwrite}) {
                log_warn "SKIPPING table $curtbl because file $pertblfile already exists";
                undef $pertblfh;
            } else {
                log_warn "Writing $pertblfile ...";
                open $pertblfh, ">", $pertblfile or die "Can't open $pertblfile: $!";
            }
        }
      L1:
        next unless $curtbl && $pertblfh;
        print $pertblfh $_;
    }
    close $pertblfh if defined $pertblfh;

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
        %args_overwrite_when,
        %args_output,
    },
    deps => {
        prog => 'mysql',
    },
};
sub mysql_run_sql_files {
    my %args = @_;

    my $dir = $args{directory} // '.';
    my $mkdir = $args{mkdir} // 1;
    if (!(-d $dir) && $mkdir) {
        require File::Path;
        File::Path::make_path($dir);
    }
    my $ov_when = $args{overwrite_when} // 'none';

    for my $sqlfile (@{ $args{sql_files} }) {

        my $txtfile = "$dir/$sqlfile";
        $txtfile =~ s/\.sql$/.txt/i;
        if ($sqlfile eq $txtfile) { $txtfile .= ".txt" }

        if (-f $txtfile) {
            if ($ov_when eq 'always') {
                log_debug("Overwriting existing %s ...", $txtfile);
            } elsif ($ov_when eq 'older') {
                if ((-M $txtfile) > (-M $sqlfile)) {
                    log_debug("Overwriting existing %s because it is older than the corresponding %s ...", $txtfile, $sqlfile);
                } else {
                    log_info("%s already exists and newer than corresponding %s, skipped", $txtfile, $sqlfile);
                    next;
                }
            } else {
                log_info("%s already exists, we never overwrite existing .txt file, skipped", $txtfile);
                next;
            }
        }

        log_info("Running SQL file '%s' and putting result to '%s' ...",
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

$SPEC{mysql_run_pl_files} = {
    v => 1.1,
    summary => 'Run each .pl file, feed the output to `mysql` command and '.
        'write result to .txt file',
    description => <<'_',

The `.pl` file is supposed to produce a SQL statement. For simpler cases, use
<prog:mysql-run-sql-files>.

_
    args => {
        pl_files => {
            schema => ['array*', of=>'filename*'],
            req => 1,
            pos => 0,
            greedy => 1,
        },
        %args_database,
        # XXX output_file_pattern
        %args_overwrite_when,
        %args_output,
    },
    deps => {
        prog => 'mysql',
    },
};
sub mysql_run_pl_files {
    my %args = @_;

    my $dir = $args{directory} // '.';
    my $mkdir = $args{mkdir} // 1;
    if (!(-d $dir) && $mkdir) {
        require File::Path;
        File::Path::make_path($dir);
    }
    my $ov_when = $args{overwrite_when} // 'none';

    for my $plfile (@{ $args{pl_files} }) {

        my $txtfile = "$dir/$plfile";
        $txtfile =~ s/\.pl$/.txt/i;
        if ($plfile eq $txtfile) { $txtfile .= ".txt" }

        if (-f $txtfile) {
            if ($ov_when eq 'always') {
                log_debug("Overwriting existing %s ...", $txtfile);
            } elsif ($ov_when eq 'older') {
                if ((-M $txtfile) > (-M $plfile)) {
                    log_debug("Overwriting existing %s because it is older than the corresponding %s ...", $txtfile, $plfile);
                } else {
                    log_info("%s already exists and newer than corresponding %s, skipped", $txtfile, $plfile);
                    next;
                }
            } else {
                log_info("%s already exists, we never overwrite existing .txt file, skipped", $txtfile);
                next;
            }
        }

        log_info("Running .pl file '%s' and putting result to '%s' ...",
                    $plfile, $txtfile);
        my $cmd = join(
            " ",
            "perl", shell_quote($plfile),
            "|",
            "mysql",
            shell_quote($args{database}),
            ">", shell_quote($txtfile),
        );
        system({log=>1}, $cmd);
    }

    [200, "OK"];
}

$SPEC{mysql_copy_rows_adjust_pk} = {
    v => 1.1,
    summary => 'Copy rows from one table to another, adjust PK column if necessary',
    description => <<'_',

This utility can be used when you have rows in one table that you want to insert
to another table, but the PK might clash. When that happens, the value of the
other columns are inspected. When all the values of the other columns match, the
row is assumed to be a duplicate and skipped. If some values of the other column
differ, then the row is assumed to be different and a new value of the PK column
is chosen (there are several choices on how to select the new PK).

An example:

    % mysql-copy-rows-adjust-pk db1 --from t1 --to t2 --pk-column id --adjust "add 1000"

Suppose these are the rows in table `t1`:

    id    date                 description        user
    --    ----                 -----------        ----
     1    2018-12-03 12:01:01  Created user u1    admin1
     2    2018-12-03 12:44:33  Removed user u1    admin1

And here are the rows in table `t2`:

    id    date                 description        user
    --    ----                 -----------        ----
     1    2018-12-03 12:01:01  Created user u1    admin1
     2    2018-12-03 13:00:45  Rebooted machine1  admin1
     3    2018-12-03 13:05:00  Created user u2    admin2

You can see that row id=1 in both tables are identical. This will be skipped. On
the other hand, row id=2 in `t1` is different with row id=2 in `t2`. This row
will be adjusted: `id` will be changed to 2+1000=1002. So the final rows in
table `t2` will be (sorted by date):

    id    date                 description        user
    --    ----                 -----------        ----
     1    2018-12-03 12:01:01  Created user u1    admin1
     1002 2018-12-03 12:44:33  Removed user u1    admin1
     2    2018-12-03 13:00:45  Rebooted machine1  admin1
     3    2018-12-03 13:05:00  Created user u2    admin2

So basically this utility is similar to MySQL's INSERT ... ON DUPLICATE KEY
UPDATE, but will avoid inserting identical rows.

If the adjusted PK column clashes with another row in the target table, the row
is skipped.

_
    args => {
        %args_common,
        %args_database0,
        from => {
            summary => 'Name of source table',
            schema => 'str*',
            req => 1,
        },
        to => {
            summary => 'Name of target table',
            schema => 'str*',
            req => 1,
        },
        pk_column => {
            summary => 'Name of PK column',
            schema => 'str*',
            req => 1,
        },
        adjust => {
            summary => 'How to adjust the value of the PK column',
            schema => ['str*', match => qr/\A(add|subtract) \d+\z/],
            req => 1,
            description => <<'_',

Currently the choices are:

* "add N" add N to the original value.
* "subtract N" subtract N from the original value.

_
        },
    },
    features => {
        dry_run => 1,
    },
};
sub mysql_copy_rows_adjust_pk {
    require Data::Cmp;
    require DBIx::Diff::Schema;

    my %args = @_;

    my $dbh = _connect(%args);

    my @cols = map { $_->{COLUMN_NAME} }
        DBIx::Diff::Schema::list_columns($dbh, $args{from});
    my $pkidx = firstidx {$_ eq $args{pk_column}} @cols;
    $pkidx >= 0 or return [412, "PK column '$args{pk_column}' does not exist"];

    my $diff = DBIx::Diff::Schema::diff_table_schema(
        $dbh, $dbh, $args{from}, $args{to},
    );
    if ($diff->{deleted_columns} && @{ $diff->{deleted_columns} } ||
            $diff->{added_columns} && @{ $diff->{added_columns} } ||
            $diff->{modified_columns} && keys %{ $diff->{modified_columns} }) {
        return [412, "Structure of tables are different"];
    }

    my %from_row_ids;
  GET_SOURCE_ROW_IDS: {
        my $sth = $dbh->prepare("SELECT `$args{pk_column}` FROM `$args{from}`");
        $sth->execute;
        while (my @row = $sth->fetchrow_array) {
            $from_row_ids{$row[0]}++;
        }
    }

    my %to_row_ids;
  GET_TARGET_ROW_IDS: {
        my $sth = $dbh->prepare("SELECT `$args{pk_column}` FROM `$args{to}`");
        $sth->execute;
        while (my @row = $sth->fetchrow_array) {
            $to_row_ids{$row[0]}++;
        }
    }

    my $num_inserted = 0;
    my $num_skipped  = 0;
    my $num_adjusted = 0;
  INSERT: {
        my $sth_select_source = $dbh->prepare(
            "SELECT ".join(",", map {"`$_`"} @cols).
                " FROM `$args{from}` WHERE `$args{pk_column}`=?"
            );
        my $sth_select_target = $dbh->prepare(
            "SELECT ".join(",", map {"`$_`"} @cols).
                " FROM `$args{to}` WHERE `$args{pk_column}`=?"
            );
        my $sth_insert = $dbh->prepare(
            "INSERT INTO `$args{to}` (".join(",", map {"`$_`"} @cols).
                ") VALUES (".join(",", map {"?"} @cols).")"
        );

        for my $id (keys %from_row_ids) {
            $sth_select_source->execute($id);
            my @row = $sth_select_source->fetchrow_array;
            if ($to_row_ids{$id}) {
                # clashes
                $sth_select_target->execute($id);
                my @rowt = $sth_select_target->fetchrow_array;

                if (Data::Cmp::cmp_data(\@row, \@rowt) == 0) {
                    # identical, skip
                    $num_skipped++;
                    next;
                } else {
                    # not identical, adjust PK
                    if ($args{adjust} =~ /\Aadd (\d+)\z/) {
                        $row[$pkidx] += $1;
                    } elsif ($args{adjust} =~ /\Asubtract (\d+)\z/) {
                        $row[$pkidx] -= $1;
                    }

                    # does adjusted PK clash with an existing row? if yes, skip
                    if ($to_row_ids{ $row[$pkidx] }) {
                        $num_skipped++;
                        next;
                    }

                    $num_adjusted++;
                }
            }

            if ($args{-dry_run}) {
                log_trace "[DRY-RUN] Inserting %s", \@row;
            } else {
                $sth_insert->execute(@row);
            }
            $num_inserted++;
        }
    }

    [200, "OK", undef, {
        "func.num_inserted" => $num_inserted,
        "func.num_skipped"  => $num_skipped,
        "func.num_adjusted" => $num_adjusted,
    }];
}

1;
#ABSTRACT:

=head1 SYNOPSIS

This distribution includes the following CLI utilities:

#INSERT_EXECS_LIST


=head1 SEE ALSO
