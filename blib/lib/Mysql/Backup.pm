package Mysql::Backup;

use strict;
#use Carp(croak);
use DBI;
our $VERSION = '0.01';

sub new{   #standart constructor
    my ($pkg, $dbname, $dbhost, $dbuser, $dbpass, %param) = @_;

    my $self           = {};
    my $dbh            = DBI->connect("DBI:mysql:$dbname:$dbhost", $dbuser, $dbpass, {RaiseError=>1});
    $self->{'DBH_OBJ'} = $dbh;
    $self->{'param'}   = {};

    foreach my $key(keys %param){
      $self->{'param'}->{$key} = $param{$key};
    }

    bless($self, $pkg);
    return $self;
}

sub new_from_DBH{   #if you have already DBI connection, you can use this
    my ($pkg, $dbh, %param) = @_;

    my $self           = {};
    $self->{'DBH_OBJ'} = $dbh;
    $self->{'param'}   = {};

    foreach my $key(keys %param){
      $self->{'param'}->{$key} = $param{$key};
    }

    bless($self, $pkg);
    return $self;
}



sub arr_hash($){
    my ($self, $sql) = @_;
    my @res;
    if (my $sth = $self->run_sql($sql)){
      while (my $ref = $sth->fetchrow_hashref){
        push @res, $ref;
      }
    }
    return @res;
}


sub run_sql($){
    my ($self, $sql) = @_;
    my $dbh = $self->{'DBH_OBJ'};
    my $sth = $dbh->prepare($sql);

    if (not $sth){
      die $DBI::errstr;
    }

    my $res = $sth->execute;
    if (not $res){
      return undef;
    }
    return $sth;
}

sub table_desc($){ #creates a structure of the inputed table

    my ($self, $table) = @_;
    my @temp = $self->arr_hash("SHOW COLUMNS FROM $table");
    my @temp2;

    foreach my $ref(@temp){
      my $null = 'NOT NULL' if ($ref->{'Null'} !~ m/YES/i);
      my $default;
      if($ref->{'default'}){
        $default .= $null.' default '."'".$ref->{'Default'}."'";
      }
      else{
        if ($ref->{'Null'} =~ m/YES/i){
	  $default .= 'default '.'NULL';
        }
        else{
          $default .= $null;
	}
      }
      chomp $default;
      push @temp2, join(' ', $ref->{'Field'}, $ref->{'Type'}, $default.($ref->{'Extra'}?' '.$ref->{'Extra'}:''));
    }

    my $columns = join(', ', @temp2);

    @temp = $self->arr_hash("SHOW KEYS FROM $table");
    foreach my $ref(@temp){
      if ($ref->{'Key_name'} =~ m/PRIMARY/i){
        $columns .= ", PRIMARY KEY (".$ref->{'Column_name'}.")";
      }
      else{
        $columns .= ", KEY ".$ref->{'Key_name'}." (".$ref->{'Column_name'}.")";
      }
    }
    my $sql = "CREATE TABLE $table ($columns);";

    return $sql;
}

sub create_structure{ #creates a structure of the current DB

    my $self = shift;
    my $sth = $self->run_sql("SHOW TABLES");
    my @arr;
    my $sql;
    while(my @temp = $sth->fetchrow_array()){
      push @arr, $temp[0];
    }

    foreach my $temp(@arr){
      $sql .= $self->table_desc($temp)."\n";
    }

    return $sql;
}

sub get_table_data{

    my ($self, $table) = @_;
    my $data;
    my @temp = $self->arr_hash("SELECT * FROM $table WHERE 1");

    foreach my $ref (@temp){
      my @keys = keys %$ref;
      my $key_list = join(', ', @keys);
      my @values;
      for(my $i=0; $i<=$#keys; $i++){
        push @values, $self->{'DBH_OBJ'}->quote($ref->{$keys[$i]});
      }
      my $value_list = join(', ', @values);
      $data .= "INSERT INTO $table ($key_list) VALUES ($value_list);\n";
    }

    return $data;
}

sub data_backup{ #get all data from current database

    my $self = shift;
    my $sth = $self->run_sql("SHOW TABLES");
    my (@tables, @tables_for_lock);
    while(my $temp = $sth->fetchrow_array()){
      push @tables, "$temp";
      push @tables_for_lock, "$temp WRITE";
    }
    #$self->run_sql("LOCK TABLES ".join(', ', @tables_for_lock));
    my @arr;
    my $sql = '';
    foreach my $temp(@tables){
      $sql .= $self->get_table_data($temp);
    }

    #$self->run_sql("UNLOCK TABLES");
    return $sql;
}

sub run_restore_script($){

    my ($self, $file) = @_;
    my $sth = $self->run_sql("SHOW TABLES");
    my $dbh = $self->{'DBH_OBJ'};
    my (@tables, @tables_for_lock);
    while(my $temp = $sth->fetchrow_array()){
      push @tables, "$temp";
      push @tables_for_lock, "$temp WRITE";
    }
    #$self->run_sql("LOCK TABLES ".join(', ', @tables_for_lock));
       #$sth = run_sql("FLUSH TABLES");
    foreach my $temp(@tables){
      $dbh->do("DROP TABLE IF EXISTS $temp");
    }

    open(FILE, $file);
    readline(FILE);
    $/ = ";\n";
    my @sql = <FILE>;
    $/= "\n";
    close(FILE);

    foreach my $sql(@sql){
      chomp $sql;
      $self->run_sql($sql);
    }

    #$dbh->do("RESET MASTER");
    #$self->run_sql("UNLOCK TABLES");
    return \@sql;
}

1;

__END__


=head1 NAME

Mysql::Backup - Perl extension for making backups of mysql DBs.

=head1 SYNOPSIS

  use Mysql::Backup;
  my $mb = new Mysql::Backup('perldesk','127.0.0.1','','');
  print $mb->data_backup;

=head1 DESCRIPTION

Mysql::Backup should be useful for people, who needed in backuping mysql DBs by perl script
and doesn't want to use mysqldump or doesn't able to do this.

=head2 EXPORT

None by default.



=head1 SEE ALSO

Mention other useful documentation such as the documentation of
related modules or operating system documentation (such as man pages
in UNIX), or any relevant external documentation such as RFCs or
standards.

If you have a mailing list set up for your module, mention it here.

If you have a web site set up for your module, mention it here.

=head1 AUTHOR

Dmitry Nikolayev<lt>dmitry@cpan.org<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2004 by Dmitry Nikolayev

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself, either Perl version 5.8.2 or,
at your option, any later version of Perl 5 you may have available.


=cut