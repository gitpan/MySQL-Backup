
use ExtUtils::MakeMaker;

WriteMakefile(
    NAME              => 'Mysql::Backup',
    VERSION_FROM      => 'lib/Mysql/Backup.pm',
    PREREQ_PM         => {'DBI' => 0,
                          'DBD::mysql' => 0},
    ($] >= 5.005 ?
      (ABSTRACT_FROM  => 'lib/Mysql/Backup.pm',
       AUTHOR         => 'Dmitry Nikolayev <dmitry@cpan.org>') : ()),
);
