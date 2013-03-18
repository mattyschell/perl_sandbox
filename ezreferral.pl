#!/usr/bin/perl

# Matt! 02/09/09

use strict;
use warnings;

use DBI;

use FindBin qw($Bin);
use lib "$Bin/../lib";

use Production::RunCamps qw( get_dbh );
use Production::ReferralResolution qw(get_referral_paths build_production_paths do_referral_qc
                                      update_production_metadata stage_referral_files scp_staged_files 
                                      write_remote_script get_referral_parms trim );
                                      
=head1 NAME

  ezreferral.pl

  created: 02/09/2009
  last modified: 06/15/2009

=head1 SYNOPSIS

  The ezreferral script moves all files and metadata into production for an entity that has been run outside of production as a referral.  Dependent on ReferralResolution.pm

=cut

if (! defined($ARGV[0])) {
   print STDERR "\n";
   print STDERR "ezreferral.pl \n";
   print STDERR " \$0 = path to parameter file  \n";
   print STDERR "\n";
   exit 1;
   
   ##testing ex of the file:##
   #referral schema =SCHEL010
   #referral schema password =schel010#
   #production schema =CAMPS_CPMB4
   #production database link =NA
   #referral map id = BAS09C27115
   #production output directory (no ending slash) =/mt/branch/cpmb/camps/render/sysbnch/camps_cpmb4/matttest2
   #metadata note =resolved in referral schema
   #superuser (beard001 or mcint005) =schel010
   #superuser production node (ex node19) =node20
   #production map id id (NA if never run in production) =BAS09C27115
   #distiller style (PC, WATCH, or CMD) =CMD
   #junk directory =/home/schel010/
   #optional database name (default PRODBNCH) =sysbnch 
} 

my $referralparms = Production::ReferralResolution::get_referral_parms($ARGV[0]);

if ($referralparms->{'status'} eq 'fail') {
   print STDOUT "Error: ".$referralparms->{'message'};
   exit;
} else {
   print STDOUT " Successfully read input parameter file\n";
}

#Added get_referral_parms last
#Should use values in return hash from that function
#But setting equal to old versions of variables to save coding time
my $referral_schema   = $referralparms->{'referral_schema'};
my $database          = $referralparms->{'database'};
my $referral_pwd      = $referralparms->{'referral_pwd'};
my $database_link     = $referralparms->{'database_link'};
my $production_o      = $referralparms->{'production_o'};
my $referral_map_id   = $referralparms->{'referral_map_id'};
my $production_map_id = $referralparms->{'production_map_id'};
my $metadata_note     = $referralparms->{'metadata_note'};
my $distiller_style   = $referralparms->{'distiller_style'};
my $prod_schema       = $referralparms->{'prod_schema'};
my $superuser         = $referralparms->{'superuser'};
my $superusernode     = $referralparms->{'superusernode'};
my $junk_directory    = $referralparms->{'junk_directory'};


#Get database connection

print STDOUT " Logging into ".$referral_schema." on ".$database."\n";

my $connection = [$database,$referral_schema,$referral_pwd];                                                       
my $dbh = get_dbh($connection) 
   or die "Could not connect to ".$database." using schema ".$referral_schema."\n"; 
   

#remove ending slash from production output dir if necessary
$production_o = Production::ReferralResolution::clean_path($production_o);
   
#get referral paths
my $referralpaths = Production::ReferralResolution::get_referral_paths($dbh,
                                                                       $referral_schema,
                                                                       $referral_map_id,
                                                                       $distiller_style);

if ($referralpaths->{'status'} eq 'fail') {
   print STDOUT "Error: ".$referralpaths->{'message'};
   $dbh->disconnect;
   exit;
} else {
   print STDOUT " ".$referralpaths->{'message'};
}
   
#build paths for production
my $productionpaths = Production::ReferralResolution::build_production_paths($referralpaths,
                                                                             $prod_schema,
                                                                             $production_map_id,
                                                                             $production_o,
                                                                             $metadata_note,
                                                                             $distiller_style);

if ($productionpaths->{'status'} eq 'fail') {
   print STDOUT "Error: ".$productionpaths->{'message'};
   $dbh->disconnect;
   exit;
} else {
   print STDOUT " ".$productionpaths->{'message'};
}
   

#check for presence of all postscript and mim filez
my $referralqccheck = Production::ReferralResolution::do_referral_qc($dbh,
                                                                     $referralpaths,
                                                                     $productionpaths,
                                                                     $distiller_style,
                                                                     $database_link,
                                                                     $junk_directory);

if ($referralqccheck->{'status'} eq 'fail') {
   print STDOUT "Error: ".$referralqccheck->{'message'};
   $dbh->disconnect;
   exit; 
} else {
   print STDOUT " ".$referralqccheck->{'message'};
}

#update production metadata table
my $prodmetadataupdate = Production::ReferralResolution::update_production_metadata($dbh,
                                                                                    $referralpaths,
                                                                                    $productionpaths,
                                                                                    $metadata_note,
                                                                                    $database_link);

if ($prodmetadataupdate->{'status'} eq 'fail') {
   print STDOUT "Error: ".$prodmetadataupdate->{'message'};
   $dbh->disconnect;
   exit; 
} else {
   print STDOUT " ".$prodmetadataupdate->{'message'};
}

#move files to development machine /home/
my $transferprep = Production::ReferralResolution::stage_referral_files($superuser,
                                                                        $superusernode,
                                                                        $referralpaths,
                                                                        $productionpaths,
                                                                        $distiller_style,
                                                                        $junk_directory);
                                                                        
if ($transferprep->{'status'} eq 'fail') {
   print STDOUT "Error: ".$transferprep->{'message'};
   $dbh->disconnect;
   exit; 
} else {
   print STDOUT " ".$transferprep->{'message'};
}        

#write perl script to be executed on remote machine to ascii text file 
my $remotescript = Production::ReferralResolution::write_remote_script($superuser,
                                                                       $transferprep->{'stagingdir'},
                                                                       $productionpaths,
                                                                       $referralpaths,
                                                                       $distiller_style);
if ($remotescript->{'status'} eq 'fail') {
   print STDOUT "Error: ".$remotescript->{'message'};
   $dbh->disconnect;
   exit; 
} else {
   print STDOUT " ".$remotescript->{'message'};
}                                                                      



# secure copy it all over
my $scpfiles = Production::ReferralResolution::scp_staged_files($superuser,
                                                                $superusernode,
                                                                $transferprep->{'stagingdir'});                  
   
                                                                                                                           
if ($scpfiles->{'status'} eq 'fail') {
   print STDOUT "Error: ".$scpfiles->{'message'};
   $dbh->disconnect;
   exit; 
} else {
   print STDOUT " ".$scpfiles->{'message'};
}   
                                                                        
   
print STDOUT "Now we connect to ".$superusernode." using account ".$superuser." and execute the ezreffilemanager.pl script\n";

#ssh connect to the production machine and execute perl script that we copied over
#ssh is nifty
#ex  ssh -t schel010@node20.csvd.census.gov "cd /home/schel010/REF444427115_26875 ; perl ezreffilemanager.pl ; exit; ksh"
my $ssh = "ssh -t ".$superuser."@".$superusernode.".csvd.census.gov \" cd ".$transferprep->{'prodstagingdir'}." ; perl ezreffilemanager.pl; exit; ksh\"";

eval {
      system($ssh );
     }; # eval is a function - not a block
   if ($@) {
	   #I think this trap only works when the ssh itself fails.
      print STDOUT "Something went wrong on ".$superusernode.".  Check the log above here. \n";
   } else {
      print STDOUT "Exited ".$superusernode." cheerfully.  But check the log above here to see if we had any complaints on ".$superusernode.". Peace out.\n";
   }

#should we clean up the REFxxx directories on either machine?  To be safe, for checking what happened, lets let them pile up for now

$dbh -> disconnect;


__END__

=head1 AUTHOR

Matt! <Matthew.C.Schell@census.gov>

=head1 SEE ALSO

CAMPS Wiki at http://node101.csvd.census.gov/cpmb-bin/CAMPSwiki.pl

=head1 HISTORY

=head2 20090610

Updated to accept a new option for distiller style and possibility of accessing production metadata by a database link

=head2 20090708

Updated to accept a new option for a staging area junk directory.  If the script is not the superuser, he or she cannot write to /home/superuser in dev

=head2 20090713

Switched to use map_id, which is indexed in the metadata sheet table, instead of jobrun


=cut

