package Production::ReferralResolution;

# Matt! 02/09/09

use strict;
use warnings;

use File::Path;
use File::Copy;
use FindBin qw($RealBin);

=head1 NAME

  ReferralResolution.pm

  created: 02/09/2009
  last modified: 06/15/2009
                 01/15/2010 for bundled pdfs

=head1 SYNOPSIS

  Works with ezreferral script to move all files and metadata into production for an entity that has been run outside of production as a referral. 

=cut



our @EXPORT = qw( );
our @EXPORT_OK = qw(
  get_referral_paths
  build_production_paths
  do_referral_qc
  clean_path
  update_production_metadata
  stage_referral_files
  scp_staged_files
  write_remote_script
  get_referral_parms
); ### use MyModule qw( :all );
our %EXPORT_TAGS = ( 'all' => [ @EXPORT, @EXPORT_OK ] );


sub get_referral_parms {
   my $parmpath                = shift;
   
   #This sub gets the input parameters from a file
   #The path to the file is the only command line input to ezreferral.pl
   #We will expect that the input parameters are in the file in this exact order
   #Experimented with checking for the keys looking like certain text
   #But it can be messy - the ref resolution text can include "referral schema", for ex
   
   my $kount = 1;
   my($returnhash);
   $returnhash->{'status'} = 'pass';
   $returnhash->{'message'} = "";
   
   unless (-r $parmpath) {
      $returnhash->{'status'} = "fail";
      $returnhash->{'message'} = "Cant read any file at ".$parmpath."\n";
   }
   

   
   
   my $FH;
   open ($FH, "<", "$parmpath") or die "Can't open yo file, ".$parmpath."\n";
   
   my @lines = <$FH>;         
   close $FH;
   
   foreach my $parmline (@lines) {
      
      if ($kount == 1) {
         my @arr = split(/=/,$parmline,2);
         $returnhash->{'referral_schema'} = trim($arr[1]);
      }
      if ($kount == 2) {
         my @arr = split(/=/,$parmline,2);
         $returnhash->{'referral_pwd'} = trim($arr[1]);
      }
      if ($kount == 3) {
         my @arr = split(/=/,$parmline,2);
         $returnhash->{'prod_schema'} = trim($arr[1]);
      }
      if ($kount == 4) {
         my @arr = split(/=/,$parmline,2);
         $returnhash->{'database_link'} = uc(trim($arr[1]));
      }
      if ($kount == 5) {
         my @arr = split(/=/,$parmline,2);
         $returnhash->{'referral_map_id'} = trim($arr[1]);
      }
      if ($kount == 6) {
         my @arr = split(/=/,$parmline,2);
         $returnhash->{'production_o'} = trim($arr[1]);
      }
      if ($kount == 7) {
         my @arr = split(/=/,$parmline,2);
         $returnhash->{'metadata_note'} = trim($arr[1]);
      }
      if ($kount == 8) {
         my @arr = split(/=/,$parmline,2);
         $returnhash->{'superuser'} = trim($arr[1]);
      }
      if ($kount == 9) {
         my @arr = split(/=/,$parmline,2);
         $returnhash->{'superusernode'} = trim($arr[1]);
      }
      if ($kount == 10) {
         my @arr = split(/=/,$parmline,2);
         $returnhash->{'production_map_id'} = uc(trim($arr[1]));
      }
      if ($kount == 11) {
         my @arr = split(/=/,$parmline,2);
         $returnhash->{'distiller_style'} = uc(trim($arr[1]));
      }
      if ($kount == 12) {
         my @arr = split(/=/,$parmline,2);
         $returnhash->{'junk_directory'} = trim($arr[1]);
      }
      #optional
      if ($kount == 13) {
         my @arr = split(/=/,$parmline,2);
         $returnhash->{'database'} = trim($arr[1]);
      }
            
      $kount++;   
   }
   
   #default to prodbnch
   if ( ! defined ($returnhash->{'database'}) ) {
      $returnhash->{'database'} = "PRODBNCH";
   }   
      
   #problem checks
   if ( ! defined $returnhash->{'referral_schema'} ) {
      $returnhash->{'status'} = "fail";
      $returnhash->{'message'} = "Cant find a referral schema in ".$parmpath."\n";
   }
   if ( ! defined $returnhash->{'referral_pwd'} ) {
      $returnhash->{'status'} = "fail";
      $returnhash->{'message'} = "Cant find a referral password in ".$parmpath."\n";
   }
   if ( ! defined $returnhash->{'prod_schema'} ) {
      $returnhash->{'status'} = "fail";
      $returnhash->{'message'} = "Cant find a production schema in ".$parmpath."\n";
   }
   if ( ! defined $returnhash->{'database_link'} ) {
      $returnhash->{'status'} = "fail";
      $returnhash->{'message'} = "Cant find a database link in ".$parmpath."\n";
   }
   if ( ! defined $returnhash->{'referral_map_id'} ) {
      $returnhash->{'status'} = "fail";
      $returnhash->{'message'} = "Cant find a referral map id in ".$parmpath."\n";
   }
   if ( ! defined $returnhash->{'production_o'} ) {
      $returnhash->{'status'} = "fail";
      $returnhash->{'message'} = "Cant find a production output path in ".$parmpath."\n";
   }
   if ( ! defined $returnhash->{'metadata_note'} ) {
      $returnhash->{'status'} = "fail";
      $returnhash->{'message'} = "Cant find a metadata note in ".$parmpath."\n";
   }
   if ( ! defined $returnhash->{'superuser'} ) {
      $returnhash->{'status'} = "fail";
      $returnhash->{'message'} = "Cant find a production machine user in ".$parmpath."\n";
   }
   if ( ! defined $returnhash->{'superusernode'} ) {
      $returnhash->{'status'} = "fail";
      $returnhash->{'message'} = "Cant find a production machine name in ".$parmpath."\n";
   }
   if ( ! defined $returnhash->{'production_map_id'} ) {
      $returnhash->{'status'} = "fail";
      $returnhash->{'message'} = "Cant find a production map id, or NA, in ".$parmpath."\n";
   }
   if ( ! defined $returnhash->{'junk_directory'} ) {
      $returnhash->{'status'} = "fail";
      $returnhash->{'message'} = "Cant find a junk directory in ".$parmpath."\n";
   }
   if ( ! defined $returnhash->{'distiller_style'} ) {
      $returnhash->{'status'} = "fail";
      $returnhash->{'message'} = "Cant find a distiller style in ".$parmpath."\n";
   } else {
      
      if ($returnhash->{'distiller_style'} eq 'CMD' ||
          $returnhash->{'distiller_style'} eq 'WATCH' || 
          $returnhash->{'distiller_style'} eq 'PC' ) {             
          #ok
      } else {
         $returnhash->{'status'} = "fail";
         $returnhash->{'message'} = "Distiller style in ".$parmpath." must be CMD, PC, or WATCH\n";
      }
   }
   
   
   
   return $returnhash;
   
}


sub get_referral_paths {
   my $dbh                = shift;
   my $referral_schema    = shift;
   my $map_id              = shift;
   my $distiller_style    = shift;
   
   #This sub gets paths from the referral schema metadata
   #It checkes if all are defined where expected
   
   
   my($sql,$sth,$returnhash);
   $returnhash->{'status'} = 'pass';
   $returnhash->{'message'} = "";
   
   #Check that we have a match in referral metadata
   
   $sql = "SELECT COUNT(*) ";
   $sql.= "FROM ".$referral_schema.".METADATA_SHEET a ";
   $sql.= "WHERE a.map_id = :p1 ";
   $sth = $dbh->prepare($sql);
   $sth->bind_param( 1, $map_id);
   $sth->execute;
   $sth->bind_columns(\my($metcount));
   $sth->fetch();
   
   
   #If no match, return 
   if ($metcount == 0) {
      $returnhash->{'status'} = 'fail';
      $returnhash->{'message'} = "No metadata_sheet referral record found in schema " .$referral_schema. " with map id " .$map_id. " \n";
      return $returnhash;
   } 
   
   $sql = "SELECT CAMPS_DATA_MIMS,ARCHIVE_PATH,CAMPS_DISTILLER_IN,CAMPS_DISTILLER_OUT,CAMPS_DATA_PDFS ";
   $sql.= "FROM ".$referral_schema.".METADATA_SHEET a ";
   $sql.= "WHERE a.map_id = :p1 ";
   $sql.= "AND rownum = 1 ";
   $sth = $dbh->prepare($sql);
   $sth->bind_param( 1, $map_id);
   $sth->execute;
   $sth->bind_columns(\my ($camps_data_mims,
                           $archive_path,
                           $camps_distiller_in,
                           $camps_distiller_out,
                           $camps_data_pdfs));
   $sth->fetch();
   
   $returnhash->{'camps_data_mims'}       = $camps_data_mims;
   $returnhash->{'archive_path'}          = $archive_path;
   $returnhash->{'camps_distiller_in'}    = $camps_distiller_in;
   $returnhash->{'camps_distiller_out'}   = $camps_distiller_out;
   $returnhash->{'map_id'}                = $map_id;
   $returnhash->{'camps_data_pdfs'}       = $camps_data_pdfs;
   $returnhash->{'schema'}                = uc($referral_schema);  #make uppercase for sqls on table owners

   
   
   #Check for problems
   unless (defined ($returnhash->{'camps_data_mims'})) {
      $returnhash->{'status'} = 'fail';
      $returnhash->{'message'} = $returnhash->{'message'}." Problem getting referral camps_data_mims\n";
   }
   unless (defined ($returnhash->{'archive_path'})) {
      $returnhash->{'status'} = 'fail';
      $returnhash->{'message'} = $returnhash->{'message'}." Problem getting referral archive_path\n";
   }
   if ($distiller_style ne 'CMD') {
      #do not expect distiller paths if using command line distiller
      unless (defined ($returnhash->{'camps_distiller_in'})) {
         $returnhash->{'status'} = 'fail';
         $returnhash->{'message'} = $returnhash->{'message'}." Problem getting referral camps_distiller_in\n";
      }
      unless (defined ($returnhash->{'camps_distiller_out'})) {
         $returnhash->{'status'} = 'fail';
         $returnhash->{'message'} = $returnhash->{'message'}." Problem getting referral camps_distiller_out\n";
      }
   }
   unless (defined ($returnhash->{'map_id'})) {
      $returnhash->{'status'} = 'fail';
      $returnhash->{'message'} = $returnhash->{'message'}." Problem getting referral map_id\n";
   }
   unless (defined ($returnhash->{'camps_data_pdfs'})) {
      $returnhash->{'status'} = 'fail';
      $returnhash->{'message'} = $returnhash->{'message'}." Problem getting referral camps_data_pdfs path\n";
   }   
   
   if ($returnhash->{'status'} eq 'pass') {
      $returnhash->{'message'} = "Retrieved paths from the referral schema\n";
   }
   
   return $returnhash;
   
}

sub build_production_paths {
   my $referralpaths       = shift;
   my $prod_schema         = shift;
   my $production_map_id   = shift;
   my $production_o        = shift;
   my $metadata_note       = shift;
   my $distiller_style     = shift;
   
   #This sub builds paths to production based on the production output directory passed into ezreferral.pl
   #It appends the expected paths to the base output path and does some checks
   #The paths built here will be used to update production metadata and move files (later)
   
   my($returnhash);
   
   $returnhash->{'status'} = 'pass';
   $returnhash->{'message'} = "";
   
   #pass this tag along, just in case
   $returnhash->{'map_id'} = $production_map_id;
   
   # $production_o does not end in slash, stripped in caller
   # Build paths
   
   $returnhash->{'schema'} = uc($prod_schema); #uppercase for sql
   $returnhash->{'note'} = $metadata_note;
   
   $returnhash->{'camps_data_mims'} = $production_o.'/mim'.$referralpaths->{'archive_path'};
   
   if ($distiller_style eq 'WATCH') {
      $returnhash->{'camps_distiller_in'} = $production_o.'/dist/linux/drop_in';   
      $returnhash->{'camps_distiller_out'} = $production_o.'/dist/linux/drop_out';
   } elsif ($distiller_style eq 'PC') {
      $returnhash->{'camps_distiller_in'} = $production_o.'/dist/pc/in';   
      $returnhash->{'camps_distiller_out'} = $production_o.'/dist/pc/out';
   } else { # $distiller_style eq CMD
      $returnhash->{'camps_distiller_in'} = '';   
      $returnhash->{'camps_distiller_out'} = '';
   }
   
   $returnhash->{'camps_data_pdfs'} = $production_o.'/pdf'.$referralpaths->{'archive_path'};
   
   $returnhash->{'camps_data_work'} = $production_o.'/work'.$referralpaths->{'archive_path'};
   
   #strip any trailing slashes
   $returnhash->{'camps_data_mims'} = Production::ReferralResolution::clean_path($returnhash->{'camps_data_mims'});
   $returnhash->{'camps_distiller_in'} = Production::ReferralResolution::clean_path($returnhash->{'camps_distiller_in'});
   $returnhash->{'camps_distiller_out'} = Production::ReferralResolution::clean_path($returnhash->{'camps_distiller_out'});
   $returnhash->{'camps_data_pdfs'} = Production::ReferralResolution::clean_path($returnhash->{'camps_data_pdfs'});
   $returnhash->{'camps_data_work'} = Production::ReferralResolution::clean_path($returnhash->{'camps_data_work'});
   
   
   #Super extra careful checks
   unless (defined ($returnhash->{'schema'})) {
      $returnhash->{'status'} = 'fail';
      $returnhash->{'message'} = $returnhash->{'message'}." Problem building production schema\n";
   }   
      unless (defined ($returnhash->{'map_id'})) {
      $returnhash->{'status'} = 'fail';
      $returnhash->{'message'} = $returnhash->{'message'}." Problem building production map id\n";
   } 
   unless (defined ($returnhash->{'camps_data_mims'})) {
      $returnhash->{'status'} = 'fail';
      $returnhash->{'message'} = $returnhash->{'message'}." Problem building production data mims path\n";
   }   
   unless (defined ($returnhash->{'camps_distiller_in'})) {
      $returnhash->{'status'} = 'fail';
      $returnhash->{'message'} = $returnhash->{'message'}." Problem building production distiller in\n";
   }   
   unless (defined ($returnhash->{'camps_distiller_out'})) {
      $returnhash->{'status'} = 'fail';
      $returnhash->{'message'} = $returnhash->{'message'}." Problem building production distiller out\n";
   }   
   unless (defined ($returnhash->{'camps_data_pdfs'})) {
      $returnhash->{'status'} = 'fail';
      $returnhash->{'message'} = $returnhash->{'message'}." Problem building production data pdfs\n";
   }   
   unless (defined ($returnhash->{'camps_data_work'})) {
      $returnhash->{'status'} = 'fail';
      $returnhash->{'message'} = $returnhash->{'message'}." Problem building production data work\n";
   }     
   
   if ($returnhash->{'status'} eq 'pass') {
      $returnhash->{'message'} = "Successfully built paths to the production environment\n";
   } 
   
   return $returnhash;
   
}


sub do_referral_qc {
   my $dbh                = shift;
   my $referralpaths      = shift;
   my $productionpaths    = shift;
   my $distiller_style    = shift;
   my $database_link      = shift;
   my $junk_directory     = shift;
   
   #This sub does a bunch of super careful checks on what we have so far


   my($returnhash,$sql,$sth,$outps,$outpdf,$outmim);
   my $sheetcount = 0;   
   $returnhash->{'status'} = 'pass';
   $returnhash->{'message'} = "";
   
   #learn bundle type
   $sql = "SELECT a.bundled ";
   $sql.= "FROM ".$referralpaths->{'schema'}.".METADATA_SHEET a ";
   $sql.= "WHERE a.map_id = :p1 ";
   $sql.= "AND a.sheet_number = :p2 ";
   $sth = $dbh->prepare( $sql );
   $sth->bind_param( 1, $referralpaths->{'map_id'});
   $sth->bind_param( 2, '001');
   $sth->execute;
   $sth->bind_columns(\my($bundleflag));
   $sth->fetch();
   
   
   $sql = "SELECT sheet_id ";
   $sql.= "FROM ".$referralpaths->{'schema'}.".METADATA_SHEET a ";
   $sql.= "WHERE a.map_id = :p1 ";
   $sth = $dbh->prepare( $sql );
   $sth->bind_param( 1, $referralpaths->{'map_id'});
   $sth->execute();
   
   my $sheetid;
   $sth->bind_columns( undef, \$sheetid );
   
   while( $sth->fetch() ) {
      
      #catch count of sheets for later
      $sheetcount = $sheetcount + 1;
      
      #test that referral ps or pdf plus mim exist and have non-zero size

      if ($distiller_style ne 'CMD') {
         #expect to find ps in the out directory.  
         #I think its too confusing to say either in or out is ok
         $outps = $referralpaths->{'camps_distiller_out'}."/".$sheetid.".ps";
         
         if (! -s $outps ) {
            $returnhash->{'status'} = 'fail';
            $returnhash->{'message'} = $returnhash->{'message'}." Cannot find referral ps file at ".$outps."\n";
         }
      } elsif ($bundleflag != 1) { #command line distiller, we don't need no stinking ps
         
         $outpdf = $referralpaths->{'camps_data_pdfs'}."/".$sheetid.".pdf";
         
         if (! -s $outpdf ) {
            $returnhash->{'status'} = 'fail';
            $returnhash->{'message'} = $returnhash->{'message'}." Cannot find referral pdf file at ".$outpdf."\n";
         }
      } 
      
      #check el mim
      $outmim = $referralpaths->{'camps_data_mims'}."/".$sheetid.".mim.gz";
      
      if (! -s $outmim ) {
         $returnhash->{'status'} = 'fail';
         $returnhash->{'message'} = $returnhash->{'message'}." Cannot find referral mim file at ".$outmim."\n";
      }     
      
   } #end while loop over sheet ids
   
   $sth->finish();
   
   
   #check special bundle pdfs
   if ($bundleflag == 1 || $bundleflag == 2) {
      
      #there should be a bundled pdf without a sheet number
      $outpdf = $referralpaths->{'camps_data_pdfs'}."/".$referralpaths->{'map_id'}.".pdf";
      
      if (! -s $outpdf ) {
            $returnhash->{'status'} = 'fail';
            $returnhash->{'message'} = $returnhash->{'message'}." Cannot find bundled referral pdf file at ".$outpdf."\n";
         }
   }
   
   
   my $pdfcount;
   
   if ($bundleflag == 1) {
      $pdfcount = 1;  #just one bundled pdf
   } elsif ($bundleflag == 2) {
      $pdfcount = $sheetcount + 1;  #Add bundled pdf to count
   } else {
      $pdfcount = $sheetcount;
   }
   
   #check if there are any other mim or ps files with the same name but different sheet numbers from metadata
   
   if ($distiller_style ne 'CMD') {
      my $globstring = $referralpaths->{'map_id'} . "*.ps";   
      my @psfiles = glob $referralpaths->{'camps_distiller_out'}."/".$globstring;
   
      if (scalar(@psfiles) != $sheetcount) {
         $returnhash->{'status'} = 'fail';
         $returnhash->{'message'} = $returnhash->{'message'}." There appear to be a different number of ps files at ".$referralpaths->{'camps_distiller_out'}." than are listed in the metadata \n";
      }
   } else { 
      #command line distiller
      my $pdfglobstring = $referralpaths->{'map_id'} . "*.pdf";   
      my @pdffiles = glob $referralpaths->{'camps_data_pdfs'}."/".$pdfglobstring;
      
      
      if (scalar(@pdffiles) != $pdfcount) {
         $returnhash->{'status'} = 'fail';
         $returnhash->{'message'} = $returnhash->{'message'}." There appear to be a different number of pdf files at ".$referralpaths->{'camps_data_pdfs'}." than are listed in the metadata \n";
      }
   }
   
   #Explicitly include the .gz extension
   #We're gonna allow a copy of unzipped mims to reside alongside the real ones
   my $globstring = $referralpaths->{'map_id'} . "*.mim.gz";
   my @mimfiles = glob $referralpaths->{'camps_data_mims'}."/".$globstring;
   
   if (scalar(@mimfiles) != $sheetcount) {
      $returnhash->{'status'} = 'fail';
      $returnhash->{'message'} = $returnhash->{'message'}." There appear to be a different number of mim files at ".$referralpaths->{'camps_data_mims'}." than are listed in the metadata \n";
   }
   

   #There are some supplemental files that technically allow output in the pdf directory
   #Havent managed this yet. Lets check for anything weird in the pdf directory and bomb
   $globstring = $referralpaths->{'map_id'} . "*.*";
   my @weirdfiles = glob $referralpaths->{'camps_data_pdfs'}."/".$globstring;
   
   foreach (@weirdfiles) {
     my $filename = $_; 
     if ($filename !~ m/.pdf$/) {
        $returnhash->{'status'} = 'fail';
        $returnhash->{'message'} = $returnhash->{'message'}." There appear to be some weird supplemental file(s) in ".$referralpaths->{'camps_data_pdfs'}." Eg: ".$filename."\n";
      }   
   }
   
   
#    if (scalar(@weirdfiles) > 0) {
#       $returnhash->{'status'} = 'fail';
#       $returnhash->{'message'} = $returnhash->{'message'}." There appear to be some weird supplemental file(s) in ".$referralpaths->{'camps_data_pdfs'}." Eg: ".$weirdfiles[0]."\n";
#    }
   
   #check that referral and production schemas have the same column names in metadata_sheet
   $sql = "SELECT COUNT(*) ";
   $sql.= "FROM user_tab_columns a ";
   $sql.= "WHERE a.table_name = :p1 ";
   $sql.= "and a.column_name NOT IN ";
   $sql.= "(SELECT column_name from all_tab_columns";
   if ($database_link ne 'NA') {
      $sql.= "@".$database_link;
   }
   $sql.=" b ";
   $sql.= "WHERE b.owner = :p2 ";
   $sql.= "AND b.table_name = :p3)";
   $sth = $dbh->prepare($sql);
   $sth->bind_param( 1, 'METADATA_SHEET');
   $sth->bind_param( 2, $productionpaths->{'schema'});
   $sth->bind_param( 3, 'METADATA_SHEET');
   $sth->execute;
   $sth->bind_columns(\my($refcolcount));
   $sth->fetch();
   
   
   if ($refcolcount > 0) {
      $returnhash->{'status'} = 'fail';
      $returnhash->{'message'} = $returnhash->{'message'}." There appear to be more columns in ".$referralpaths->{'schema'}.".METADATA_SHEET";
      $returnhash->{'message'} = $returnhash->{'message'}." than in ".$productionpaths->{'schema'}.".METADATA_SHEET\n";
   }
   
   $sql = "SELECT COUNT(*) ";
   $sql.= "FROM all_tab_columns";
   if ($database_link ne 'NA') {
      $sql.= "@".$database_link;
   }
   $sql.= " a ";
   $sql.= "WHERE a.table_name = :p1 ";
   $sql.= "AND a.owner = :p2 ";
   $sql.= "and a.column_name NOT IN ";
   $sql.= "(SELECT column_name from user_tab_columns b ";
   $sql.= "WHERE b.table_name = :p3)";
   $sth = $dbh->prepare($sql);
   $sth->bind_param( 1, 'METADATA_SHEET');
   $sth->bind_param( 2, $productionpaths->{'schema'});
   $sth->bind_param( 3, 'METADATA_SHEET');
   $sth->execute;
   $sth->bind_columns(\my($prodcolcount));
   $sth->fetch();
   
   if ($prodcolcount > 0) {
      $returnhash->{'status'} = 'fail';
      $returnhash->{'message'} = $returnhash->{'message'}." There appear to be more columns in ".$productionpaths->{'schema'}.".METADATA_SHEET";
      $returnhash->{'message'} = $returnhash->{'message'}." than in ".$referralpaths->{'schema'}.".METADATA_SHEET\n";
   }
   
   #If the user has entered no production map id
   #meaning that this entity/project has never run in production
   #double check that there is no sheet_id (primary key) in the production metadata table
   #that matches the referral metadata we are about to copy in
   
   if ($productionpaths->{'map_id'} eq 'NA') {
      
      $sql = "SELECT COUNT(*) ";
      $sql.= "FROM ".$referralpaths->{'schema'}.".METADATA_SHEET";
      $sql.= " a ";
      $sql.= "WHERE a.map_id = :p1 ";
      $sql.= "AND a.sheet_id IN ";
      $sql.= "(SELECT b.sheet_id FROM ".$productionpaths->{'schema'}.".METADATA_SHEET";
      if ($database_link ne 'NA') {
         $sql.= "@".$database_link;
      }
      $sql.=" b)";
      $sth = $dbh->prepare($sql);
      $sth->bind_param( 1, $referralpaths->{'map_id'});
      $sth->execute;
      $sth->bind_columns(\my($sheetidcount));
      $sth->fetch();
      
      if ($sheetidcount > 0) {
         $returnhash->{'status'} = 'fail';
         $returnhash->{'message'}.= " No production map_id id was input but there are already some";
         $returnhash->{'message'}.= " sheet_id values in ".$productionpaths->{'schema'}.".METADATA_SHEET";  
         $returnhash->{'message'}.= " that match what is in ".$referralpaths->{'schema'}.".METADATA_SHEET\n";
         $returnhash->{'message'}.= " Are you sure that this entity has not already run in production?\n";
      }
        
   }
   
   
   #If the user has entered a production map id
   #meaning that this entity has run in production
   #check that the newly built production paths match the old production paths when it ran before
   #they should be the same, and if not, cleanup later just gets too complicated
   
   if ($productionpaths->{'map_id'} ne 'NA') {
      
      #First, see if there's a record in production at all
      
      $sql = "SELECT count(*) ";
      $sql.= "FROM ".$productionpaths->{'schema'}.".METADATA_SHEET";
      if ($database_link ne 'NA') {
         $sql.= "@".$database_link;
      }
      $sql.= " a ";
      $sql.= "WHERE a.map_id = :p1 ";
      $sth = $dbh->prepare($sql);
      $sth->bind_param( 1, $productionpaths->{'map_id'});
      $sth->execute;
      $sth->bind_columns(\my($prodsheetcount));
      $sth->fetch();
      
      if ($prodsheetcount == 0) {
         
         $returnhash->{'status'} = 'fail';
         $returnhash->{'message'}.= " Map id ".$productionpaths->{'map_id'}." does not exist in ".$productionpaths->{'schema'}.".METADATA_SHEET. Are you sure it has run in production?\n";
      
      } else {      
         
         $sql = "SELECT camps_data_mims, ";
         $sql.= "camps_data_pdfs, camps_data_work, ";
         $sql.= "camps_distiller_in, camps_distiller_out ";
         $sql.= "FROM ".$productionpaths->{'schema'}.".METADATA_SHEET";
         if ($database_link ne 'NA') {
            $sql.= "@".$database_link;
         }
         $sql.= " a ";
         $sql.= "WHERE a.map_id = :p1 ";
         $sql.= "AND rownum = 1 ";
         $sth = $dbh->prepare($sql);
         $sth->bind_param( 1, $productionpaths->{'map_id'});
         $sth->execute;
         $sth->bind_columns(\my ($camps_data_mims,
                                 $camps_data_pdfs,
                                 $camps_data_work,
                                 $camps_distiller_in,
                                 $camps_distiller_out));
         $sth->fetch();
         
         unless ($productionpaths->{'camps_data_mims'} eq $camps_data_mims) {
            $returnhash->{'status'} = 'fail';
            $returnhash->{'message'}.= " Our new camps_data_mims path(".$productionpaths->{'camps_data_mims'}.") does not match the run in production.  Please investigate.\n";   
         }
         unless ($productionpaths->{'camps_data_pdfs'} eq $camps_data_pdfs) {
            $returnhash->{'status'} = 'fail';
            $returnhash->{'message'}.= " Our new camps_data_pdfs path(".$productionpaths->{'camps_data_pdfs'}.") does not match the run in production.  Please investigate.\n";   
         }
         unless ($productionpaths->{'camps_data_work'} eq $camps_data_work) {
            $returnhash->{'status'} = 'fail';
            $returnhash->{'message'}.= " Our new camps_data_work path(".$productionpaths->{'camps_data_work'}.") does not match the run in production.  Please investigate.\n";   
         }
         if ($distiller_style ne 'CMD') {
            unless ($productionpaths->{'camps_distiller_in'} eq $camps_distiller_in) {
               $returnhash->{'status'} = 'fail';
               $returnhash->{'message'}.= " Our new camps_distiller_in path(".$productionpaths->{'camps_distiller_in'}.") does not match the run in production.  Please investigate.\n";   
            }
            unless ($productionpaths->{'camps_distiller_out'} eq $camps_distiller_out) {
               $returnhash->{'status'} = 'fail';
               $returnhash->{'message'}.= " Our new camps_distiller_out path(".$productionpaths->{'camps_distiller_out'}.") does not match the run in production.  Please investigate.\n";   
            }
         } else {
            unless (! defined $camps_distiller_in) {
               $returnhash->{'status'} = 'fail';
               $returnhash->{'message'}.= " Production metadata shows a camps_distiller_in path(".$camps_distiller_in."). This is unexpected when running command line distiller\n";   
            }
            unless (! defined $camps_distiller_out) {
               $returnhash->{'status'} = 'fail';
               $returnhash->{'message'}.= " Production metadata shows a camps_distiller_out path(".$camps_distiller_out."). This is unexpected when running command line distiller\n";   
            }
         }
      } 
   } #End check on if there is a map_id in production
   
   #If this is command line distiller, the pdfs should have file size 
   #populated in the referral metadata 
   if ($distiller_style eq 'CMD') {
      
      if ($bundleflag != 1) {
         #SOP. Technically we could also check for bundled size when bundle = 2, but Im not worried about it
         $sql = "SELECT pdf_size_bytes ";
      } elsif ($bundleflag == 1) {
         #No pdf size bytes for individual sheets when all bundled into one
         $sql = "SELECT pdf_size_bytes_bundled ";
      }      
      
      $sql.= "FROM ".$referralpaths->{'schema'}.".METADATA_SHEET";
      $sql.= " a ";
      $sql.= "WHERE a.map_id = :p1 ";
      $sql.= "and rownum = 1 ";
      $sth = $dbh->prepare($sql);
      $sth->bind_param( 1, $referralpaths->{'map_id'});;
      $sth->execute;
      $sth->bind_columns(\my($pdffilesize));
      $sth->fetch();
      
      unless (defined $pdffilesize && $pdffilesize > 0)  {
         $returnhash->{'status'} = 'fail';
         $returnhash->{'message'}.= " Cant find a pdf_size_bytes in ".$referralpaths->{'schema'}.".METADATA_SHEET for map_id ".$referralpaths->{'map_id'}.". We expect this for command line distiller\n";   
      }
      
   }
   
   #check if temp junk directory exists
   unless (-d $junk_directory) {
         $returnhash->{'status'} = 'fail';
         $returnhash->{'message'}.= " Cant find junk directory ".$junk_directory." where we are gonna stage files.  Maybe you need to make it?\n";   
      }
   

   if ($returnhash->{'status'} eq 'pass') {
      $returnhash->{'message'} = "Performed qc check and all files appear to be present in the referral area and metadata table columns match production\n";
   } 
   
   return $returnhash;
   
}


sub update_production_metadata {
   my $dbh                = shift;
   my $referralpaths      = shift;
   my $productionpaths    = shift;
   my $metadata_note      = shift;
   my $database_link      = shift;
   
   #This sub updates the production schema metadata
   #It deletes records that are already there, if necessary
   #And updates all paths in production based on the paths we have built
   #Note: Does not explicitly commit.  If we bomb later writing files and
   #      whatnot, there should be a rollback so metadata returns to where 
   #      we were.  This was accidental but I think I like it.

   my($returnhash,$sql,$sth);
   $returnhash->{'status'} = 'pass';
   $returnhash->{'message'} = "";
   
   #Find out how many records we are deleting
   
   if ($productionpaths->{'map_id'} ne 'NA') {
      
      $sql = "SELECT COUNT(*) ";
      $sql.= "FROM ".$productionpaths->{'schema'}.".METADATA_SHEET";
      if ($database_link ne 'NA') {
         $sql.= "@".$database_link;
      }
      $sql.= " a ";
      $sql.= "WHERE a.map_id = :p1 ";
      $sth = $dbh->prepare($sql);
      $sth->bind_param( 1, $productionpaths->{'map_id'});
      $sth->execute;
      $sth->bind_columns(\my($prodmetcount));
      $sth->fetch();
      
      $returnhash->{'message'} = "Deleting ".$prodmetcount." records from the production metadata table\n";
      
      #Delete them
      $sql = "DELETE ";
      $sql.= "FROM ".$productionpaths->{'schema'}.".METADATA_SHEET";
      if ($database_link ne 'NA') {
         $sql.= "@".$database_link;
      }
      $sql.= " a ";
      $sql.= "WHERE a.map_id = :p1 ";
      $sth = $dbh->prepare($sql);
      $sth->bind_param( 1, $productionpaths->{'map_id'});
      $sth->execute;
      
   } else {
      $returnhash->{'message'} = "Deleting 0 records from the production metadata table since no production map id provided\n";
   }
   
   $sql = "SELECT COUNT(*) ";
   $sql.= "FROM ".$referralpaths->{'schema'}.".METADATA_SHEET a ";
   $sql.= "WHERE a.map_id = :p1 ";
   $sth = $dbh->prepare($sql);
   $sth->bind_param( 1, $referralpaths->{'map_id'});
   $sth->execute;
   $sth->bind_columns(\my($refmetcount));
   $sth->fetch();
   
   $returnhash->{'message'} = $returnhash->{'message'}." Inserting ".$refmetcount." records into the production metadata table\n";
   

   #insert referral schema records
   $sql = "INSERT INTO";
   $sql.= " ".$productionpaths->{'schema'}.".METADATA_SHEET";
   if ($database_link ne 'NA') {
      $sql.= "@".$database_link;
   }
   $sql.= " a ";
   $sql.= "SELECT * FROM ".$referralpaths->{'schema'}.".METADATA_SHEET b ";
   $sql.= "WHERE b.map_id = :p1 ";
   $sth = $dbh->prepare($sql);
   $sth->bind_param( 1, $referralpaths->{'map_id'});
   $sth->execute;  
   

   #update paths
   $sql = "UPDATE ".$productionpaths->{'schema'}.".METADATA_SHEET";
   if ($database_link ne 'NA') {
      $sql.= "@".$database_link;
   }
   $sql.= " a ";
   $sql.= "SET ";
   $sql.= "a.camps_data_pdfs = :p1, ";
   $sql.= "a.camps_data_mims = :p2, ";
   $sql.= "a.camps_data_work = :p3, ";
   $sql.= "a.camps_distiller_in = :p4, ";
   $sql.= "a.camps_distiller_out = :p5, ";
   $sql.= "a.note = a.note || ' ' || :p6 ";
   $sql.= "WHERE a.map_id = :p7 ";
   $sth = $dbh->prepare($sql);
   $sth->bind_param( 1, $productionpaths->{'camps_data_pdfs'});
   $sth->bind_param( 2, $productionpaths->{'camps_data_mims'});
   $sth->bind_param( 3, $productionpaths->{'camps_data_work'});
   $sth->bind_param( 4, $productionpaths->{'camps_distiller_in'});
   $sth->bind_param( 5, $productionpaths->{'camps_distiller_out'});
   $sth->bind_param( 6, $productionpaths->{'note'});
   $sth->bind_param( 7, $referralpaths->{'map_id'});
   $sth->execute;     
   
  
   if ($returnhash->{'status'} eq 'pass') {
      $returnhash->{'message'} = $returnhash->{'message'}." Updated metadata in production schema\n";
   } 
   
   return $returnhash;
   
}


sub stage_referral_files {
   my $superuser          = shift;
   my $superusernode      = shift;
   my $referralpaths      = shift;
   my $productionpaths    = shift;
   my $distiller_style    = shift;
   my $junk_directory     = shift;
   
   #This sub makes a new directory in the specified junk directory of the script user
   #It copies all postscript or pdf, plus mim files we will be transferring to production to this staging directory
   
   #note on paths
   #node20 home is /mt/users/schel010 with a softlink at / with home->/mt/users
   #Node19 home is /home/beard001 with no softlinks
   #Node101 home is /mt/home/schel010 with a softlink at / with home->/mt/home
   #so we will assume that the superusers home always works like /home/user on all machines   

   my($returnhash);
   $returnhash->{'status'} = 'pass';
   $returnhash->{'message'} = "";
   

   
   #copy to a new dir named REF<map_id>_<processid>
   my $stagingdir = $junk_directory."/REF".$referralpaths->{'map_id'}."_".$$;
   my $prodstagingdir = "/home/".$superuser."/REF".$referralpaths->{'map_id'}."_".$$;
   
   $returnhash->{'stagingdir'} = $stagingdir;
   $returnhash->{'prodstagingdir'} = $prodstagingdir;
   
   #cleanup any old dirs in home just in case   
   eval {
      rmtree($stagingdir) 
     }; # eval is a function - not a block
   if ($@) {
     die "Couldn't remove " . $stagingdir . ": $@";
   }
   
   #Make dir like /home/schel010/REF402135028_24740
   eval {
      mkpath($stagingdir, 1, 0777) # won't work w/o UMASK despite documentation
     }; # eval is a function - not a block
   if ($@) {
     die "Couldn't create " . $stagingdir . ": $@";
   }
   
   
   if ($distiller_style ne 'CMD') { 
      #copy ps in there if watchy
      my $globstring = $referralpaths->{'map_id'} . "*.ps";   
      my @psfiles = glob $referralpaths->{'camps_distiller_out'}."/".$globstring;
   
      my $file;
   
      foreach (@psfiles) {
         $file = $_; #Have to do this for some reason
         copy($file,$stagingdir."/.") || ($returnhash->{'status'} = 'fail');
      }
   } else {
      #command line distiller.  Grab pdfs
      my $pdfglobstring = $referralpaths->{'map_id'} . "*.pdf";   
      my @pdffiles = glob $referralpaths->{'camps_data_pdfs'}."/".$pdfglobstring;
   
      my $pdffile;
   
      foreach (@pdffiles) {
         $pdffile = $_; #Have to do this for some reason
         copy($pdffile,$stagingdir."/.") || ($returnhash->{'status'} = 'fail');
      }
   }
   
   #copy mim and any supplemental files in there
   my $globstring = $referralpaths->{'map_id'} . "*";
   my @mimfiles = glob $referralpaths->{'camps_data_mims'}."/".$globstring;
   
   foreach (@mimfiles) {
      #avoid copying unzipped mims if they live alongside the zipped
      unless ($_ =~ m/.mim$/i) {
         my $file = $_; 
         copy($file,$stagingdir."/.") || ($returnhash->{'status'} = 'fail');
      }
   }
   
   
   if ($returnhash->{'status'} eq 'fail') {
      $returnhash->{'message'} = $returnhash->{'message'}." Problem copying mims, pdfs, or postscripts into ".$stagingdir."\n";
   } 
   
   if ($returnhash->{'status'} eq 'pass') {
      $returnhash->{'message'} = $returnhash->{'message'}." Completed copying ps or pdf and mim files to ".$stagingdir." \n";
   } 
   
   
   return $returnhash;
   
}


sub write_remote_script {
   my $superuser          = shift;
   my $stagingdir         = shift;
   my $productionpaths    = shift;
   my $referralpaths      = shift;
   my $distiller_style    = shift;
   
   #This sub writes a perl script to the staging directory
   #The script, when run on the production machine, will delete and copy files in production

   my($returnhash);
   $returnhash->{'status'} = 'pass';
   $returnhash->{'message'} = "";
   
   my $map_id;
   if ($productionpaths->{'map_id'} eq 'NA') {
      $map_id = $referralpaths->{'map_id'};
   } else {
      $map_id = $productionpaths->{'map_id'};
   }   
   
   #write script header to file
   #should probably do all of this with the qq[] syntax instead of always escaping each character
   #I like how the {}s still line up this way though.  Thats dumb
   
   open (MYFILE, ">".$stagingdir."/ezreffilemanager.pl");
   print MYFILE "#!/usr/bin/perl\n";
   print MYFILE "#This script was written by the CAMPS ez_referral utility\n";
   print MYFILE "#To resolve the referral with map_id ".$map_id."\n";
   print MYFILE "\n";
   
   print MYFILE "use strict;\n";
   print MYFILE "use warnings;\n";
   print MYFILE "use File::Path;\n";
   print MYFILE "use File::Copy;\n";
   print MYFILE "use Cwd;\n";
   
   #set umask
   print MYFILE "my \$um = umask;\n";
   print MYFILE "umask 000;\n";
   
   print MYFILE "my \$current=getcwd();\n";
   print MYFILE "print STDOUT \"Starting ezreffilemanager.pl in \".\$current.\"\\n\";\n";
   
   #prep array of file directories
   #f_dirs are for cleanup.  Why'd I name it like that?
   #final_dirs are where we are putting some files
   print MYFILE "my \@f_dirs;\n";
   if ($distiller_style ne 'CMD') {
      print MYFILE "\@f_dirs = (\"".$productionpaths->{'camps_data_mims'}."\", \"".$productionpaths->{'camps_data_pdfs'}."\", \"".$productionpaths->{'camps_data_work'}."\", \"".$productionpaths->{'camps_distiller_in'}."\", \"".$productionpaths->{'camps_distiller_out'}."\" );\n";  
   } else {
      print MYFILE "\@f_dirs = (\"".$productionpaths->{'camps_data_mims'}."\", \"".$productionpaths->{'camps_data_pdfs'}."\", \"".$productionpaths->{'camps_data_work'}."\" );\n";
   }  
   print MYFILE "my \@final_dirs;\n"; 
   if ($distiller_style ne 'CMD') {
      print MYFILE "\@final_dirs = (\"".$productionpaths->{'camps_data_mims'}."\", \"".$productionpaths->{'camps_data_pdfs'}."\", \"".$productionpaths->{'camps_distiller_in'}."\", \"".$productionpaths->{'camps_distiller_out'}."\" );\n";  
   } else {
      print MYFILE "\@final_dirs = (\"".$productionpaths->{'camps_data_mims'}."\", \"".$productionpaths->{'camps_data_pdfs'}."\" );\n"; 
   } 

   print MYFILE "if (\"".$productionpaths->{'map_id'}."\" eq 'NA') {\n";
      
      #Supposedly this guy never ran in production before
      #If we see anything in the production area we die 
      
      print MYFILE "my \$globstring = \"".$referralpaths->{'map_id'}."*\";\n";
      #like $globstring = "BAS09C27115*";
      
      print MYFILE "my \@mimfiles = glob \"".$productionpaths->{'camps_data_mims'}."\".\"/\".\$globstring;\n";
      print MYFILE "my \@pdffiles = glob \"".$productionpaths->{'camps_data_pdfs'}."\".\"/\".\$globstring;\n";
      print MYFILE "my \@wrkfiles = glob \"".$productionpaths->{'camps_data_work'}."\".\"/\".\$globstring;\n";
      if ($distiller_style ne 'CMD') {
         print MYFILE "my \@psfiles = glob \"".$productionpaths->{'camps_distiller_in'}."\".\"/\".\$globstring;\n";
         print MYFILE "my \@pdfiles = glob \"".$productionpaths->{'camps_distiller_out'}."\".\"/\".\$globstring;\n";
      } else {
         print MYFILE "my \@psfiles;\n";
         print MYFILE "my \@pdfiles;\n";
      }
      
      print MYFILE "if (\@mimfiles or \@pdffiles or \@wrkfiles or \@psfiles or \@pdfiles) {\n";
         #if we find any files die
         print MYFILE "die \"Oops, there are files in the output area but no map_id to cleanup was entered\";\n";  
      print MYFILE "}\n";   
      
      #make directories
      #they may already be present if the job died after directory processing but before mim generation
      #dont bother making a work directory
      
      print MYFILE "foreach (\@final_dirs) {\n";
         print MYFILE "unless (-d \$_) {";
            print MYFILE "print STDOUT \"   Making directory \".\$_.\"\\n\";\n";
            print MYFILE "mkpath(\$_, 1, 0777); \n";
         print MYFILE "}\n";  
      print MYFILE "}\n";
         
              
   print MYFILE "} else {\n";
   
      #this job has run in production before
      
      #If this job has run in production, no need to make directories
      #But better double check that they are there and we can write to them 
      print MYFILE "foreach (\@f_dirs) {\n";
         print MYFILE "unless (-d \$_) {\n";
            print MYFILE "die \"Supposedly this job ran in production previously but I can't locate \".\$_.\"\\n\";\n";  
         print MYFILE "}\n";  
      print MYFILE "}\n";      
      
      #then, locate any production files and delete them
      print MYFILE "my \@files;\n";     
      
      print MYFILE "foreach (\@f_dirs) {\n";
         #push all files to be deleted in the five directories into an array
         print MYFILE "push (\@files, glob( \$_ . '/' .\"".$referralpaths->{'map_id'}. "\" . '*.*') );\n";
      print MYFILE "}\n";
      
      print MYFILE "foreach (\@files) {\n";
          #unlink them
          print MYFILE "print STDOUT \"   Deleting file \".\$_.\"\\n\";\n";
          print MYFILE "unlink \$_ || die \"Cannot remove preexisting \".\$_.\"!\\n\";\n";  
      print MYFILE "}\n";
         
   
   print MYFILE "}\n";
   
   #regardless of whether we ran this job in production before
   #we should now have all of the necessary paths, and they should be empty
   #globstring2 has no path, files are here with us
   
   #get all files that have a dot in them
   print MYFILE "my \$globstring2 = \"*.*\";\n";   
   print MYFILE "my \@mimfiles = glob \$globstring2;\n";
   #need to get all mims and any oddly named supplemental files
   #anything that isnt .pdf, .ps, or .pl, gets shipped off to the mim directory
   print MYFILE "foreach (\@mimfiles) {\n";
      print MYFILE "unless (\$_ =~ m/.pdf\$/i || \$_ =~ m/.ps\$/i || \$_ =~ m/.pl\$/i) {\n";
         print MYFILE "print STDOUT \"   Copying file \".\$_.\" to ".$productionpaths->{'camps_data_mims'}."\\n\";\n";
         print MYFILE "copy(\$_,\"".$productionpaths->{'camps_data_mims'}."\".\"/.\") || die \"Cannot copy mims into \".\"".$productionpaths->{'camps_data_mims'}."\".\"!\\n\";\n";
      print MYFILE "}\n";
   print MYFILE "}\n";
   
   if ($distiller_style ne 'CMD') {
      
      #copy postscripts to mim area first
      print MYFILE "\$globstring2 = \"*.ps\";\n";   
      print MYFILE "my \@psfiles = glob \$globstring2;\n";
      print MYFILE "foreach (\@psfiles) {\n";
         print MYFILE "print STDOUT \"   Copying file \".\$_.\" to ".$productionpaths->{'camps_data_mims'}."\\n\";\n";
         print MYFILE "copy(\$_,\"".$productionpaths->{'camps_data_mims'}."\".\"/.\") || die \"Cannot copy postscripts into \".\"".$productionpaths->{'camps_data_mims'}."\".\"!\\n\";\n";
      print MYFILE "}\n";
      
      #move postscripts to in\
      print MYFILE "foreach (\@psfiles) {\n";
         print MYFILE "print STDOUT \"   Moving file \".\$_.\" to ".$productionpaths->{'camps_distiller_in'}."\\n\";\n";
         print MYFILE "move(\"".$productionpaths->{'camps_data_mims'}."/\".\$_,\"".$productionpaths->{'camps_distiller_in'}."\".\"/.\") || die \"Cannot move postscripts into \".\"".$productionpaths->{'camps_distiller_in'}."\".\"!\\n\";\n";
      print MYFILE "}\n"; 
      
   } else {
      
      #copy pdfs to final pdf dir
      print MYFILE "\$globstring2 = \"*.pdf\";\n";   
      print MYFILE "my \@pdffiles = glob \$globstring2;\n";
      print MYFILE "foreach (\@pdffiles) {\n";
         print MYFILE "print STDOUT \"   Copying file \".\$_.\" to ".$productionpaths->{'camps_data_pdfs'}."\\n\";\n";
         print MYFILE "copy(\$_,\"".$productionpaths->{'camps_data_pdfs'}."\".\"/.\") || die \"Cannot copy pdfs into \".\"".$productionpaths->{'camps_data_pdfs'}."\".\"!\\n\";\n";
      print MYFILE "}\n";
   }
      
   #set umask back
   print MYFILE "umask \$um;\n";      
   
   close (MYFILE);     
   
   if ($returnhash->{'status'} eq 'pass') {
      $returnhash->{'message'} = $returnhash->{'message'}." Completed writing script to ".$stagingdir."/ezreffilemanager.pl\n";      
   }     
   
   return $returnhash;


}


sub scp_staged_files {
   my $superuser          = shift;
   my $superusernode      = shift;
   my $stagingdir         = shift;
   
   #This sub secure copies the staging directory from the home of the script user in a development/referral area
   #to the same user's home on a production machine

   my($returnhash);
   $returnhash->{'status'} = 'pass';
   $returnhash->{'message'} = "";
   
   #need to do this first or else choke on the at sign
   my $outputpath = $superuser."@".$superusernode.".csvd.census.gov:/home/".$superuser;  
   
   #-r is for directory, -p makes the permissions on remote machine -rw-rw-rw-
   eval {
      system("scp -r -p ".$stagingdir." ".$outputpath) 
     }; # eval is a function - not a block
   if ($@) {
     $returnhash->{'status'} = 'fail';
     $returnhash->{'message'} = $returnhash->{'message'}." Secure copy to production machine failed\n";
   }
   
  
   if ($returnhash->{'status'} eq 'pass') {
      $returnhash->{'message'} = $returnhash->{'message'}." Completed copying directory ".$stagingdir." to home/".$superuser." on ".$superusernode."\n";      
   } 
   
   return $returnhash;


}





sub clean_path {
  my $path = shift;
  
  #stole this from camps_prod
  
  if ($path) {
    $path =~ s/\\/\//g;
    $path =~ s/^\s*//;
    $path =~ s/\s*$//;
#   $path =~ s/^\///; # keep root slash
    $path =~ s/\/$//;
  }
  return $path;
}

# Perl trim function to remove whitespace from the start and end of the string
sub trim {
   my $string = shift;
   $string =~ s/^\s+//;
   $string =~ s/\s+$//;
   return $string;
}








1;

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



