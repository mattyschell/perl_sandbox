#!/usr/bin/perl

#Matt! 10/21/10
#Copy a topology from one database to another, hopefully catch any hiccups up front
#usage: gz_exp_imp_topo.pl -s <from schema> -p <from password> -d <from database> -t <topology> -S <to schema> -P <to password> -D <to database> -T <to topology> -f <path for dump (f)iles and any logs> -e <[optional] exp/imp only>
#ex full exp and imp:    gz_exp_imp_topo.pl -s GZCPB_1 -p xxxx -d prodbnch -t CHEDGE7 -S GZCPB1 -P xxxx -D devbench -f /home/schel010/
#ex exp only:            

#3/07/11 Updated for user suggestions on privileges and ownership, copy to new topology name, and cleaned up list of tables exported
#4/12/11 Updated for 11g imports, adding toid_novalidate and special trailing zeroes JDBC bug fix call
#5/25/11 Added toid_novalidate for all imports.  Darn MT_VARCHAR2_COLLECTION		
#6/23/11 Removed grants from import due to constant barrage of warnings and added explict grant	select to public on all tables
#        Also added check that each table gets imported before running topo initialization
#9/29/11 Attempt to purge temp topology on from database on failure.  Make topology inputs case insensitive
#10/13/11 Added post import call to gz_topo_util.fix_relation_dollar_indexes 
#01/18/12 Added cleanup of exp$ table on fromschema.  Prevents problems on copy back (like prod-->dev-->prod)
#02/10/12 Added escape syntax on imp and exp for special characters in password
#03/22/12 Added options to only exp or only imp

use strict;
use warnings;
use Getopt::Long qw(:config no_ignore_case bundling);
use gz_dbihelpers;

=head1 NAME

  gz_exp_imp_topo.pl

  created: 10/21/2010
  last modified (unlikely): 10/21/2010

=head1 SYNOPSIS

  Attempts to copy a topology from one database to another, hopefully catch any hiccups up front

=cut

my $debug = 0;


my (
  $opt_fromschema,
  $opt_frompassword,
  $opt_fromdatabase,
  $opt_topology,
  $opt_toschema,
  $opt_topassword,
  $opt_todatabase,
  $opt_totopology,
  $opt_filepath,
  $opt_expimp,
);

my $rc = GetOptions(
   'schemafrom|s=s'          => \$opt_fromschema,
   'passwordfrom|p=s'        => \$opt_frompassword,
   'databasefrom|d=s'        => \$opt_fromdatabase,
   'topology|t=s'            => \$opt_topology,
   'schemato|S=s'            => \$opt_toschema,
   'passwordto|P=s'          => \$opt_topassword,
   'databaseto|D=s'          => \$opt_todatabase,
   'topologyto|T=s'          => \$opt_totopology,
   'filepath|f=s'            => \$opt_filepath,
   'expimp|e=s'              => \$opt_expimp,
    
);

# display usage if options not entered...

my $usage = "\nUSAGE: gz_exp_imp_topo.pl -s <from schema> -p <from password> -d <from database> -t <topology> -S <to schema> -P <to password> -D <to database> -T <[optional] to topology> -f <path for dump (f)iles and any logs> -e <[optional] exp/imp only>\n\n";

if (defined ($opt_expimp) ) {
   #only exp or only imp
   
   if ($opt_expimp eq 'exp') {
      
      unless ($opt_fromschema && $opt_frompassword && $opt_fromdatabase && $opt_topology && $opt_filepath) {
         print STDOUT "$usage";
         die "Missing arguments for export only, check the usage\n\n";
      }
      $opt_toschema = 'NULL';
      
   } elsif ($opt_expimp eq 'imp') {
      
      unless ($opt_toschema && $opt_topassword && $opt_todatabase && $opt_topology && $opt_filepath) {
         print STDOUT "$usage";
         die "Missing arguments for export only, check the usage\n\n";
      }
      $opt_fromschema = 'NULL';
   }
      
} else {
   
   #SOP exp and imp
   
   unless ($opt_fromschema && $opt_frompassword && $opt_fromdatabase && $opt_topology && $opt_toschema && $opt_topassword && $opt_todatabase && $opt_filepath) {
      print STDOUT "$usage";
      die "Missing arguments, check the usage\n\n";
   } 

}

#set up exp and or imp if user just wants one
my $exp_andor_imp;

if (defined ($opt_expimp)) {
   
   unless ($opt_expimp eq 'exp' || $opt_expimp eq 'imp') {
      
         print STDOUT "$usage";
         die "Sorry, I dont know what to do with the -e argument -->".$opt_expimp."<--\n\n";
   }
   
   $exp_andor_imp = $opt_expimp;
   
} else {
   
   $exp_andor_imp = 'expimp';

}
   
# set up destination topology  

unless ($opt_totopology) {
   
   $opt_totopology = $opt_topology;

}

###############################
#Shared variable setup
###############################

#upper case
$opt_totopology = uc($opt_totopology);
$opt_topology = uc($opt_topology);

my $dbhfrom; 
my $dbhto;  
my $connection;


my $sql;
my $sth;

#set these to be not equal, in case we are just exping or just imping
my $fromversion = '999';
my $toversion = '998';

my $table_commas = "";
my @table_list;

#################################
#################################

if ($exp_andor_imp =~ 'exp') {
   

   print STDOUT "   Getting a database connection to ".$opt_fromschema."@".$opt_fromdatabase."\n";
   
  
   $connection = [$opt_fromdatabase,$opt_fromschema,\$opt_frompassword]; #password will be dereferenced in get_dbh   
                                                                         #too funny, since its passed in at the cmd line and printed to the screen  
   
   eval {
         $dbhfrom = get_dbh($connection); 
        };
       
       if ($@) {
          print STDOUT "\n";
          print STDOUT "login failure\n";
          print STDOUT "Heres the error we caught:\n";
          print STDOUT "\n";
          print STDOUT $@;
          exit;
       }
    
}
    
if ($exp_andor_imp =~ 'imp') {
   
   print STDOUT "   Getting a database connection to ".$opt_toschema."@".$opt_todatabase."\n";
    
   $connection = [$opt_todatabase,$opt_toschema,\$opt_topassword]; 
                                                                       
   
   eval {
         $dbhto = get_dbh($connection); 
        };
       
       if ($@) {
          print STDOUT "\n";
          print STDOUT "login failure\n";
          print STDOUT "Heres the error we caught:\n";
          print STDOUT "\n";
          print STDOUT $@;
          
          $dbhfrom->disconnect;  
          exit;
       }
       
}
   
######################## 
###CHECKS CHECKS CHECKS
########################




#Check 1
#Check that TO database does not already have a topology named this

if ($exp_andor_imp =~ 'imp') {

print STDOUT "   Checking that topology ".$opt_totopology." does not already exist in schema ".$opt_toschema."@".$opt_todatabase."\n";

   $sql ="SELECT count(*) from user_sdo_topo_info ";
   $sql.="WHERE topology = :p1 ";
   $sth = $dbhto->prepare($sql);
   $sth->bind_param( 1, $opt_totopology);
   $sth->execute;
   $sth->bind_columns(\my($kount));
   $sth->fetch();
   
   if ($kount > 0) {
      print STDOUT "Looks like topology ".$opt_totopology." already exists in ".$opt_toschema."@".$opt_todatabase."\n";
      print STDOUT "Best to copy ".$opt_totopology." to a new name, or purge the topology from ".$opt_toschema."@".$opt_todatabase."\n";
      print STDOUT "Bye!\n";
      
      $sth->finish;
      $dbhfrom->disconnect;    
      $dbhto->disconnect; 
      exit;
   }

}

#Check 2
#Take a stab at the relation$ partition mess
#Maybe this check should be that its the default tablespace for the user?
#I dont really understand how this works. Does it just have to exist?  Thats what this checks, thats it

if ($exp_andor_imp =~ 'imp' && $exp_andor_imp =~ 'exp') {
   
   $sql ="SELECT def_tablespace_name from user_part_tables ";
   $sql.="WHERE table_name = :p1 ";
   $sth = $dbhfrom->prepare($sql);
   $sth->bind_param( 1, $opt_topology."_RELATION\$");
   $sth->execute;
   $sth->bind_columns(\my($tablespace));
   $sth->fetch();
   
   
   
   print STDOUT "   Checking that ".$opt_topology."_RELATION\$ tablespace ".$tablespace." is available on ".$opt_todatabase." to user ".$opt_toschema."\n";
   
   #Not 100 pct robust, but all users should have access to user_tablespaces
   
   $sql ="SELECT count(*) from user_tablespaces ";
   $sql.="WHERE tablespace_name = :p1 ";
   $sth = $dbhto->prepare($sql);
   $sth->bind_param( 1, $tablespace);
   $sth->execute;
   $sth->bind_columns(\my($tbkount));
   $sth->fetch();
       
   if ($tbkount != 1) {
      
      print STDOUT "I dont see tablespace ".$tablespace." on ".$opt_todatabase." available to ".$opt_toschema."\n";
      print STDOUT "Best to get it made first or do this process by hand.\n";
      print STDOUT "Bye!\n";
      
      $sth->finish;
      $dbhfrom->disconnect;    
      $dbhto->disconnect; 
      exit;
   }
}

# Check 3
# Check that path is valid

unless (-d $opt_filepath ) {
   print STDOUT "Looks like path ".$opt_filepath." does not exist\n";
   print STDOUT "Bye!\n";
   
   $sth->finish;
   $dbhfrom->disconnect;    
   $dbhto->disconnect; 
   exit;
}


# Check 4 make sure .dmp is there if imp only

if ( ($exp_andor_imp =~ 'imp') &&  ($exp_andor_imp !~ 'exp') ) {
   
   unless (-e $opt_filepath.$opt_topology.".dmp" ) {
      
      print STDOUT "Looks like dmp file ".$opt_filepath.$opt_topology.".dmp does not exist\n";
      print STDOUT "Bye!\n\n";
      
      $sth->finish;
    
      $dbhto->disconnect; 
      exit;
   
   }
}


# Check 5
# Compare DB versions

#Thought about writing a function to call DBMS_DB_VERSION.VERSION
#But I think I want these checks to at least run on installations 
#without any GZ code installed

if ($exp_andor_imp =~ 'imp' && $exp_andor_imp =~ 'exp') {
   
   $sql ="SELECT SUBSTR(a.banner,(instr(a.banner,'g') - 2),2) ";
   $sql.="FROM v\$version a ";
   $sql.="WHERE a.banner like :p1 ";
   $sth = $dbhfrom->prepare($sql);
   $sth->bind_param( 1, 'Oracle%');
   $sth->execute;
   $sth->bind_columns(\($fromversion));
   $sth->fetch();
   
   
   $sql ="SELECT SUBSTR(a.banner,(instr(a.banner,'g') - 2),2) ";
   $sql.="FROM v\$version a ";
   $sql.="WHERE a.banner like :p1 ";
   $sth = $dbhto->prepare($sql);
   $sth->bind_param( 1, 'Oracle%');
   $sth->execute;
   $sth->bind_columns(\($toversion));
   $sth->fetch();
   
}



##################
#Done with Checks
##################


########
#Step 0: Copy to new name if desired
#after this, only use $opt_totopology variable until cleanup
########

if ($exp_andor_imp =~ 'exp') {
   
   if ($opt_topology ne $opt_totopology) {
      
      $sql ="SELECT count(*) from user_sdo_topo_info ";
      $sql.="WHERE topology = :p1 ";
      $sth = $dbhfrom->prepare($sql);
      $sth->bind_param( 1, $opt_totopology);
      $sth->execute;
      $sth->bind_columns(\my($kount));
      $sth->fetch();
      
      if ($kount > 1) {
         
         print STDOUT "Looks like temp topology ".$opt_totopology." already exists in ".$opt_fromschema."@".$opt_fromdatabase."\n";
         print STDOUT "Best to purge ".$opt_totopology." manually and rerun\n";
         print STDOUT "Bye!\n";
      
         $sth->finish;
         $dbhfrom->disconnect;    
         $dbhto->disconnect; 
         exit;
         
      }
      
      print STDOUT "   Creating temp copy of ".$opt_topology." to ".$opt_totopology." before running export \n";
    
      $sql ="BEGIN ";
      $sql.="GZ_TOPO_UTIL.COPY_TOPOLOGY(:p1,:p2,:p3,:p4,:p5,:p6); ";
      $sql.="END;";
      $sth = $dbhfrom->prepare($sql);
      $sth->bind_param( 1, $opt_fromschema);
      $sth->bind_param( 2, $opt_topology);
      $sth->bind_param( 3, $opt_fromschema);
      $sth->bind_param( 4, $opt_totopology);
      $sth->bind_param( 5, 'N');              #Do not purge if it exists
      $sth->bind_param( 6, 'N');              #Skip validation
      
      eval { $sth->execute; };
         if ($@) {
            
            print STDOUT "\nTemp copy of ".$opt_topology." to ".$opt_totopology." failed\n";
            print STDOUT "Heres the copy error:\n\n";
            print STDOUT $@."\n";
            
            print STDOUT "\   going to try to purge ".$opt_totopology."\n";
            
            $sql ="BEGIN ";
            $sql.="GZ_TOPO_UTIL.PURGE_TOPOLOGY(:p1, :p2); ";
            $sql.="END;";
            $sth = $dbhfrom->prepare($sql);
            $sth->bind_param( 1, $opt_fromschema);
            $sth->bind_param( 2, $opt_totopology);
            $sth->execute;
            
            print STDOUT "Bye!\n";
            
            
            
            $dbhfrom->disconnect;    
            $dbhto->disconnect; 
            exit;
                
         }    
   }




   ########
   #Step 1: prepare for export
   ########
   
   print STDOUT "   Running prepare_for_export\n";
   
   $sql ="BEGIN ";
   $sql.="SDO_TOPO.PREPARE_FOR_EXPORT(:p1); ";
   $sql.="END;";
   $sth = $dbhfrom->prepare($sql);
   $sth->bind_param( 1, $opt_totopology);
   $sth->execute;

   ########
   #Step 2: Get table names to export
   ########
   
   
   
   $sql ="SELECT table_name FROM user_sdo_topo_info ";
   $sql.="WHERE topology = :p1 ";
   $sth = $dbhfrom->prepare($sql);
   $sth->bind_param( 1, $opt_totopology);
   $sth->execute;
   $sth->bind_columns(\my ($table_name));
   while ( $sth->fetch() ) {
      push(@table_list,$table_name);
   }
   
   #Add the dollar tables
   push(@table_list, $opt_totopology."_EDGE\$");
   push(@table_list, $opt_totopology."_FACE\$");
   push(@table_list, $opt_totopology."_HISTORY\$");
   push(@table_list, $opt_totopology."_NODE\$");
   push(@table_list, $opt_totopology."_RELATION\$");
   push(@table_list, $opt_totopology."_EXP\$");
    
   
   my $lamekount = 0;
   
   foreach my $tab (@table_list) {
      $lamekount++;
      
      if ($lamekount != scalar(@table_list)) {      
         $table_commas = $table_commas.$tab.",";
      } else {
         $table_commas = $table_commas.$tab;
      }
      
   }

   #print STDOUT "   We will export these tables: ".$table_commas."\n";
   
   #######
   #Step3: Call exp
   #######
   
   my $exp_stmt;
   
   $exp_stmt = "exp \\'".$opt_fromschema."/".$opt_frompassword."@".$opt_fromdatabase."\\' FILE=".$opt_filepath.$opt_totopology.".dmp ";
   $exp_stmt.= "indexes=N statistics=none tables=(".$table_commas.")";
   
   
   print STDOUT "   Heres a printout of the exp we are about to send to the system:\n\n";
   print STDOUT "      ".$exp_stmt."\n\n";
   
   eval {
      system ($exp_stmt);
       };
       
   if ($@) {
      print STDOUT "Looks like export failed and I dont know how to recover.  Sorry bub\n";
      exit;
   }
   
}  # end EXP section


#######
#Step4: Call imp
#######

if ($exp_andor_imp =~ 'imp') {
   
   my $imp_stmt;
   
   print STDOUT "\n";
   print STDOUT "   Now we will call import on the same tables\n";
   
      $imp_stmt = "imp \\'".$opt_toschema."/".$opt_topassword."@".$opt_todatabase."\\' FILE=".$opt_filepath.$opt_totopology.".dmp ";
      
   
   #add toid_novalidate if 10g to 11g. To be safe
   #though this issue seems to have resolved itself on one of one test 11g databases
   if ($fromversion ne $toversion) {
      
      $imp_stmt.= "TOID_NOVALIDATE=MAFTIGER.MT_VARCHAR2_COLLECTION,MDSYS.SDO_LIST_TYPE,MDSYS.SDO_GEOMETRY,MDSYS.SDO_ELEM_INFO_ARRAY,MDSYS.SDO_ORDINATE_ARRAY,MDSYS.SDO_TOPO_GEOMETRY ";	
      
   } else {
   
     #F it, always add toid_novalidate for the wacky MT_VARCHAR2_COLLECTION. 
     #if not present will generate imp warning--  IMP-00086: TOID "MT_VARCHAR2_COLLECTION" not found in export file    
     $imp_stmt.= "TOID_NOVALIDATE=MAFTIGER.MT_VARCHAR2_COLLECTION ";
   }
   
   $imp_stmt.= "indexes=N ignore=Y grants=N ";
   
   if ($exp_andor_imp =~ 'exp') {
      #we just exported this file ourselves and know the contents
      $imp_stmt.= "tables=(".$table_commas.")";
   } else {
      $imp_stmt.= "tables=('%')";
   }
   
   print STDOUT "   Heres a printout of the imp we are about to send to the system:\n\n";
   print STDOUT "      ".$imp_stmt."\n\n";
   
   
   eval {
      system ($imp_stmt);
       };
       
   if ($@) {
      print STDOUT "Looks like export failed and I dont know how to recover.  Sorry bub\n";
      exit;
   }
      
   #give us some space from the imp garbage
   print STDOUT "\n\n";
   
   #check that all tables imported
   #otherwise we get weird nonsensical errors on topo initialization
   #also grant select to public so no matter what happens next, its debuggable to a second eyeset
   
   foreach my $granttab (@table_list) {
      
      $sql ="SELECT COUNT(*) ";
      $sql.="FROM user_tables a ";
      $sql.="WHERE a.table_name = :p1 ";
      $sth = $dbhto->prepare($sql);
      $sth->bind_param( 1, $granttab);
      $sth->execute;
      $sth->bind_columns(\my($tabkounter));
      $sth->fetch();
      
      
      if ($tabkounter == 0) {
         print STDOUT "\n";
         print STDOUT "Looks like table ".$granttab." didnt get imported.  Going to quit before initializing topology, otherwise theres a mess.\n";
         print STDOUT "PS: Check the log above here\n";
         die "Peace Out\n";
      }
      
      $sql ="GRANT SELECT ON ".$granttab." TO PUBLIC ";
      $sth = $dbhto->prepare($sql);
      $sth->execute;
      
   }
   
   ########
   #step 5: update exp$ table if necessary
   ########
   
   if ($opt_fromschema ne $opt_toschema) {
      
      print STDOUT "   updating exp\$ table to owner ".$opt_toschema."\n";
      
      $sql ="UPDATE ".$opt_totopology."_exp\$ ";
      $sql.="set owner = :p1, ";
      $sql.="table_schema = :p2 ";
      $sth = $dbhto->prepare($sql);
      $sth->bind_param( 1, $opt_toschema);
      $sth->bind_param( 2, $opt_toschema);
      $sth->execute;
      $dbhto->commit();
      
   } else {
      
      print STDOUT "   skipping exp\$ update, schema names match\n";
      
   }
   
   ########
   #step 6: run initialize after import and various tidying
   ########
   
   print STDOUT "   executing initialize_after_import\n";
   
   $sql ="BEGIN ";
   $sql.="SDO_TOPO.INITIALIZE_AFTER_IMPORT(:p1); ";
   $sql.="END;";
   $sth = $dbhto->prepare($sql);
   $sth->bind_param( 1, $opt_totopology);
   $sth->execute;
   
   print STDOUT "   also running GZ_TOPO_UTIL.FIX_RELATION_DOLLAR_INDEXES\n";
   
   #Work around various bugs
   $sql ="BEGIN ";
   $sql.="GZ_TOPO_UTIL.FIX_RELATION_DOLLAR_INDEXES(:p1); ";
   $sql.="END;";
   $sth = $dbhto->prepare($sql);
   $sth->bind_param( 1, $opt_totopology);
   $sth->execute;
   
} #end big if IMP section

if ($exp_andor_imp =~ 'exp') {
   
   #Unlike destination schema, this doesnt get cleaned up from the donor schema. 
   #Creates problems on copy backs, like prod-->dev-->prod
   print STDOUT "   also dropping ".$opt_totopology."_EXP\$ table from donor schema on ".$opt_fromdatabase."\n";
   
   $sql ="DROP TABLE ".$opt_totopology."_EXP\$ ";
   $sth = $dbhfrom->prepare($sql);
   $sth->execute;
   
}


########
#step 7: check that exp$ is gone
########

if ($exp_andor_imp =~ 'imp') {
   
   $sql ="SELECT count(*) FROM user_tables ";
   $sql.="WHERE table_name = :p1 ";
   $sth = $dbhto->prepare($sql);
   $sth->bind_param( 1, $opt_totopology."_EXP\$");
   $sth->execute;
   $sth->bind_columns(\my($expkount));
   $sth->fetch();   
   
   if ($expkount == 0) {
      
      print STDOUT "   ".$opt_totopology."_EXP\$ table is gone from destination schema on ".$opt_todatabase.", this appears to be a total success!\n";
      
      #clean up 10g to 11g quote-unquote trailing zeroes JDBC bug, if possible
   
      if (($fromversion ne $toversion) 
          && ($toversion eq '11')) {
      	   
         #First, check that to user has code base installed
         #kinda wishy washy on whether this is a requirement
         $sql ="SELECT count(*) FROM user_objects ";
         $sql.="WHERE object_name = :p1 AND ";
         $sql.="object_type = :p2 ";
         $sth = $dbhto->prepare($sql);
         $sth->bind_param( 1, 'GZ_UTILITIES');
         $sth->bind_param( 2, 'PACKAGE');
         $sth->execute;
         $sth->bind_columns(\my($pkkount));
         $sth->fetch(); 
         
         if ($pkkount == 1) {
            
             print STDOUT "   Calling GZ_UTILITIES.IMPORT_SDOTOPO11G to fix trailing zeroes JDBC bug\n";
            
             $sql ="BEGIN ";
             $sql.="GZ_UTILITIES.IMPORT_SDOTOPO11G(:p1,:p2); ";
             $sql.="END; ";
             $sth = $dbhto->prepare($sql);
             $sth->bind_param( 1, $opt_totopology);
             $sth->bind_param( 2, 'Y');
             $sth->execute;
            
         } else {
         
            print STDOUT "\n";
            print STDOUT "   *****WARNING***** \n";
            print STDOUT "      Database copied from is oracle version ".$fromversion."\n";
            print STDOUT "      Database copied to is oracle version ".$toversion."\n";
            print STDOUT "      Ordinarily we would run a bug fix procedure in GZ_Utilities, but the package isnt installed on ".$opt_toschema."@".$opt_todatabase."\n"; 
            print STDOUT "   *****WARNING***** \n\n";
            
         }
         
      }
      
   } else {
      
      print STDOUT "   Uh oh, ".$opt_totopology."_EXP\$ table still exists in ".$opt_toschema."\n";
      print STDOUT "   Better investigate, something probably went wrong.\n";
      
      $sth->finish;
      $dbhfrom->disconnect;    
      $dbhto->disconnect; 
   
      exit 1;
      
   }

}
    
#clean up temp topo if we made it
if (($opt_topology ne $opt_totopology) && ($exp_andor_imp =~ 'exp')) {
   
   print STDOUT "   Purging temporary topology " .$opt_totopology. " in ". $opt_fromschema."@".$opt_fromdatabase."\n\n";
    
   $sql ="BEGIN ";
   $sql.="GZ_TOPO_UTIL.PURGE_TOPOLOGY(:p1,:p2); ";
   $sql.="END;";
   $sth = $dbhfrom->prepare($sql);
   $sth->bind_param( 1, $opt_fromschema);
   $sth->bind_param( 2, $opt_totopology);
   $sth->execute;
    
        
}



    
#print STDOUT "Peace Out\n";
print STDOUT "\n";
print STDOUT "     .\"\".    .\"\",    \n";
print STDOUT "     |  |   /  /         \n";
print STDOUT "     |  |  /  /          \n";
print STDOUT "     |  | /  /           \n";
print STDOUT "     |  |/  ;-._         \n";
print STDOUT "     }  ` _/  / ;        \n";
print STDOUT "     |  /` ) /  /        \n";
print STDOUT "     | /  /_/\_/\        \n";
print STDOUT "     |/  /      |        \n";
print STDOUT "     (  ' \ '-  |        \n";
print STDOUT "      \    `.  /         \n";
print STDOUT "       |      |          \n";
print STDOUT "       |      |          \n";


$sth->finish;

if ($exp_andor_imp =~ 'exp') {
   $dbhfrom->disconnect;    
}
if ($exp_andor_imp =~ 'imp') {
   $dbhto->disconnect; 
}

exit 1;













