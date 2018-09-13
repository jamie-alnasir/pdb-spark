#//==============================================================================
#// PDB-Spark - Python library for executing legacy apps on Brookhaven PDB files
#// Centre for Systems and Synthetic Biology, Dept of Computer Science
#// Royal Holloway, University of London
#// By Jamie Al-Nasir, 07/2015 
#// Copyright (c) 2015 Jamie J. Alnasir, All Rights Reserved
#//==============================================================================
#// Version: Python (PySpark) edition
#//==============================================================================


"""PDB-Spark.py"""
import sys;
import os;
import os.path;
from pyspark import SparkContext
from multiprocessing import Pool


#//------------------------------------------------------------------------------
# These values should be passed on the command line
_HDFS_OUT_ = "hdfs://bigdata/user/anony/pdb-spark-out";
_HDFS_PDB_FLAT_FILE_ = "";

# These values should be set here
_LEGACY_PROGRAM_ = "timeout 4m python /home/local/anony/python/Artemis.py";

# Set to 1 if results are to be stored in Spark output log rather than separate file.
# NB: PDB-Spark-clean.sh should be run on Spark log output files, but is not necessary
# when saving directly to file (_BOOL_SAVE_TO_LOG_=0).
_BOOL_SAVE_TO_LOG_=0;

# ** Must add trailing / **
_TEMP_DIR_ = "/home/local/anony/PDB-Spark/_working/";
#//------------------------------------------------------------------------------

sc = SparkContext("yarn-client", " PDB-Spark");
logf=open('/home/local/anony/PDB-Spark/_working/debug.txt', 'w');


print "WELCOME TO PDB-SPARK!";
logf.write("DEBUG LOG");
lstDebug = [];

def mapper(line):
    global _LEGACY_PROGRAM_;

    _data = line.split("^|");
    pdb_file =  _data[0];
    pdb_file_path = _TEMP_DIR_ + pdb_file;
    
    if os.path.isfile(pdb_file_path) and os.access(pdb_file_path, os.R_OK):
        print "File " + pdb_file_path + " already exists!";
    else:
        print "Extracting: " + pdb_file;
        
    with open(pdb_file_path, 'w') as f:
            for i in range(1, len(_data)):
                f.write(_data[i] + "\n");
                
    # NB: This part needs some work: i.e. wait for legacy program
    # to exit and clean-up extracted pdb_file
    if (_BOOL_SAVE_TO_LOG_):
	strExec = _LEGACY_PROGRAM_ + " " + pdb_file_path;
        pdb_comp_output = os.popen(strExec).read();
        yield (pdb_file, pdb_comp_output);
    else:
	strExec = _LEGACY_PROGRAM_ + " " + pdb_file_path  + " >" + pdb_file_path + ".out";
	os.popen(strExec).read();
        yield (pdb_file);


if (len(sys.argv) > 3):

	global _HDFS_PDB_FLAT_FILE_, _HDFS_OUT_, _LEGACY_PROGRAM_;
	_HDFS_PDB_FLAT_FILE_=sys.argv[1];
	_HDFS_OUT_=sys.argv[2];
	_LEGACY_PROGRAM_=sys.argv[3];
	#_LEGACY_PROGRAM_="python /home/local/anony/python/Artemis.py";

	print "Loading data from HDFS PDB flat file: ", _HDFS_PDB_FLAT_FILE_;
	PDBFiles = sc.textFile(_HDFS_PDB_FLAT_FILE_);
	pdb_spark_output = PDBFiles.flatMap(mapper);

	print "Saving output of Legacy jobs to: ", _HDFS_OUT_;
	pdb_spark_output.saveAsTextFile(_HDFS_OUT_);

	for i in lstDebug:
		logf.write(i);
