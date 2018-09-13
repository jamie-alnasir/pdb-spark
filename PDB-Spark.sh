#!/bin/bash
#//==============================================================================
#// PDB-Spark, Framework for executing legacy applications on the PDB
#// Centre for Systems and Synthetic Biology, Dept of Computer Science
#// Royal Holloway, University of London
#// By Jamie Alnasir, 07/2015 
#// Copyright (c) 2015 Jamie J. Alnasir, All Rights Reserved
#//==============================================================================
#// Version: Bash boot-strap for Apache Spark (YARN mode)
#//==============================================================================

#//------------------------------------------------------------------------------
#// YARN "yarn-client" specific parameters
#// ** Crucial for distribution and optimisation **
_YARN_NUM_EXECUTORS_=5
_YARN_NUM_EXECUTOR_CORES_=3
_YARN_EXECUTOR_MEMORY_=20G
#//------------------------------------------------------------------------------


#//------------------------------------------------------------------------------
# Use _LEGACY_PROGRAM_ to define the Legacy program you wish to parallelise
# using Spark. Provide the filepath to the program and any command-line options
_LEGACY_PROGRAM_="timeout 5m python /home/local/anony/python/Artemis.py";

_HDFS_PDB_FLAT_FILE_="hdfs://bigdata/user/anony/pdb-text-full.txt";
_HDFS_PDB_SPARK_OUTPUT_="hdfs://bigdata/user/anony/pdb-spark-out";
_LOCAL_OUTPUT_="/home/local/anony/PDB-Spark/";


# Temp dir, used to create one if non-existant, should
# also be specified in Python script.
_TEMP_DIR_="/home/local/anony/PDB-Spark/_working/";
#//------------------------------------------------------------------------------


echo "WELCOME TO PDB-SPARK"

#read -r -p "Run job? Previous HDFS data at $_HDFS_PDB_SPARK_OUTPUT_ will be first removed...Are you sure? [y/N] " response
#if [[ $response =~ ^([yY][eE][sS]|[yY])$ ]]
#then

	echo "initiating system bootstrap..."
	echo "removing existing data..."

	# Remove existing PDB-Spark data
	hadoop fs -rm -r $_HDFS_PDB_SPARK_OUTPUT_
	
	# Ensure temp folder exists
	mkdir $_TEMP_DIR_

	# Execute the Map Job to assign key, value pairs to Aligned Reads (SAM) by using
	# GTF annotation to find the discrete regions of interest (Genes/Coding segments)
	echo "excuting the PySpark job [PDB-SPARK] on the cluster..."	
	time spark-submit --master yarn-client --num-executors $_YARN_NUM_EXECUTORS_ --executor-cores $_YARN_NUM_EXECUTOR_CORES_ --executor-memory $_YARN_EXECUTOR_MEMORY_ PDB-Spark.py  $_HDFS_PDB_FLAT_FILE_ $_HDFS_PDB_SPARK_OUTPUT_ $_LEGACY_PROGRAM_

	
	echo "pulling off PDB-Spark results from HDFS into $_TEMP_DIR_"
	time hadoop fs -getmerge $_HDFS_PDB_SPARK_OUTPUT_ $_LOCAL_OUTPUT_/pdb-spark-log.txt

#else
#    echo "Nothing to do here. Bye";
#	exit;
#fi
