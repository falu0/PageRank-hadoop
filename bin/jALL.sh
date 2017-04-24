#!/bin/bash

source config.txt
jobs=("Job1ParseGraphDriver" "Job2CalculateRankDriver" "Job3CheckConvergenceDriver" "Job4SortRankDriver")

echo ${jobs[*]}

for in in $DATASET/datainJ1_*
do

	# Skip garbage
	if [ "${in: -1}" == "~" ]; then
		continue
	fi

	basein="$(basename $in)"

	############### Job1ParseGraphDriver ###############

	job=${jobs[0]}
	echo "job: $job"
	
	input=$basein
	echo "input: $input"
	output="out_$input"
	echo "output: $output"

	echo "Processing ..."

	# Insert $input in the hadoop distibuted file system
	$HADOOP fs -put $in $input
	echo "Added /user/$USER/$input in hdfs"

	# Start the Job and check if it is completed correctly
	echo "$job Job started!"
	result=$($HADOOP jar $JAR_PATH pad.luchetti.pagerank.$job $input $output true 2>&1)
	if [[ "$result" != *"Job complete"* ]]; then
		echo -e "\033[1;31mError in $job Job:\033[0m"; echo $result; exit 1
	else
		echo "$job Job completed correctly!"
	fi

	############### Job2CalculateRankDriver ###############

	job=${jobs[1]}
	echo "job: $job"

	input=$output
	echo "input: $input"
	output="out_$input"
	echo "output: $output"
	outputj2=$output

	echo "Processing ..."

	# Start the Job and check if it is completed correctly
	echo "$job Job started!"
	result=$($HADOOP jar $JAR_PATH pad.luchetti.pagerank.$job $input $output true 2>&1)
	if [[ "$result" != *"Job complete"* ]]; then
		echo -e "\033[1;31mError in $job Job:\033[0m"; echo $result; exit 1
	else
		echo "$job Job completed correctly!"
	fi

	############### Job3CheckConvergenceDriver ###############

	job=${jobs[2]}
	echo "job: $job"

	input=$outputj2
	echo "input: $input"
	output="out-j3_$input"
	echo "output: $output"

	echo "Processing ..."

	# Start the Job and check if it is completed correctly
	echo "$job Job started!"
	result=$($HADOOP jar $JAR_PATH pad.luchetti.pagerank.$job $input $output true 2>&1)
	if [[ "$result" != *"Job complete"* ]]; then
		echo -e "\033[1;31mError in $job Job:\033[0m"; echo $result; exit 1
	else
		echo "$job Job completed correctly!"
	fi

	echo "Comparing ..."

	# Merge the results of the Job and copy the output file locally
	output_file="$output.txt"
	echo "output_file: $output_file"
	$HADOOP fs -getmerge $output $DATASET/$output_file


	correct_out_file="${basein/datainJ1/dataoutJ3}"
	echo "correct_out_file: $correct_out_file"

	$WORKING_DIR/bin/compare_result.sh $output_file $correct_out_file
	if [ $? != 0 ]; then
		exit 1
	fi
	echo -e "\033[1;92mTest on $input completed correctly!\033[0m"

	############### Job4SortRankDriver ###############

	job=${jobs[3]}
	echo "job: $job"

	input=$outputj2
	echo "input: $input"
	output="out-j4_$input"
	echo "output: $output"

	echo "Processing ..."

	# Start the Job and check if it is completed correctly
	echo "$job Job started!"
	result=$($HADOOP jar $JAR_PATH pad.luchetti.pagerank.$job $input $output true 2>&1)
	if [[ "$result" != *"Job complete"* ]]; then
		echo -e "\033[1;31mError in $job Job:\033[0m"; echo $result; exit 1
	else
		echo "$job Job completed correctly!"
	fi

	echo "Comparing ..."

	# Merge the results of the Job and copy the output file locally
	output_file="$output.txt"
	echo "output_file: $output_file"
	$HADOOP fs -getmerge $output $DATASET/$output_file
	
	# Clean file on hadoop
	$HADOOP fs -rmr datainJ*
	$HADOOP fs -rmr out*

	correct_out_file="${basein/datainJ1/dataoutJ4}"
	echo "correct_out_file: $correct_out_file"

	$WORKING_DIR/bin/compare_result.sh $output_file $correct_out_file
	if [ $? != 0 ]; then
		exit 1
	fi
	echo -e "\033[1;92mTest on $input completed correctly!\033[0m"


done
