#!/bin/bash

source config.txt

if [ $# != 2 ]; then
	echo "Usage: $0 output_file correct_out_file"
	exit 1
fi

output_file=$1
correct_out_file=$2

echo "comparing $output_file vs $correct_out_file"

# sort the file output_file
sort $DATASET/$output_file > $DATASET/${output_file}_tmp.txt
rm $DATASET/$output_file
mv $DATASET/${output_file}_tmp.txt $DATASET/$output_file

# Check existence of file
if [ ! -f $DATASET/$correct_out_file ]; then
	echo -e "\033[1;31mFailed! Correct file $correct_out_file do not exists.\033[0m"
	exit 1
fi



# Compare the result of the Job with the expected result.
num_differences=$(diff $DATASET/$correct_out_file $DATASET/$output_file | wc -l)
#if [ $num_differences != 0 ]; then
if cmp -s $DATASET/$correct_out_file $DATASET/$output_file; then
	echo "No differences between $correct_out_file and $output_file."
else
	echo -e "\033[1;31mFailed to process $base_input !\033[0m"
	echo "Compare $DATASET/$correct_out_file with $DATASET/$output_file for more details."
	exit 1
fi

# Delete the output file since the test is been correctly compleated
rm $DATASET/$output_file
echo "Deleted $DATASET/$output_file"
