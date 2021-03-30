#!/bin/bash

if [ "$#" -ne 3 ]; then
    echo -e  "Usage: ./eventsTrimmer.sh  <file path of spark event log file> <last percentage of required Jobs > <number of threads available for trimming>"
    echo -e "eg: ./eventsTrimmer.sh ~/Downloads/application_1605334641754_0001 40 8"
    echo -e "This script does not create an intermediate file, please make a copy before submitting."
    exit 0
fi

filename=$1
percentageToBeTrimmed=100-$2
threads=$3

if ! command -v rg &> /dev/null
then
    echo "RIPGREP COMMAND could not be found. RG is used for speed reasons"
    echo "How to install RIPGREP: https://github.com/BurntSushi/ripgrep#installation"
    exit
fi

# numLines has the line numbers on the event log file, for each SparkListenerJobStart. SparkListenerJobStart defines the init of a JobID 
# LC_ALL=C, fixed strings and RIPGREP with higher number of threads makes it faster.
numLines=($(LC_ALL=C rg -j $threads -n -F SparkListenerJobStart $filename| cut -f1 -d:))
# declare -p prints the array content
# numLinesLength, tells how large is the list of jobs ids, the size of the array.
numLinesLength=${#numLines[@]}
echo "Number of existing jobs: "$numLinesLength
# jobEndToDelete, calculates aproximately what would be the oldest max jobId to be deleted, starting from jobEndToDelete + 1, all jobs should be preserved.
let "lastNumLinesIndexToDelete=( numLinesLength * percentageToBeTrimmed / 100 )"
echo "Will delete everything up to the jobID at line#:"${numLines[lastNumLinesIndexToDelete]}
jobIDNumber=($(sed -n "${numLines[lastNumLinesIndexToDelete]}p" $filename |cut -d: -f3|cut -d, -f1))
echo "JobID is::: "$jobIDNumber
if [[ "$lastNumLinesIndexToDelete" -lt 1 ]]; then
    echo "Number of resulting jobs left after trimming= "$lastNumLinesIndexToDelete
    echo $percentageToBeTrimmed"% of "$numLinesLength" "
    echo "This script is not yet optimized for low number of jobs."
    echo "The number of Jobs * required percentage, is equal or lower than 1."
    echo $numLinesLength " * " $percentageToBeTrimmed "% = " $lastNumLinesIndexToDelete 
fi

# jobStartLineNumber, is the event log line number where the first Job is initialized.
jobStartLineNumber=${numLines[0]}
# jobEndLineNumber, gets the event log line number up to which we should delete everything, this is the jobEnd line. If we want to delete 25% of 20 jobs, then this would be SparkListenerJobEnd, Job ID 5.
jobEndLineNumber=${numLines[lastNumLinesIndexToDelete - 1]}
echo "Deleting lines from #"$numLines" until "$jobEndLineNumber

#firstJob=$(sed -n "${numLines}p" $filename |cut -d: -f3|cut -d, -f1)
#lastJob=$(sed -n "${jobEndLineNumber}p" $filename |cut -d: -f3|cut -d, -f1)

# This will delete all lines INPLACE
sed -i '' "$jobStartLineNumber,$jobEndLineNumber d" $filename
numLinesPost=($(LC_ALL=C rg -j $threads --count -n -F SparkListenerJobStart $filename))
echo "Number of Jobs Pre Processing:"$numLinesLength
echo "Number of Jobs Post Processing:"$numLinesPost
