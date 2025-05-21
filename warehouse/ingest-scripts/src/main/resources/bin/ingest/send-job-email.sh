#!/bin/bash

if [[ $(uname) == "Darwin" ]]; then
	THIS_SCRIPT=$(python -c 'import os,sys;print os.path.realpath(sys.argv[1])' $0)
    MKTEMP="mktemp -t $(basename $0)"
    STATLASTMOD="stat -f %m"	
else
	THIS_SCRIPT=$(readlink -f $0)
    MKTEMP="mktemp -t $(basename $0).XXXXXXXX"
    STATLASTMOD="stat -c %Y"	
fi
THIS_DIR="${THIS_SCRIPT%/*}"
cd $THIS_DIR

# configures RCPT_TO
. ../ingest/ingest-env.sh

JOB=$1
JOB_LOG_FILE=$2
MESSAGE=$3
EXTRA_MESSAGE=

# avoid processing anything or sending anything if this has been disabled
if [[ "$SEND_JOB_EMAIL_DISABLED" == "true" ]]; then
    send_message=0
    processed=1
else
    JOB_CONTENT=$(tail -100 $JOB_LOG_FILE)

    TMPFILE=$($MKTEMP)

    # this pattern is for log likes that contain the date and time at the beginning of lines
    PATTERN='^[0-9][0-9]\/[0-9][0-9]\/[0-9][0-9] [0-9][0-9]:[0-9][0-9]:[0-9][0-9]'

    # we are only comparing the log lines sans the date and time itself and sans INFO lines (should leave WARN and above)
    echo -e "$JOB_CONTENT" | grep -v ' INFO ' | grep -v 'Executed Command: ' | grep -v 'hadoop jar ' | sed "s/$PATTERN\(.*\)/\1/" >> $TMPFILE

    now=$($STATLASTMOD $TMPFILE)

    # We need to lock out any concurrent send-job-email scripts while checking for duplicate messages
    lockfile="/tmp/.sent-job-email.lock"

    send_message=1
    processed=0
fi

# process the message
while [[ "$processed" == "0" ]] ; do
    # attempt the lock
    if ( set -o noclobber; echo "$$" > "$lockfile") 2> /dev/null ; then
	# ensure the lock file is removed if we get interrupted
	trap 'rm -f "$lockfile"; exit $?' INT TERM EXIT


	# determine the closest match out of the previous sent emails
	lines=$(cat $TMPFILE | wc -l)
	best_match=
	best_diffs=
	best_age=
	best_count=
	for file in $(ls -1 /tmp/sent-job-email.* 2> /dev/null); do
	    
	    # ignore file sent over 2 hours ago
	    sent=$($STATLASTMOD $file)
	    if [[ $((now - sent)) -gt 7200 ]]; then
		rm $file
	    else

		# we consider two files matching if they have the same number of lines with 2
		fileLines=$(cat $file | wc -l)
		if [[ $fileLines -gt $((lines - 3 )) && $fileLines -lt $((lines + 3)) ]]; then

		    # and the differences are within 5%
		    diffs=$(diff --ignore-all-space --ignore-blank-lines --ignore-case $TMPFILE $file | egrep '^(<|>) ' | wc -l)
		    # echo "diffs between $file are $diffs out of ($lines + $fileLines)"
		    if [[ $diffs -lt $(( (lines + fileLines ) / 20 )) ]]; then
			if [[ "$best_match" == "" || $diffs -lt $best_diffs ]]; then
			    best_match=$file
			    best_diffs=$diffs
			    best_age=$(( now - sent ))
			    best_count=${file##*.}
			fi
		    fi
		fi
		
	    fi
	    
	done


	if [[ "$best_match" != "" ]]; then

	    rm $TMPFILE
	    EXTRA_MESSAGE="*** This message has been encountered $best_count other times within the past $best_age seconds ***"
	    
	    # if this content is the same as the content sent within the past hour, then suppress unless a new order of magnitude
	    if [[ $best_age -lt 3600 ]]; then
		best_count=$(( best_count + 1 ))
		mv $best_match ${best_match%.*}.$best_count
		if [[ ! ( "$((best_count - 1))" =~ ^10+$ ) ]]; then
		    echo "$EXTRA_MESSAGE ... suppressing email"
		    send_message=0
		fi
	    else
		mv $best_match ${best_match%.*}.1
		# reset its date
		touch ${best_match%.*}.1
	    fi
	    
	else
	    mv $TMPFILE /tmp/sent-job-email.$$.1
	fi
	
	
	rm -f "$lockfile"
	trap - INT TERM EXIT
	processed=1
    fi
done

if [[ "$send_message" == "1" ]]; then
    CONTENT="\
\n\
The ingest job $JOB had issues.\n\
The last 100 lines of the log file $JOB_LOG_FILE follow.\n\n\
$EXTRA_MESSAGE\n\n\
$JOB_CONTENT\n\n\n"

    echo -e "$CONTENT" | mail -s "(U) [INGEST] Job $MESSAGE notification" $RCPT_TO
fi
