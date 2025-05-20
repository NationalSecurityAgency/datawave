#!/bin/bash

export JOB_FILE=$1
export LOG_DIR=$2
export FLAG_DIR=$3
EXTRA_ARGS=${@:4}
export JOB_FILE_BASE=${JOB_FILE%%.inprogress}
export LOG_FILE_BASE=$(basename $JOB_FILE .inprogress)
export LOG_FILE=${LOG_FILE_BASE}.log

CMD=$(head -1 $JOB_FILE | envsubst)
CMD="$CMD $EXTRA_ARGS -flagFile $JOB_FILE"
echo "Executed Command: $CMD" >> $LOG_DIR/$LOG_FILE 2>&1
$CMD >> $LOG_DIR/$LOG_FILE 2>&1
RETURN_CODE=$?
echo "RETURN CODE: $RETURN_CODE" >> $LOG_DIR/$LOG_FILE 2>&1

if [[ $RETURN_CODE == -2 || $RETURN_CODE == 254 ]]; then
  # -2 (which can also be reported as 254) means job was killed, not failed.
  # Rename so that it gets picked up again
  mv $JOB_FILE $JOB_FILE_BASE
elif [[ $RETURN_CODE == -5 || $RETURN_CODE == 251 ]]; then
  # Job was successful, but there were parsing errors

  #### no longer sending email in this case as we are using the processing-errors.sh summary as notification instead
  #### ./send-job-email.sh $JOB_FILE_BASE $LOG_DIR/$LOG_FILE "parse error"

  mv $JOB_FILE $JOB_FILE_BASE.done
  gzip -f $LOG_DIR/$LOG_FILE
elif [[ $RETURN_CODE != 0 ]]; then
  # Rename to failed so we don't retry failed jobs
  mv $JOB_FILE $JOB_FILE_BASE.failed
  ./send-job-email.sh $JOB_FILE_BASE $LOG_DIR/$LOG_FILE "failure" 

  echo "Job return code was: $RETURN_CODE"
else
  mv $JOB_FILE $JOB_FILE_BASE.done
  gzip -f $LOG_DIR/$LOG_FILE
fi
