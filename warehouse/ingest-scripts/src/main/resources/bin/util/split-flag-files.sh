
usage()
{
    echo "Usage: [A-Za-z\/]* [0-9]* [A-Za-z\/]*"
    echo "  The first argument is the input path"
    echo "  The second argument is the split point of the flag files"
    echo "  The third argument is the directory to store the resulting files"
    exit 1
}

INDIR="."
if [ ! -z "$1" ]; then
        if [ "$1" == "--help" ]; then
                usage
        fi
        INDIR="$1"
fi

SPLIT=900
if [ ! -z "$2" ]; then
	SPLIT="$2"
fi

OUTDIR=`mktemp -d`
if [ ! -z "$3" ]; then
        OUTDIR="$3"
	mkdir -p $OUTDIR 2> /dev/null
else
	echo "Output directory will be ${OUTDIR}"
fi

for i in `find  ${INDIR} -maxdepth 1 -type f -name '*bulk*.flag.failed'`
do
	echo $i 
	SCRIPT=`cat $i | sed -nr 's/([A-Za-z0-9\/]+.sh) (.*)/\1/p'`
	SLEN=${#SCRIPT}
	SLEN=`expr $SLEN + 2`
	echo $SLEN 
	COMM="cat $i | cut -c $SLEN-"
	RES=`eval $COMM`
	FILESs=`echo $RES | sed -nr 's/([^ ]+) ([0-9]+ .*)/\1/p'`
	FLEN=${#FILESs}
	SLEN=`expr $FLEN + 2`
	COMM="echo $RES | cut -c $SLEN-"
	OARGS=`eval $COMM`
	echo "args $OARGS"
	NUMBER=`echo $FILESs | tr ',' '\n' | wc -l`
	COUNT=0
	FILE_NUM=1
	NAME=`basename $i | sed -nr 's/([^ ]+)\.flag\.failed/\1/p'`
	CURR=""
	echo $FILESs >> f
	for j in `echo $FILESs | tr ',' '\n'`
	do
		((COUNT++))
		ISREP=`expr $COUNT % $SPLIT`
		if [ $ISREP -eq 0 ]; then
			FILEWRITE="${NAME}_${COUNT}_${FILE_NUM}.flag.failed"
			echo "write ${COUNT} to ${OUTDIR}/${FILEWRITE}"
			echo "$SCRIPT $CURR,$j $OARGS" > $OUTDIR/$FILEWRITE
			((FILE_NUM++))
			COUNT=0
			CURR=""
		else
			if [[ -z "$CURR" ]]; then
				CURR="$j"
			else
				CURR="$CURR,$j"
			fi
		fi
#		if (
	done	
	if [[ -z "$CURR" ]]; then
                                echo "all written"
        else
		FILEWRITE="${NAME}_${COUNT}_${FILE_NUM}.flag.failed"			
		echo "write ${COUNT} to ${OUTDIR}/${FILEWRITE}"
		echo "$SCRIPT $CURR $OARGS" > $OUTDIR/$FILEWRITE
               ((FILE_NUM++))
               CURR=""
       fi

done
