#!/bin/bash
set -e
DEFAULT_AGGR_FILE=_wip.csv

UPLOAD_TO_BQ=0
PROJECT=at-data-science-research
BQ_TABLE=at-data-science-research:MediaMath_Logs.

usage() {
  echo
  echo "Usage: $0 gs://[path/filename] [ -t TYPE ] [ hbd ]" 1>&2
}

exit_abnormal() {
  usage
  exit 1
}

help() {
  echo "Usage: $0 [ -f complete path, ex.: gs://dir/filename.txt ] [ -t type ] [ hbd ]"
  echo
  echo "       -f MANDATORY parameter, path points to GCP Cloud Storage file"
  echo
  echo "       -t MANDATORY parameter, can be attributes, impressions or events"
  echo
  echo "       -b Upload processed file to BQ"
  echo
  echo "       -d decompress"
  echo
  echo "       -h Help"
  echo
  echo " * To execute in background - run as nohup ./batch.sh -d gs://path -t impressions </dev/null &>/dev/null &"
}

decompress() {
  lzop -d $FILENAME
}

upload_to_bq() {
  bq load --source_format=CSV --field_delimiter=tab --null_marker='\N' --encoding=UTF-8 $BQ_TABLE$TYPE $FILE_NO_EXTENSION.csv
}

process_file() {
  gsutil --quiet cp $ORIGINAL_FILENAME .
  sed $'1s/\xef\xbb\xbf//' < $FILENAME > $FILE_NO_EXTENSION.bom
  cat $FILE_NO_EXTENSION.bom | sed "s/^/$FILENAME\t/g" | sed 's/\\"/""/g' > $FILE_NO_EXTENSION.csv
  rm -rf $FILE_NO_EXTENSION.bom $FILENAME
}

while getopts "f:t:hbd" options; do
  case "${options}" in
    f)
      ORIGINAL_FILENAME=${OPTARG}
      FILENAME=$(echo $ORIGINAL_FILENAME | awk -F "/" '{print $NF}' )
      FILE_PATH=${ORIGINAL_FILENAME/$FILENAME/''}
      FILE_NO_EXTENSION=${FILENAME%????}
      ;;
    t)
      TYPE=${OPTARG}
      options_type=("attributed-events" "impressions" "events")
      if [[ ! " ${options_type[*]} " =~ " ${TYPE} " ]]; then
        echo "ERROR: type is not a listed type, check help"
        exit_abnormal
        exit 1
      fi
      ;;
    h)
      help
      exit 0
      ;;
    b)
      UPLOAD_TO_BQ=1
      ;;
    d)
      decompress
      ;;
    :)
      echo "ERROR: -${OPTARG} requires an argument."
      exit_abnormal
      ;;
    *)
      exit_abnormal
      ;;
  esac
done

if [ -z "${TYPE}" ]; then
  echo "ERROR: You MUST assign type [-t flag]"
  exit_abnormal
fi

if [[ -z "${ORIGINAL_FILENAME}" ]]; then
  echo "ERROR: Script requires a file"
  exit_abnormal
fi

process_file

if [[ "${UPLOAD_TO_BQ}" == 1 ]]; then
  upload_to_bq
else
  cat $FILE_NO_EXTENSION.csv >> $TYPE$DEFAULT_AGGR_FILE
fi
rm -rf $FILE_NO_EXTENSION.csv

exit 0                                    