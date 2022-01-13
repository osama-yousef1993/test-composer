#!/bin/bash
set -e
DEFAULT_FEED_FILE=gsfiles.txt
DEFAULT_SIZE_CACHE=_size.txt
DEFAULT_AGGR_FILE=_wip.csv
DEFAULT_FLUSH_TRIGGER=3000000000
BQ_TABLE=at-data-science-research:MediaMath_Logs.
CLOUD_EXEC=0

usage() {
  echo
  echo "Usage: $0 gs://[path] [ -t TYPE ] [ hc ]" 1>&2
}

exit_abnormal() {                         
  usage
  exit 1
}

help() {
  echo "Usage: $0 [ -d path with gs:// prefix ] [ -t type ] [ hc ]"
  echo
  echo "       -d Path points to GCP Cloud Storage directory, recursive read will find all files"
  echo "          this flag is MANDATORY unless you create a feedfile (a txt file with complete "
  echo "          paths and file you wish to ingest). filename must be ${DEFAULT_FEED_FILE}"
  echo
  echo "       -t MANDATORY parameter, can be attributes, impressions or events"
  echo
  echo "       -c if executing in Airflow this param is required to allow script to execute bash"
  echo "          files with proper permissions and from correct location"
  echo
  echo "       -h Help"
  echo
}

flush() {
  bq load --source_format=CSV --field_delimiter=tab --null_marker='\N' --encoding=UTF-8 $BQ_TABLE$TYPE $TYPE$DEFAULT_AGGR_FILE
  rm -rf $TYPE$DEFAULT_SIZE_CACHE
}

check_if_needs_flushing() {
  TOTAL=$( awk '{total += $1}END{ print total}' ${TYPE}${DEFAULT_SIZE_CACHE} )
  if [ "$TOTAL" -gt "${DEFAULT_FLUSH_TRIGGER}" ]; then
    echo "INFO: Flushing aggregation file at ${TOTAL} bytes"
    flush
  fi
}

process_from_file() {
  if [ ! -f "$DEFAULT_FEED_FILE" ]; then
    gsutil ls -R $DIRECTORY | grep -e "txt$" > $DEFAULT_FEED_FILE
  fi

  TMP_NUM_LINES=$( wc -l gsfiles.txt | awk '{print $1}' )
  if [[ "${TMP_NUM_LINES}" == 0 ]]; then
    echo "ERROR: No directory or files found"
    rm -rf $DEFAULT_FEED_FILE
    exit_abnormal
  fi
  echo "INFO: Processing ${TMP_NUM_LINES} file(s)"

  while read gcpfile; do
    gsutil du -s -a $gcpfile | awk '{print $1}' >> $TYPE$DEFAULT_SIZE_CACHE
    if [[ "${CLOUD_EXEC}" == 0 ]]; then
      ./process.sh -f $gcpfile -t $TYPE
    else
      bash /home/airflow/gcs/dags/scripts/process.sh -f $gcpfile -t $TYPE
    fi
    check_if_needs_flushing
  done < $DEFAULT_FEED_FILE

  if [ -f "${TYPE}${DEFAULT_SIZE_CACHE}" ]; then
    TOTAL=$( awk '{total += $1}END{ printf "%.0f", total}' ${TYPE}${DEFAULT_SIZE_CACHE} )
    echo "INFO: FINAL flush of aggregation file at ${TOTAL} bytes"
    flush
  fi
  rm -rf $DEFAULT_FEED_FILE $TYPE$DEFAULT_AGGR_FILE
}

while getopts "d:t:hc" options; do
  case "${options}" in
    d)
      DIRECTORY=${OPTARG}
      ;;
    c)
      CLOUD_EXEC=1
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

if [[ -z "${DIRECTORY}" && ! -f "${DEFAULT_FEED_FILE}" ]]; then
  echo "ERROR: You must point to storage directory [-d flag] OR supply a feed file"
  exit_abnormal
fi

process_from_file
exit 0                                    