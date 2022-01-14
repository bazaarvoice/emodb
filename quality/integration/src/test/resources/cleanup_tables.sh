#!/usr/bin/env bash
set -o errexit
set -o nounset

#List of prefixes used in DTQ emo integration test
table_prefixes=("gatekeeper_")

#-------------------------------------------------------------------------------
# Main entry point
#-------------------------------------------------------------------------------
function main() {
  local url=""
  local api_key=""
  local dry_run=false

  while [[ ${#} -gt 0 ]]; do
    case "${1}" in
      -h | --host)          url="${2}"; shift 2;;
      -k | --api-key)       api_key="${2}"; shift 2;;
      --dry-run)            dry_run=true; shift;;
      --help)               usage; exit 0;;
      --)                   break;;
      -*)                   usage_error "Unrecognized option ${1}";;
    esac

  done

  if [[ -z "${url}" ]]; then
    usage_error "url isn't specified."
  fi

  if [[ -z "${api_key}" ]]; then
    usage_error "api_key isn't specified."
  fi

  api_key_header="X-BV-API-Key: ${api_key}"
  tables=$(curl -H "${api_key_header}" "${url}/sor/1/_table?limit=2147483647" | jq .[].name)

  # Go through each prefix
  for table_prefix in ${table_prefixes[*]}; do
    log "Deleting tables for $table_prefix prefix"
    # And get the tables that match the prefix
    filtered_tables=$(echo "${tables}" | grep "^\\\"${table_prefix}" | tr -d '"' )
    for table in ${filtered_tables}; do
      if [[ "${table}" == *"blob"* ]]; then
        service_endpoint="blob"
      else
        service_endpoint="sor"
      fi
      log "Table: ${table}"

      purge_url="${url}/${service_endpoint}/1/_table/${table}/purge?audit=comment:'PurgeTable'"
      delete_url="${url}/${service_endpoint}/1/_table/${table}?audit=comment:'DeleteTable'"
      if [[ ${dry_run} == true ]]; then
        log "dry run: curl -XPOST -H ${api_key_header} ${purge_url}"
        log "dry run: curl -XDELETE -H ${api_key_header} ${delete_url}"
      else
        curl -XPOST -H "${api_key_header}" "${purge_url}"
        log ""
        curl -XDELETE -H "${api_key_header}" "${delete_url}"
        log ""
      fi
    done
  done
}

#-------------------------------------------------------------------------------
# Emit an error message and then usage information and then exit the program.
#-------------------------------------------------------------------------------
function usage_error() {
  error "$@"
  echo 1>&2
  usage
  exit 1
}

#-------------------------------------------------------------------------------
# Emit an error message and then usage information and then exit the program.
#-------------------------------------------------------------------------------
function error() {
  echo 1>&2 "ERROR: $@"
}

#-------------------------------------------------------------------------------
# Log a message.
#-------------------------------------------------------------------------------
function log {
  echo 1>&2 "$@"
}

#-------------------------------------------------------------------------------
# Emit the program's usage to the console.
#-------------------------------------------------------------------------------
function usage() {
    cat <<EOF
Usage: ${0#./} [OPTION]...
Options:
  -h | --host <emodb web url>
  -k | --api-key <emodb api key>
  --dry-run
      dry run
  --help
      Display this help message.
EOF
}

main "${@:-}"
