#!/bin/sh

function retry {
  local retries=$1
  shift

  local count=0
  until "$@"; do
    exit=$?
    wait=$((2 ** $count))
    count=$(($count + 1))
    if [ $count -lt $retries ]; then
      echo "Retry $count/$retries exited $exit, retrying in $wait seconds..."
      sleep $wait
    else
      echo "Retry $count/$retries exited $exit, no more retries left."
      return $exit
    fi
  done
  return 0
}

# Check if emo-web is up

healthcheck_url="http://emodb-main:8481/healthcheck"

retry 5 curl $healthcheck_url

# Send roles via curl
curl -XPOST -s "http://emodb-main:8480/uac/1/role/anonymous/anonymous/" \
  -H "Content-Type: application/x.json-create-role" -H "X-BV-API-Key: local_admin" \
  -d '{"name":"anonymous","description":"anonymous","permissions":["sor|if(or(\"read\",\"create_table\"))|*","blob|read|*","databus|*","queue|*","stash|*"]}'
