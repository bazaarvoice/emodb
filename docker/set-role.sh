#!/bin/sh

counter=0

url="http://emodb-web-dc1:8080/uac/1/role/anonymous/anonymous/"
data='{"name":"anonymous","description":"anonymous","permissions":["sor|if(or(\"read\",\"create_table\"))|*","blob|read|*","databus|*","queue|*","stash|*"]}'

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

retry 5 curl -sf ${url} -H "Content-Type: application/x.json-create-role" -H "X-BV-API-Key: local_admin" -d "${data}"
