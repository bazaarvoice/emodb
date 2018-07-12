#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
CONFIG_FILE="${DIR}/config-local.yaml"
DDL_FILE="${DIR}/config-ddl-local.yaml"

function print_usage_and_exit {
    cat <<EOF
    $(basename $0) - Start EmoDB locally.


     Starts the following servers locally (using ${DDL_FILE} and ${CONFIG_FILE}):
     - EmoDB (ports 8080, 8081)
     - Cassandra (port 9160)
     - ZooKeeper (port 2181)

     The first time this is run, Cassandra will be initialized with a default
     schema and an empty # data set.  Data will be stored in "target/cassandra".
     On subsequent runs where "target/cassandra" already exists, the Cassandra
     schema and data will not be modified.

     Once the server is running you can access the Cassandra command line
     interface using the following commands:

       cd target/cassandra/bin
       java -jar cassandra-cli.jar

     Options:

        --ddl-file       Which ddl file to use [Default: emodb/web-local/${DDL_FILE}]
        --config-file    Which config file to use [Default: emodb/web-local/${CONFIG_FILE}]

     Examples:

        Using default files
            $(basename $0)

        Passing in files
            $(basename $0) --ddl-file config-ddl-local-2.yaml --config-file config-local-2.yaml

EOF

    exit 2
}


if [[ $# -gt 0 ]]; then
    while [[ $# -gt 0 ]]; do
        case "${1}" in
            -h|--help)
                print_usage_and_exit
                ;;
            --ddl-file)
                DDL_FILE="${PWD}/${2}"
                shift 2
                ;;
            --config-file)
                CONFIG_FILE="${PWD}/${2}"
                shift 2
                ;;
            *)
                error "Unknown option ${1}"
                ;;
        esac
    done
fi

mvn clean verify -f ${DIR{/pom.xml -P init-cassandra,start-emodb -Dconfig.file="${CONFIG_FILE}" -Dddl.file="${DDL_FILE}"