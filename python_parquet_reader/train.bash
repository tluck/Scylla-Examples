#!/bin/bash -x

if [[ $1 == "" ]]; then 
    printf "Usage: $0 ip-adress\n"
    exit 1
fi
address=$1

family=('mp' 'dc' 'ib' 'pb' 'is' 'ta' 'cb' 'ha' 'of' 'ie' 'if' 'up' 'ci' 'd' 'ra' 't' 'e' 'ch')
keyspace="moloco_zdic"

nodetool -h $address clearsnapshot
nodetool -h $address flush
#nodetool -h $address compact $keyspace
#curl -X POST "http://$address:10000/storage_service/flush"
#curl -X POST "http://$address:10000/storage_service/compact"
for f in ${family[@]}
do
table=table_${f}
nodetool -h $address compact $keyspace $table
curl -X POST "http://$address:10000/storage_service/retrain_dict?keyspace=$keyspace&cf=$table"
done