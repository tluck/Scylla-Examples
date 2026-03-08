java -cp target/alternator-loader-1.0-SNAPSHOT.jar AlternatorUserIdLoader \
  --hosts ${HOSTS} \
  --username ${USERNAME} \
  --password ${PASSWORD} \
  --dc ${DC} \
  --keyspace alternator_userid \
  --num-inserts 10000 \
  --user-id-start 1
