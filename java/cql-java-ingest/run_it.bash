ver=4.1
java -jar target/scylla-loader-${ver}.jar \
  -k mercado \
  -t userid \
  -u "${USERNAME:-cassandra}" \
  -p "${PASSWORD:-cassandra}" \
  --dc "${DC:-dc1}" \
  -s "${CONTACT_POINTS:-127.0.0.1}" \
  -w 4 \
  -r 1000000 \
	--batch_mode unlogged \
  --batch_size 1000 \
  -c 200 \
  -d
