ver=4.1
java -jar target/scylla-loader-${ver}.jar \
  -k mercado \
  -t userid \
  -u $USERNAME \
  -p $PASSWORD \
  --dc $DC \
  -s $HOSTS \
  -w 4 \
  -r 1000000 \
	--batch_mode unlogged \
  --batch_size 1000 \
  -c 200 \
  -d
