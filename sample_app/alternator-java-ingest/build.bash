# Build image
myRegistry=docker.io/tjlscylladb

# Enable Buildx (multi-arch support)
docker buildx create --use --name multiarch

# Build + push for BOTH architectures
docker buildx build \
  --platform linux/amd64,linux/arm64 \
  --tag ${myRegistry}/alternator-loader:1.0 \
  --push \
  .

