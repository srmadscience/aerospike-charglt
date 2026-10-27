#!/bin/sh
# Start a single node Aerospike Community Edition server on localhost:3000.
# The demo uses the 'test' namespace that ships in the image's default config.
docker run -d --name aerospike -p 3000-3002:3000-3002 aerospike/aerospike-server:latest
