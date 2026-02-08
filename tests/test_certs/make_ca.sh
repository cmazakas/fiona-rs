#!/bin/bash

set -e

# Generate the private key for the root CA.
openssl genpkey -algorithm ED25519 -out ca.key

# Generate the root certificate for the CA using the above privatey key.
# We set the Common Name and use default values for the rest of the certificate.
openssl req -x509 -new -key ca.key -out ca.crt -days 3650 -subj "/CN=fiona-rs Dummy CA"

# Generate the private key for the dummy TCP servers in our test suite.
openssl genpkey -algorithm ED25519 -out server.key

# Generate the certificate signing request using our server's private key.
openssl req -new -key server.key -out server.csr -subj "/CN=localhost"

# Sign the generated CSR using our dummmy CA from above.
openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out server.crt -days 3650 -extfile server.v3.ext
