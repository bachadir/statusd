#!/bin/sh
# Generates a throwaway CA and node certificates (SAN "statusd") for the HA test.
set -eu
d="$(dirname "$0")/certs"; mkdir -p "$d"; cd "$d"
[ -f ca.pem ] && exit 0
openssl req -x509 -newkey rsa:2048 -nodes -keyout ca-key.pem -out ca.pem -days 30 -subj "/CN=statusd-test-ca" 2>/dev/null
for n in eu1 us1 witness; do
  openssl req -newkey rsa:2048 -nodes -keyout $n-key.pem -out $n.csr -subj "/CN=$n" 2>/dev/null
  printf "subjectAltName=DNS:statusd,DNS:%s\nextendedKeyUsage=serverAuth,clientAuth\n" "$n" > $n.ext
  openssl x509 -req -in $n.csr -CA ca.pem -CAkey ca-key.pem -CAcreateserial -out $n.pem -days 30 -extfile $n.ext 2>/dev/null
  rm $n.csr $n.ext
done
chmod 644 *.pem
echo "certs in $d"
