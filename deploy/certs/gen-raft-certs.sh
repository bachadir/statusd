#!/bin/sh
# One-time, offline: a raft CA and node certificates for eu1, us1 and witness.
#   deploy/certs/gen-raft-certs.sh <out-dir>
# Store them in your secret store (CI file variables, Vault, ...), then delete the directory.
# Keep the CA key offline to issue replacements. Peers verify each other against this CA
# with server_name "statusd", so the SAN does not depend on DNS names or LB IPs.
# Node names: NODES="eu1 us1 witness" (default).
set -eu
# RSA, not EC: LibreSSL (macOS) writes explicit EC parameters, which Go rejects.
d=${1:?usage: $0 <out-dir>}
mkdir -p "$d"; cd "$d"
umask 077
[ -f ca.pem ] && { echo "$d/ca.pem exists; refusing to overwrite" >&2; exit 1; }
days=${DAYS:-730}
openssl req -x509 -newkey rsa:3072 -nodes -keyout ca-key.pem -out ca.pem \
  -days $((days + 365)) -subj "/CN=statusd-raft-ca" 2>/dev/null
for n in ${NODES:-eu1 us1 witness}; do
  openssl req -newkey rsa:3072 -nodes -keyout $n-key.pem -out $n.csr -subj "/CN=$n" 2>/dev/null
  printf "subjectAltName=DNS:statusd,DNS:%s\nextendedKeyUsage=serverAuth,clientAuth\n" "$n" > $n.ext
  openssl x509 -req -in $n.csr -CA ca.pem -CAkey ca-key.pem -CAcreateserial -out $n.pem -days "$days" -extfile $n.ext 2>/dev/null
  rm $n.csr $n.ext
done
rm -f ca.srl
echo "certificates in $d (valid $days days)"
