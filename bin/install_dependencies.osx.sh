brew install cmake openssl protobuf postgresql
if [ -z "$(which go)" ]; then
	echo "You'll need to install go. See https://golang.org/dl/"
fi