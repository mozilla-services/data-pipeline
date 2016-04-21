# As of 20160421, the latest cmake won't work. Install cmake 3.1 as a workaround.
brew install openssl protobuf postgresql homebrew/versions/cmake31
if [ -z "$(which go)" ]; then
	echo "You'll need to install go 1.4.x - see https://golang.org/dl/"
fi
