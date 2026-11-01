vet:
	go vet ./...

fmt:
	gofumpt -l .

build:
	go build -trimpath -ldflags="-s -w" -o bsv .

install:
	install -m755 bsv $PREFIX/bin/
