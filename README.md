# BSV - A better runit for termux

I wanted to try self hosting on my old phone with termux. I tried termux-services but it was a little not to my liking and hence here I am making it myself that could maybe like it a bit more


# Installing
## Binaries
Install the aarch64 binary from the latest releases page and add the binary to the path

## From source
Clone this github repository
```
git clone https://github.com/ExcessByte/bsv.git
```
If there is `make` on your device,
```
make build && make install
```
else run,
```
go build .
install -m755 bsv $PREFIX/bin/
```
