start:
	go run main.go

build:
	go build -o kvshard main.go

client:
	nc localhost 9500

clean:
	rm -rf kvshard

.PHONY: start build client clean
