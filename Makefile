run:
	go run main.go

build:
	go build -o kvshard main.go

clean:
	rm -rf kvshard

.PHONY: run build clean