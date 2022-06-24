BUILD_DIR=bin
BUILD_NAME=bullhorn-to-dataset
BUILD_PREFIX=${BUILD_DIR}/${BUILD_NAME}

VERSION=0.0.2
LDFLAGS="-X bullhorn-to-dataset/cmd.version=$(VERSION)"

build:
	@mkdir -p ${BUILD_DIR}
	@GOOS=darwin GOARCH=amd64  go build -o ${BUILD_PREFIX}-darwin-amd64      -ldflags=${LDFLAGS}
	@GOOS=darwin GOARCH=arm64  go build -o ${BUILD_PREFIX}-darwin-arm64      -ldflags=${LDFLAGS}
	@GOOS=linux GOARCH=amd64   go build -o ${BUILD_PREFIX}-linux-amd64       -ldflags=${LDFLAGS}
	@GOOS=linux GOARCH=386     go build -o ${BUILD_PREFIX}-linux-386         -ldflags=${LDFLAGS}
	@GOOS=linux GOARCH=amd64   go build -o ${BUILD_PREFIX}-linux-amd64       -ldflags=${LDFLAGS}
	@GOOS=windows GOARCH=amd64 go build -o ${BUILD_PREFIX}-windows-amd64.exe -ldflags=${LDFLAGS}
	@GOOS=windows GOARCH=386   go build -o ${BUILD_PREFIX}-windows-386.exe   -ldflags=${LDFLAGS}

test:
	@go test ./... -coverprofile coverage.out
	@go tool cover -html coverage.out
