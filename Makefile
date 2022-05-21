BUILD_DIR=bin
BUILD_NAME=bullhorn-to-dataset
BUILD_PREFIX=${BUILD_DIR}/${BUILD_NAME}

VERSION=0.0.1
LDFLAGS="-X bullhorn-to-dataset/cmd.version=$(VERSION)"

build:
	@mkdir -p ${BUILD_DIR}
	@go build -o ${BUILD_PREFIX} -ldflags=${LDFLAGS}

test:
	@go test ./... -coverprofile coverage.out
	@go tool cover -html coverage.out
