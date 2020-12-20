proto:
		protoc -I proto/ proto/agent.proto --go_out=plugins=grpc:proto/
		cp proto/github.com/headend/iptv-agent-service/proto/agent.pb.go proto/
build:
		protoc -I proto/ proto/agent.proto --go_out=plugins=grpc:proto/
		cp proto/github.com/headend/iptv-agent-service/proto/agent.pb.go proto/
		GOOS=linux GOARCH=amd64 go build -o deployment/agent-service main.go
		docker build -t agent-services-server deployments
run:
		docker run -p 50006:50006 -v /opt/centertower/application.yml:/opt/centertower/application.yml  -v /opt/centertower/keys/:/opt/centertower/keys/ -d agent-services-server
docs:
		protoc --doc_out=./doc  proto/*.proto
dev:
		docker run -p 40006:40006 -v /opt/centertower/dev.yml:/opt/centertower/application.yml -v /opt/centertower/keys/:/opt/centertower/keys/ --name agent-dev -d agent-services-server
staging:
		docker run -p 50006:50006 -v /opt/centertower/staging.yml:/opt/centertower/application.yml -v /opt/centertower/keys/:/opt/centertower/keys/ --name agent-staging -d agent-services-server
