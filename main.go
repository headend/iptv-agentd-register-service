package main

import (
	"context"
	"encoding/json"
	messagequeue "github.com/headend/share-module/MQ"
	"github.com/headend/share-module/configuration"
	"github.com/headend/share-module/model/register"
	"log"
	agentpb "github.com/headend/iptv-agent-service/proto"
	"time"
	myRpc"github.com/headend/share-module/mygrpc/connection"
)

func main()  {
	// load config
	var conf configuration.Conf
	conf.LoadConf()
	var mq messagequeue.MQ
	mq.InitConsumerByTopic(&conf, conf.MQ.WarmUpTopic)
	defer mq.CloseConsumer()
	if mq.Err != nil {
		log.Print(mq.Err)
	}

	log.Printf("Listen mesage from %s topic\n",conf.MQ.WarmUpTopic)
	var agentConn myRpc.RpcClient
	//try connect to agent
	agentConn.InitializeClient(conf.RPC.Agent.Host, string(conf.RPC.Agent.Port))
	defer agentConn.Client.Close()
	//	connect agent services
	agentClient := agentpb.NewAgentCTLServiceClient(agentConn.Client)
	for {
		msg, err := mq.Consumer.ReadMessage(-1)
		if err != nil {
			log.Printf("Consumer error: %v (%v)\n", err, msg)
			log.Print("Se you again!")
			break
		}
		log.Print(msg.Value)
		var registerMsgData *register.Register
		json.Unmarshal(msg.Value, &registerMsgData)

		// Check Agent exists
		c, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		res, err := (agentClient).Get(c, &agentpb.AgentFilter{IpControl: registerMsgData.IP})
		if err != nil {
			log.Println(err)
			continue
		}
		if len(res.Agents) > 0 {
			log.Println("Agent already exists")
			continue
		}
		//
		// create agent
		newAgent := agentpb.AgentRequest{
			IpControl:          registerMsgData.IP,
			IpReceiveMulticast: "",
			Status:             true,
		}
		_, err2 := (agentClient).Add(c, &newAgent)
		if err2 != nil {
			log.Println(err2)
			continue
		}
	}
}
