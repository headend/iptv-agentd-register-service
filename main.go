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
	"github.com/headend/share-module/model/warmup"
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
		res, err2 := getAgents(agentClient, registerMsgData)
		if err2 != nil {
			log.Println(err2)
			continue
		}
		if len(res.Agents) > 0 {
			log.Println("Agent already exists")
			// tạo message warmup cho agent này
			warmupMessageString, err9 := makeWarmupMessageString(res.Agents)
			if err9 != nil {
				log.Println(err9)
				continue
			}
			err10 := pushMessageToWarmup(conf, warmupMessageString)
			if err10 != nil {
				log.Println(err10)
				continue
			}
			continue
		}
		//
		// create agent
		err11 := addAgent(registerMsgData, agentClient)
		if err11 != nil {
			log.Println(err11)
			continue
		}
		log.Printf("Success to add new agent %#v", registerMsgData)
	}
}

func addAgent(registerMsgData *register.Register, agentClient agentpb.AgentCTLServiceClient) error {
	newAgent := agentpb.AgentRequest{
		IpControl:          registerMsgData.IP,
		IpReceiveMulticast: "",
		Status:             true,
	}
	c, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := (agentClient).Add(c, &newAgent)
	return err
}

func getAgents(agentClient agentpb.AgentCTLServiceClient, registerMsgData *register.Register) (res *agentpb.AgentResponse, err error) {
	c, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	res, err = (agentClient).Get(c, &agentpb.AgentFilter{IpControl: registerMsgData.IP})
	return res, err
}

func pushMessageToWarmup(conf configuration.Conf, warmupMessageString string) error {
	var mq messagequeue.MQ
	mq.PushMsgByTopic(&conf, warmupMessageString, conf.MQ.WarmUpTopic)
	return mq.Err
}

func makeWarmupMessageString(agents []*agentpb.Agent) (string, error) {
	var warmupMessage warmup.WarmupMessage
	var warmupData []warmup.WarmupElement
	for _, agent := range agents {
		warupElement := warmup.WarmupElement{
			IP:     agent.IpControl,
			Status: true,
		}
		warmupData = append(warmupData, warupElement)
	}
	warmupMessageString, err9 := warmupMessage.GetJsonString()
	return warmupMessageString, err9
}
