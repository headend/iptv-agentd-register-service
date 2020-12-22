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
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	// load config
	var conf configuration.Conf
	conf.LoadConf()
	var mq messagequeue.MQ
	mq.InitConsumerByTopic(&conf, conf.MQ.NraTopic)
	defer mq.CloseConsumer()
	if mq.Err != nil {
		log.Print(mq.Err)
	}

	log.Printf("Listen message from %s topic\n",conf.MQ.NraTopic)
	var agentConn myRpc.RpcClient
	//try connect to agent
	agentConn.InitializeClient(conf.RPC.Agent.Host, conf.RPC.Agent.Port)
	defer agentConn.Client.Close()
	//	connect agent services
	agentClient := agentpb.NewAgentCTLServiceClient(agentConn.Client)
	for {
		msg, err := mq.Consumer.ReadMessage(-1)
		if err != nil {
			log.Printf("Consumer error: %v (%v)\n", err, msg)
			log.Println("Wait for retry connect")
			time.Sleep(10 * time.Second)
			continue
		}

		go func() {
			if len(msg.Value) == 0 {
				log.Println("Message empty")
				return
			} else {
				log.Print(msg.Value)
			}
			var registerMsgData *register.Register
			json.Unmarshal(msg.Value, &registerMsgData)

			// Check Agent exists
			res, err2 := getAgents(agentClient, registerMsgData)
			if err2 != nil {
				log.Println(err2)
				return
			}
			if len(res.Agents) > 0 {
				log.Println("Agent already exists")
				// tạo message warmup cho agent này
				warmupMessageString, err9 := makeWarmupMessageString(res.Agents)
				if err9 != nil {
					log.Println(err9)
					return
				}
				err10 := pushMessageToWarmup(conf, warmupMessageString)
				if err10 != nil {
					log.Println(err10)
					return
				}
				return
			}
			//
			// create agent
			err11 := addAgent(registerMsgData, agentClient)
			if err11 != nil {
				log.Println(err11)
				return
			}
			log.Printf("Success to add new agent %#v", registerMsgData)
			return
		}()

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
	warmupMessage.Data = warmupData
	warmupMessage.EventTime = time.Now().Unix()
	warmupMessage.WupType = "event"
	warmupMessageString, err9 := warmupMessage.GetJsonString()
	return warmupMessageString, err9
}
