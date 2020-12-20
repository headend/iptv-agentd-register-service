package main

import (
	messagequeue "github.com/headend/share-module/MQ"
	"github.com/headend/share-module/configuration"
	"log"
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
	for {
		msg, err := mq.Consumer.ReadMessage(-1)
		if err != nil {
			log.Printf("Consumer error: %v (%v)\n", err, msg)
			log.Print("Se you again!")
			break
		}
		log.Print(msg.value)
		//var controlMsgData *model.AgentCTLQueueRequest
		//json.Unmarshal(msg.Value, &controlMsgData)
	}
}
