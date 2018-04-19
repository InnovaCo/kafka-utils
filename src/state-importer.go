package main

import (
	"github.com/Shopify/sarama"

	"strings"
	"log"
	"flag"
	"sync"
	"os"
	"regexp"
	"encoding/json"
	"io/ioutil"
	"strconv"
	"hash/fnv"
	"os/signal"
	"fmt"
	//"sync/atomic"
	"time"
	"github.com/rcrowley/go-metrics"
)

var (
	in       = flag.String("in", "", "input topic")
	out      = flag.String("out", "", "output topic")
	brokers  = flag.String("brokers", "", "list of brokers")
	expect   = flag.String("expect", "", "expected regular expresions for selected keys")
	replace  = flag.Bool("delete", false, "delete messages with selected keys")
	test     = flag.Bool("test", false, "testing without producing and deleting messages")
)

type PartitionOffset struct {
	partId string
	offset int64
}

func main() {

	flag.Parse()
	checkDefault(*in)
	checkDefault(*brokers)
	checkDefault(*expect)

	action := "copy_offsets"
	if (*replace)  {
		action = "delete_offsets"
	}

	file := *in + "-" + action + "-" + strconv.Itoa(int(hash(*expect))) + ".json"

	lastSavedOffsets := loadFile(file)

	//metrics
	msgCounter := metrics.NewCounter()
	metrics.Register("counterHandeledMsg", msgCounter)
	sumOffsets := int64(0)
	for key := range lastSavedOffsets { sumOffsets += lastSavedOffsets[key] }
	msgCounter.Inc(sumOffsets)

	mpsRate1 := metrics.NewMeter()
	metrics.Register("meanRate", mpsRate1)

	//channels
	offsetsChannel := make(chan PartitionOffset)
	signalChannel  := make(chan os.Signal)
	signal.Notify(signalChannel, os.Kill)
	saveOffsets := PartitionOffset{"saveOffsets", int64(1)}

	// для удаления ключей пишем сообщения с телом null в топик из которого читаем
	if (*replace) { *out = *in }

	expected := strings.Fields(*expect)
	brokers  := strings.Fields(*brokers)

	client := buildClient(brokers)
	defer client.Close()

	consumer := buildConsumer(client)
	defer consumer.Close()

	producer := buildProducer(client)
	defer producer.Close()

	partitions, _ := consumer.Partitions(*in)

	var regexpList []*regexp.Regexp
	for i := 0; i < len(expected); i++ {
		regexpList = append(regexpList, regexp.MustCompile(string(expected[i])))
	}

	//var msgCount uint64 = 0

	var wg sync.WaitGroup

	for _, p := range partitions {
		wg.Add(1)

		partId := p
		partIdStr := strconv.Itoa(int(partId))
		lastSavedOffset := lastSavedOffsets[partIdStr]

		startingOffset := int64(0)

		//если в файле есть оффсет для текущей партиции, то начинаем с него, если нет, то с нулевого
		if _, ok := lastSavedOffsets[partIdStr]; ok {
			startingOffset = int64(lastSavedOffset) + 1
		}

		go func() {

			defer wg.Done()

			consumerPart := buildConsumer(client)
			defer consumerPart.Close()

			c, err := consumerPart.ConsumePartition(*in, int32(partId), startingOffset)

			log.Println("Read partition", partId, "from offset", startingOffset)

			if err != nil {
				log.Printf("Consumer failed =(, but don't take it to heart")
				panic(err)
			}

			offsetNewest, err := client.GetOffset(*in, int32(partId), sarama.OffsetNewest)
			if err != nil {
				log.Printf("Get offset newest failed")
				panic(err)
			}

			offsetOldest, err := client.GetOffset(*in, int32(partId), sarama.OffsetOldest)
			if err != nil {
				log.Printf("Get offset oldest failed")
				panic(err)
			}

			if(offsetNewest - 1 == offsetOldest || offsetNewest - 1 == startingOffset) {
				log.Println("Partition", partId, "already read")
			} else {
				for {
					select {
					case msg := <-c.Messages():

						msgKey := sarama.StringEncoder(msg.Key)
						msgVal := sarama.ByteEncoder(msg.Value)
						msgOffset := msg.Offset

						msgCounter.Inc(1)
						mpsRate1.Mark(1)

						//atomic.AddUint64(&msgCount, 1)

						if (grep(regexpList, string(msgKey)) && (!*replace || (*replace && (msgOffset < offsetNewest)))) {
						//if (grep(regexpList, string(msgKey)) && (!*delete || (*delete && (msgVal != nil)))) {

							if (*replace) {
								msgVal = nil
							}

							msgOutput := &sarama.ProducerMessage {
								Topic: *out,
								Key:   msgKey,
								Value: msgVal,
								Partition: int32(partId),
							}

							if (*test) {
								log.Println("[", partId, ":", msgOffset, "/", offsetNewest, "] msgOutput", msgOutput)
							} else {
								producer.Input() <- msgOutput
							}
						}
							offsetsChannel <- PartitionOffset{ partIdStr, msgOffset }

					case <- signalChannel:
						fmt.Println("Interrupt is detected")
						offsetsChannel <- saveOffsets
					}
				}
			}
		}()
	}


	clientForDiff := buildClient(brokers)
	defer clientForDiff.Close()

	go func() {
		for range time.Tick(10000 * time.Millisecond) {
			offsetsChannel <- saveOffsets
		}
	}()

	go func() {
		for message := range offsetsChannel {
			
			switch message {

			case saveOffsets:

				mpsRate1 := mpsRate1.Rate1()
				restMessages := int64(0)
				allMessages  := int64(0)

				for _, p := range partitions {
					partId    := p
					partIdStr := strconv.Itoa(int(partId))
					offsetNewest, _ := clientForDiff.GetOffset(*in, int32(partId), sarama.OffsetNewest)
					restMessagesByPartition := (offsetNewest - 1) - lastSavedOffsets[partIdStr]
					restMessages = restMessages + restMessagesByPartition
					allMessages  = allMessages + offsetNewest
				}

				log.Println("MPS_rate_1 : ", int64(mpsRate1))
				log.Println("Remained time (based on rate1):", (float64(restMessages)/(mpsRate1*60)))
				log.Println("Rest messages :", restMessages)
				log.Println("Number of all messages:", allMessages, "\n")

				saveFile(file, lastSavedOffsets)

		        default:
				lastSavedOffsets[message.partId] = message.offset
			}
		}
	}()
	wg.Wait()
}

func buildClient(brokerList []string) sarama.Client {

	config := sarama.NewConfig()
	config.Producer.Partitioner = sarama.NewManualPartitioner

	client, err := sarama.NewClient(brokerList, config)
	if err != nil {
		panic(err)
	} else {
		log.Println("Connected.")
	}

	return client
}

func buildConsumer(client sarama.Client) sarama.Consumer {

	consumer, err := sarama.NewConsumerFromClient(client)
	if err != nil {
		log.Fatalln("Failed to start consumer:", err)
	}

	return consumer
}

func buildProducer(client sarama.Client) sarama.AsyncProducer {

	producer, err := sarama.NewAsyncProducerFromClient(client)

	if err != nil {
		log.Fatalln("Failed to start producer:", err)
	}

	go func() {
		for err := range producer.Errors() {
			log.Println("Failed to write message:", err)
		}
	}()

	return producer
}

func loadFile(path string) map[string]int64 {

	lastSavedOffsets := make(map[string]int64)

	_, err := os.Stat(path)
	if err != nil {
		log.Println("File not exist")
	} else {
		file, err:= ioutil.ReadFile(path)
		if err != nil {
			log.Fatalln("File load with error: ", err)
		}
		json.Unmarshal(file, &lastSavedOffsets)}

	return lastSavedOffsets
}

func saveFile(path string, lastSavedOffsets map[string]int64) {
	b, err := json.Marshal(lastSavedOffsets)
	if err != nil {
		log.Fatalln(err)
	}
	e := ioutil.WriteFile(path, b, 0644)
	if e != nil {
		log.Fatalln("File can't write: ", e)
	}
}

func checkDefault(value string) {
	if value == "" {
		flag.PrintDefaults()
		os.Exit(1)
	}
}

func grep(regex []*regexp.Regexp, value string) bool {
	for i := 0; i < len(regex); i++ {
		if (regex[i].MatchString(value)) {
			return true
		}
	}
	return false
}

func hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}
