package mapreduce

import (
	"bufio"
	"encoding/json"
	"hash/fnv"
	"io"
	"log"
	"os"
	"sync"
)

func doMap(
	jobName string, // the name of the MapReduce job
	mapTask int, // which map task this is
	inFile string,
	nReduce int, // the number of reduce task that will be run ("R" in the paper)
	mapF func(filename string, contents string) []KeyValue,
) {
	fd, err := os.Open(inFile)
	if err != nil {
		log.Printf("doMap: fail to open %s, err: %v\n", inFile, err)
		return
	}

	var kvChan = make(chan KeyValue, 1000)

	wg := sync.WaitGroup{}
	go func() {
		wg.Add(1)
		defer wg.Done()
		writeWorker(kvChan, jobName, mapTask, nReduce)
	}()

	reader := bufio.NewReaderSize(fd, 20480)

	for {
		// assume no \n in data
		lines, err := reader.ReadBytes('\n')
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Printf("read bytes line err: %v\n", err)
		}

		keyValues := mapF(inFile, string(lines[:len(lines)-1]))
		for _, item := range keyValues {
			kvChan <- item
		}
	}
	close(kvChan)

	wg.Wait()
}

func writeWorker(kvChan chan KeyValue, jobName string, mapTask int, nReduce int) {
	writeFd := map[string]*json.Encoder{}
	for kv := range kvChan {
		reduceFile := reduceName(jobName, mapTask, ihash(kv.Key)%nReduce)
		var ec *json.Encoder
		var ok bool
		if ec, ok = writeFd[reduceFile]; !ok {
			fd, err := os.Create(reduceFile)
			defer func() {
				err := fd.Sync()
				if err != nil {
					log.Println("doMap: sync failed", err)
				}
				err = fd.Close()
				if err != nil {
					log.Println("doMap: close failed", err)
				}
			}()

			if err != nil {
				log.Println("doMap: create reduce, err: ", err)
			}
			ec = json.NewEncoder(fd)
			writeFd[reduceFile] = ec
		}
		err := ec.Encode(&kv)
		if err != nil {
			log.Println("doMap: encode failed", kv, err)
		}
	}
}

func ihash(s string) int {
	h := fnv.New32a()
	h.Write([]byte(s))
	return int(h.Sum32() & 0x7fffffff)
}
