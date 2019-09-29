package mapreduce

import (
    "encoding/json"
    "fmt"
    "log"
    "os"
    "sort"
)

func doReduce(
    jobName string, // the name of the whole MapReduce job
    reduceTask int, // which reduce task this is
    outFile string, // write the output here
    nMap int,       // the number of map tasks that were run ("M" in the paper)
    reduceF func(key string, values []string) string,
) {
    fmt.Println(jobName, reduceTask, outFile, nMap)
    keyValues := make(map[string][]string, 0)

    for i := 0; i < nMap; i++ {
        mapTempName := reduceName(jobName, i, reduceTask)
        file, err := os.Open(mapTempName)
        if err != nil {
            log.Printf("cannot find file %s, %v", mapTempName, err)
        }
        defer file.Close()

        dec := json.NewDecoder(file)
        for {
            var kv KeyValue
            err := dec.Decode(&kv)
            if err != nil {
                break
            }
            _, ok := keyValues[kv.Key]
            if !ok {
                keyValues[kv.Key] = make([]string, 0)
            }
            keyValues[kv.Key] = append(keyValues[kv.Key], kv.Value)
        }
    }

    var keys []string

    for k, _ := range keyValues {
        keys = append(keys, k)
    }
    sort.Strings(keys)

    mergeFile, err := os.Create(outFile)
    if err != nil {
        log.Printf("doReduce: create merge file %s, %v ", outFile, err)
    }
    defer func() {
        err := mergeFile.Sync()
        if err != nil {
            log.Println("doReduce: sync failed", err)
        }
        err = mergeFile.Close()
        if err != nil {
            log.Println("doReduce: close failed", err)
        }
    }()

    enc := json.NewEncoder(mergeFile)
    for _, k := range keys {
        res := reduceF(k, keyValues[k])
        err := enc.Encode(&KeyValue{k, res})
        if err != nil {
            log.Printf("doReduce: encode error: %v", err)
        }
    }
}
