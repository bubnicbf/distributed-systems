package mapreduce

import (
	"encoding/json"
	"fmt"
	"os"
	//"sync"
	//"strings"
)

// doReduce manages one reduce task: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// You will need to write this function.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTaskNumber) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//

	// CUTSOM
	// get all kv pairs from tmp data
	// Dataflow
	// 1. read each tmp file which map task generated
	// 2. decode file json data to kv
	// 3. sorted kv list by k
	// 4. group the kv pairs to k-(v list) which k is same
	// 5. get reduce(k, v-list) result and write the k-result to output file

	fmt.Println("doReduce1")
	kvList := make([]KeyValue, 0)
	for i := 0; i < nMap; i++ {

		readFileName := reduceName(jobName, i, reduceTaskNumber)
		readFile, readFileErr := os.Open(readFileName)
		if readFileErr != nil {
			return
		}
		dec := json.NewDecoder(readFile)
		for dec.More() {
			var kv KeyValue
			decErr := dec.Decode(&kv)
			if decErr != nil {
				return
			}
			kvList = append(kvList, kv)
		}
	}

	//close(kvChan)
	//fmt.Println("doReduce chan finish")

	// sorted or not
	// we can skip sort procedure daze!
	kvsMap := make(map[string][]string)
	for _, kv := range kvList {
		if _, ok := kvsMap[kv.Key]; ok {
			// found key in the kvList
			kvsMap[kv.Key] = append(kvsMap[kv.Key], kv.Value)
		} else {
			kvsMap[kv.Key] = make([]string, 1)
			kvsMap[kv.Key] = append(kvsMap[kv.Key], kv.Value)
		}
	}

	writeFile, writeFileErr := os.Create(outFile)
	if writeFileErr != nil {
		return
	}
	defer writeFile.Close()

	outEnc := json.NewEncoder(writeFile)
	for key, vlist := range kvsMap {
		outEnc.Encode(KeyValue{key, reduceF(key, vlist)})
	}

}
