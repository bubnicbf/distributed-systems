package mapreduce

import (
	//"bufio"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"os"
)

// doMap manages one map task: it reads one of the input files
// (inFile), calls the user-defined map function (mapF) for that file's
// contents, and partitions the output into nReduce intermediate files.
func doMap(
	jobName string, // the name of the MapReduce job
	mapTaskNumber int, // which map task this is
	inFile string,
	nReduce int, // the number of reduce task that will be run ("R" in the paper)
	mapF func(file string, contents string) []KeyValue,
) {
	//
	// You will need to write this function.
	//
	// The intermediate output of a map task is stored as multiple
	// files, one per destination reduce task. The file name includes
	// both the map task number and the reduce task number. Use the
	// filename generated by reduceName(jobName, mapTaskNumber, r) as
	// the intermediate file for reduce task r. Call ihash() (see below)
	// on each key, mod nReduce, to pick r for a key/value pair.
	//
	// mapF() is the map function provided by the application. The first
	// argument should be the input file name, though the map function
	// typically ignores it. The second argument should be the entire
	// input file contents. mapF() returns a slice containing the
	// key/value pairs for reduce; see common.go for the definition of
	// KeyValue.
	//
	// Look at Go's ioutil and os packages for functions to read
	// and write files.
	//
	// Coming up with a scheme for how to format the key/value pairs on
	// disk can be tricky, especially when taking into account that both
	// keys and values could contain newlines, quotes, and any other
	// character you can think of.
	//
	// One format often used for serializing data to a byte stream that the
	// other end can correctly reconstruct is JSON. You are not required to

	// use JSON, but as the output of the reduce tasks *must* be JSON,
	// familiarizing yourself with it here may prove useful. You can write
	// out a data structure as a JSON string to a file using the commented
	// code below. The corresponding decoding functions can be found in
	// common_reduce.go.
	//
	//   enc := json.NewEncoder(file)
	//   for _, kv := ... {
	//     err := enc.Encode(&kv)
	//
	// Remember to close the file after you have written all the values!
	//
	debug("doMap: %d-%d", mapTaskNumber, nReduce)
	readData, readDataErr := ioutil.ReadFile(inFile)
	if readDataErr != nil {
		return
	}

	content := string(readData)
	kvList := mapF(inFile, content)

	// open all the reduce tmp file
	// and keep all the file handler
	//writeFileList := make([]*File, 0)
	encList := make([]*json.Encoder, 0)
	for i := 0; i < nReduce; i++ {

		filename := reduceName(jobName, mapTaskNumber, i)
		f, createFileErr := os.Create(filename)
		if createFileErr != nil {
			fmt.Println("err happened when create tmp file")
			return
		}
		//writeFileList = append(writeFileList, f)
		enc := json.NewEncoder(f)
		encList = append(encList, enc)
		defer f.Close()
		// keep create file open
	}
	for _, kv := range kvList {
		key := kv.Key
		//fmt.Printf("%s\n", key)
		//value := kv.Value
		reduceTaskNumber := ihash(key) % nReduce
		//writefileName := mergeName(jobName, mapTaskNumber, reduceTaskNumber)
		writeErr := encList[reduceTaskNumber].Encode(&kv)
		if writeErr != nil {
			return
		}
	}
}

func ihash(s string) int {
	h := fnv.New32a()
	h.Write([]byte(s))
	return int(h.Sum32() & 0x7fffffff)
}