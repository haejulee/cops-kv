package mapreduce

import (
	"encoding/json"
	"log"
	"os"
	"sort"
)

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// doReduce manages one reduce task: it should read the intermediate
	// files for the task, sort the intermediate key/value pairs by key,
	// call the user-defined reduce function (reduceF) for each key, and
	// write reduceF's output to disk.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTask) yields the file
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
	// Your code here (Part I).
	// 

	// Read input from intermediate files
	var inputs []KeyValue
	for m := 0; m < nMap; m++ {
		fileName := reduceName(jobName, m, reduceTask)
		file, err := os.Open(fileName)
		if err != nil {
			log.Fatal("Reduce: %", err)
		}
		dec := json.NewDecoder(file)
		for dec.More() {
			var keyval KeyValue
			err = dec.Decode(&keyval)
			if err != nil {
				log.Fatal("Reduce: %", err)
			}
			inputs = append(inputs, keyval)
		}
		if err = file.Close(); err != nil {
			log.Fatal("Reduce: %", err)
		}
	}
	
	// Sort inputs by keys, aggregate all values with identical keys
	keysorter := func(i, j int) bool { return inputs[i].Key < inputs[j].Key }
	sort.SliceStable(inputs, keysorter)
	type keyVals = struct{ key string; values []string }
	var sortedInputs []keyVals
	for _, keyval := range inputs {
		if len(sortedInputs) == 0 || sortedInputs[len(sortedInputs)-1].key != keyval.Key {
			sortedInputs = append(sortedInputs, keyVals{keyval.Key, []string{keyval.Value}})
		} else {
			sortedInputs[len(sortedInputs)-1].values = append(sortedInputs[len(sortedInputs)-1].values, keyval.Value)
		}
	}	
	
	// Call reduceF for each distinct key, write to outFile
	file, err := os.OpenFile(outFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		log.Fatal("Reduce: %", err)
	}
	enc := json.NewEncoder(file)
	for _, keyval := range sortedInputs {
		output := reduceF(keyval.key, keyval.values)
		// encode output to JSON & write to outFile
		if err = enc.Encode(KeyValue{keyval.key, output}); err != nil {
			log.Fatal("Reduce: %", err)
		}
	}
}
