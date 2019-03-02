package main

import (
	"encoding/binary"
	"encoding/json"
	"github.com/aws/aws-lambda-go/events"
	"math"
	"math/rand"
	"testing"
)

func createKinesisEvent() events.KinesisEvent {
	// Make some raw bindary data
	rawData := createRawData()

	// Add the raw data to a JSON object
	preSend := unparsedAppleWatch3Data{
		WatchPosition: watchPosition{"uuid1", 1},
		RawData:       rawData,
	}
	jsonEncoded, _ := json.Marshal(preSend)

	// Add the JSON to AWS Kinesis records (can be many records per event)
	ker := events.KinesisEventRecord{
		Kinesis: events.KinesisRecord{
			Data: jsonEncoded,
		},
	}

	// Add two records to the event
	return events.KinesisEvent{
		Records: []events.KinesisEventRecord{ker, ker},
	}
}

func createRawData() []byte {
	// Byte arrays for storage
	var rawData = make([]byte, 0)
	var tempBytes = make([]byte, 8)
	// Add row by row
	for i := 0; i < 10; i++ {
		// Timestamp (uint64) data
		binary.LittleEndian.PutUint64(tempBytes, rand.Uint64())
		rawData = append(rawData, tempBytes...)

		// Float64 data (10 rows)
		for j := 0; j < 10; j++ {
			binary.LittleEndian.PutUint64(tempBytes, math.Float64bits(rand.Float64()))
			rawData = append(rawData, tempBytes...)
		}
	}
	return rawData
}

func TestLambdaHandler(*testing.T) {
	lambdaMain(nil, createKinesisEvent())
}
