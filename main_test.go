package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"github.com/aws/aws-lambda-go/events"
	"math"
	"math/rand"
	"testing"
)

func createKinesisEvent(rawData []byte) events.KinesisEvent {
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

func TestLambdaMain(t *testing.T) {
	lambdaMain(nil, createKinesisEvent(createRawData()))
}

func TestExtractKinesisData(t *testing.T) {
	// Create a kinesis event with the raw data used twice
	rawData := createRawData()
	kinesisEvent := createKinesisEvent(rawData)

	// Extract the raw data from the kinesis event
	halfParsedData := extractKinesisData(kinesisEvent)

	// Check there are exactly two outputs
	const expectedNumRecords = 2
	if len(halfParsedData) != expectedNumRecords {
		t.Errorf("Did not extract exactly 2 records")
	}

	// Check the first output
	if bytes.Compare(rawData, halfParsedData[0].RawData) != 0 {
		t.Errorf("First extracted data set was not equal to input")
	}

	// Check the second output
	if bytes.Compare(rawData, halfParsedData[0].RawData) != 0 {
		t.Errorf("Second extracted data set was not equal to input")
	}
}
