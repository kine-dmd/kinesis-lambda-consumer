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

func createByteRow(intData uint64, floatData []float64) []byte {
	// Check data is correct size
	if len(floatData) != 10 {
		panic("Should be exactly 10 floats")
	}

	// Byte arrays for storage
	var rawData = make([]byte, 0)
	var tempBytes = make([]byte, 8)

	// Timestamp (uint64) data
	binary.LittleEndian.PutUint64(tempBytes, intData)
	rawData = append(rawData, tempBytes...)

	// Float64 data (10 rows)
	for _, fd := range floatData {
		binary.LittleEndian.PutUint64(tempBytes, math.Float64bits(fd))
		rawData = append(rawData, tempBytes...)
	}
	return rawData
}

func createRandomRawData() []byte {
	// Byte arrays for storage
	var rawData = make([]byte, 0)
	// Add row by row
	for i := 0; i < 10; i++ {
		var floatData = make([]float64, 10)
		for j := range floatData {
			floatData[j] = rand.Float64()
		}
		rawData = append(rawData, createByteRow(rand.Uint64(), floatData)...)
	}
	return rawData
}

func TestLambdaMain(t *testing.T) {
	lambdaMain(nil, createKinesisEvent(createRandomRawData()))
}

func TestExtractKinesisData(t *testing.T) {
	// Create a kinesis event with the raw data used twice
	rawData := createRandomRawData()
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

func TestDecodeBinaryData(t *testing.T) {
	// Make 11 random bits of data
	var uIntData uint64 = rand.Uint64()
	var floatData = make([]float64, 10)
	for i := range floatData {
		floatData[i] = rand.Float64()
	}

	// Convert to the byte array and attempt to parse back
	byteData := createByteRow(uIntData, floatData)
	watchRow := decodeBinaryData(byteData)

	if watchRow[0].Ts != uIntData {
		t.Errorf("Incorrect TS decoded")
	}
	if watchRow[0].Rx != floatData[0] {
		t.Errorf("Incorrect RX decoded")
	}
	if watchRow[0].Ry != floatData[1] {
		t.Errorf("Incorrect RY decoded")
	}
	if watchRow[0].Rz != floatData[2] {
		t.Errorf("Incorrect RZ decoded")
	}
	if watchRow[0].Rl != floatData[3] {
		t.Errorf("Incorrect RL decoded")
	}
	if watchRow[0].Pt != floatData[4] {
		t.Errorf("Incorrect PT decoded")
	}
	if watchRow[0].Yw != floatData[5] {
		t.Errorf("Incorrect YW decoded")
	}
	if watchRow[0].Ax != floatData[6] {
		t.Errorf("Incorrect AX decoded")
	}
	if watchRow[0].Ay != floatData[7] {
		t.Errorf("Incorrect AY decoded")
	}
	if watchRow[0].Az != floatData[8] {
		t.Errorf("Incorrect AZ decoded")
	}
	if watchRow[0].Hr != floatData[9] {
		t.Errorf("Incorrect HR decoded")
	}
}
