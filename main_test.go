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
	// Run a full end to end test of the lambda main
	//lambdaMain(nil, createKinesisEvent(createRandomRawData()))
	t.Skip()
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
	decodeAndCheckData(byteData, uIntData, t, floatData)
}

func TestDecodeBinaryDataWithCorruptedEnd(t *testing.T) {
	// Make 11 random bits of data
	var uIntData uint64 = rand.Uint64()
	var floatData = make([]float64, 10)
	for i := range floatData {
		floatData[i] = rand.Float64()
	}

	// Make a row and then append some data to end to simulate cut off second row
	byteData := createByteRow(uIntData, floatData)
	byteData = append(byteData, make([]byte, 5)...)

	// Check that the parsed rows has not been affected
	decodeAndCheckData(byteData, uIntData, t, floatData)
}

func decodeAndCheckData(byteData []byte, uIntData uint64, t *testing.T, floatData []float64) {
	// Try and deocde the data
	watchRow := decodeBinaryData(byteData)

	// Check the inputs and outputs match
	if len(watchRow) != 1 {
		t.Errorf("Wrong number of rows")
	}
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

func TestCombineDataDifferentLimbs(t *testing.T) {
	// Make a list of parsed data from same user but different limbs
	parsed := []parsedAppleWatch3Data{createParsedWatchData("a", 1),
		createParsedWatchData("a", 2)}

	// Try combining the data
	combined := combineData(parsed)

	// Check there are three outputs
	if len(combined) != 2 {
		t.Errorf("Incorrect number of unique id-limb pairs")
	}

	// Check rows have not been internally combined
	if len(combined[0].StructuredData) != 1 {
		t.Errorf("Incorrect number of rows in first parsed watch data.")
	}
	if len(combined[1].StructuredData) != 1 {
		t.Errorf("Incorrect number of rows in second parsed watch data.")
	}
}

func TestCombineDataSameLimbs(t *testing.T) {
	// Make a list of parsed data from same user but different limbs
	parsed := []parsedAppleWatch3Data{createParsedWatchData("a", 1),
		createParsedWatchData("a", 1)}

	// Try combining the data
	combined := combineData(parsed)

	// Check there are three outputs
	if len(combined) != 1 {
		t.Errorf("Incorrect number of unique id-limb pairs")
	}

	// Check rows have not been internally combined
	if len(combined[0].StructuredData) != 2 {
		t.Errorf("Incorrect number of rows in combined data.")
	}
}

func TestCombineDataMultiUsers(t *testing.T) {
	// Make a list of parsed data from same user but different limbs
	parsed := []parsedAppleWatch3Data{createParsedWatchData("a", 1),
		createParsedWatchData("b", 1),
		createParsedWatchData("a", 1)}

	// Try combining the data
	combined := combineData(parsed)

	// Check there are three outputs
	if len(combined) != 2 {
		t.Errorf("Incorrect number of unique id-limb pairs")
	}

	// Check rows have not been internally combined
	if len(combined[0].StructuredData) != 2 && combined[0].WatchPosition.PatientID == "a" {
		t.Errorf("Incorrect number of rows in combined data.")
	}
	if len(combined[0].StructuredData) != 1 && combined[0].WatchPosition.PatientID == "b" {
		t.Errorf("Incorrect number of rows in combined data.")
	}
}

func createParsedWatchData(s string, n int) parsedAppleWatch3Data {
	// Use the same number (n) and same string (s) for all values
	f := float64(n)
	row := appleWatch3Row{uint64(n), f, f, f, f, f, f, f, f, f, f}

	// Make a watch position
	position := watchPosition{s, uint8(n)}

	// Combine the two to make a completed parsed watch data
	return parsedAppleWatch3Data{position, []appleWatch3Row{row}}
}
