package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/xitongsys/parquet-go/ParquetFile"
	"github.com/xitongsys/parquet-go/ParquetWriter"
	"github.com/xitongsys/parquet-go/parquet"
	"log"
	"math"
	"os"
	"runtime"
	"time"
)

type appleWatch3Row struct {
	Ts uint64  `parquet:"name=ts, type=UINT_64"`
	Rx float64 `parquet:"name=rx, type=DOUBLE"`
	Ry float64 `parquet:"name=ry, type=DOUBLE"`
	Rz float64 `parquet:"name=rz, type=DOUBLE"`
	Rl float64 `parquet:"name=rl, type=DOUBLE"`
	Pt float64 `parquet:"name=pt, type=DOUBLE"`
	Yw float64 `parquet:"name=yw, type=DOUBLE"`
	Ax float64 `parquet:"name=ax, type=DOUBLE"`
	Ay float64 `parquet:"name=ay, type=DOUBLE"`
	Az float64 `parquet:"name=az, type=DOUBLE"`
	Hr float64 `parquet:"name=hr, type=DOUBLE"`
}

type unparsedAppleWatch3Data struct {
	WatchPosition watchPosition `json:"WatchPosition"`
	RawData       []byte        `json:"RawData"`
}

type parsedAppleWatch3Data struct {
	WatchPosition  watchPosition    `json:"WatchPosition"`
	StructuredData []appleWatch3Row `json:"RawData"`
}

type watchPosition struct {
	PatientID string `json:"PatientID"`
	Limb      uint8  `json:"Limb"`
}

func main() {
	lambda.Start(lambdaMain)
}

func lambdaMain(_ context.Context, event events.KinesisEvent) {
	// Decode the data
	halfParsedData := extractKinesisData(event)
	parsedData := decodeBinaryWatchData(halfParsedData)

	// Combine the data into one per user-limb
	combined := combineData(parsedData)

	// Write to parquet and transmit
	compressAndSend(combined)
}

func extractKinesisData(event events.KinesisEvent) []unparsedAppleWatch3Data {
	records := extractKinesisRecords(event)
	return decodeJSON(records)
}

func extractKinesisRecords(event events.KinesisEvent) [][]byte {
	// Make space to store the decoded data
	decodedRecords := make([][]byte, len(event.Records))
	for i, record := range event.Records {
		decodedRecords[i] = record.Kinesis.Data
	}
	return decodedRecords
}

func decodeJSON(jsonRows [][]byte) []unparsedAppleWatch3Data {
	decodedStructs := make([]unparsedAppleWatch3Data, len(jsonRows))
	for i, b64 := range jsonRows {
		err := json.Unmarshal(b64, &decodedStructs[i])
		if err != nil {
			log.Println("Unable to decode JSON data ", err)
		}
	}
	return decodedStructs
}

func decodeBinaryWatchData(undecodedData []unparsedAppleWatch3Data) []parsedAppleWatch3Data {
	parsedWatchData := make([]parsedAppleWatch3Data, len(undecodedData))
	for i, unparsed := range undecodedData {
		parsedWatchData[i].WatchPosition = unparsed.WatchPosition
		parsedWatchData[i].StructuredData = decodeBinaryData(unparsed.RawData)
	}
	return parsedWatchData
}

func decodeBinaryData(raw []byte) []appleWatch3Row {
	const bytesPerNumber int = 8
	const numFields int = 11
	const rowSize = bytesPerNumber * numFields

	// Check if there are an integer number of rows
	if len(raw)%rowSize != 0 {
		log.Println("Last row of binary data may be corrupted.")
	}

	// Calculate number of rows to be read
	var n int = len(raw) / rowSize
	rows := make([]appleWatch3Row, n)

	// Parse each row
	offset := 0
	for i := 0; i < n; i++ {

		// Store the numbers as an intermediary uint64
		nums := make([]uint64, numFields)

		// Read each row in the field
		for j := 0; j < numFields; j++ {
			nums[j] = binary.LittleEndian.Uint64(raw[offset : offset+bytesPerNumber])
			offset += bytesPerNumber
		}

		// Convert to floats and put inside struct
		rows[i] = appleWatch3Row{
			nums[0],
			math.Float64frombits(nums[1]),
			math.Float64frombits(nums[2]),
			math.Float64frombits(nums[3]),
			math.Float64frombits(nums[4]),
			math.Float64frombits(nums[5]),
			math.Float64frombits(nums[6]),
			math.Float64frombits(nums[7]),
			math.Float64frombits(nums[8]),
			math.Float64frombits(nums[9]),
			math.Float64frombits(nums[10]),
		}
	}
	return rows
}

func combineData(watchData []parsedAppleWatch3Data) []parsedAppleWatch3Data {
	m := make(map[string]*parsedAppleWatch3Data)

	// Concatenate any data from the same user's limb
	for _, data := range watchData {
		key := fmt.Sprintf("%s-%d", data.WatchPosition.PatientID, data.WatchPosition.Limb)
		oldData, exists := m[key]
		if exists {
			m[key].StructuredData = append(oldData.StructuredData, data.StructuredData...)
		} else {
			m[key] = &data
		}
	}

	// Return only the values (not the keys)
	combined := make([]parsedAppleWatch3Data, len(m))
	i := 0
	for _, val := range m {
		combined[i] = *val
		i++
	}
	return combined
}

func compressAndSend(allData []parsedAppleWatch3Data) {
	// Create a reusable connection to the S3 bucket
	s3Connection := createS3Connection()

	for _, data := range allData {
		// Create unique filenames
		timestamp := time.Now().UnixNano()
		commonFilename := fmt.Sprintf("%s-%d-%d.parquet", data.WatchPosition.PatientID, data.WatchPosition.Limb, timestamp)
		localFilename := fmt.Sprintf("/tmp/%s", commonFilename)
		s3Filename := fmt.Sprintf("%s/%s", data.WatchPosition.PatientID, commonFilename)

		// Write the data to a parquet file
		file, writer := createParquetFile(localFilename)
		writeDataToParquet(data.StructuredData, writer)
		closeParquetFile(file, writer)

		// Transmit the file to S3
		uploadToS3(s3Connection, localFilename, s3Filename)
	}
}

func createParquetFile(filename string) (ParquetFile.ParquetFile, *ParquetWriter.ParquetWriter) {
	// Create a file to write to
	fileWriter, err := ParquetFile.NewLocalFileWriter(filename)
	if err != nil {
		log.Fatal("Unable to create parquet file ", err)
	}

	// Create a file writer for that file
	cpuThreads := int64(runtime.NumCPU())
	parquetWriter, err := ParquetWriter.NewParquetWriter(fileWriter, new(appleWatch3Row), cpuThreads)
	if err != nil {
		log.Fatal("Unable to create parquet writer ", err)
	}

	// Use default row group size and compression codecs
	parquetWriter.RowGroupSize = 128 * 1024 * 1024 // 128 MB
	parquetWriter.CompressionType = parquet.CompressionCodec_SNAPPY

	return fileWriter, parquetWriter
}

func writeDataToParquet(allData []appleWatch3Row, parquetWriter *ParquetWriter.ParquetWriter) {
	// Write each row to the file
	for _, row := range allData {
		err := parquetWriter.Write(row)
		if err != nil {
			log.Println("Error writing row to file ", err, row)
		}
	}
}

func closeParquetFile(file ParquetFile.ParquetFile, writer *ParquetWriter.ParquetWriter) {
	// Write footer to parquet file
	err := writer.WriteStop()
	if err != nil {
		log.Fatal("Unable to write footer to Parquet file ", err)
	}

	// Close the file itself
	err = file.Close()
	if err != nil {
		log.Fatal("Unable to close Parquet file ", err)
	}
}

func createS3Connection() *s3manager.Uploader {
	sess := session.Must(session.NewSession())
	return s3manager.NewUploader(sess)
}

func uploadToS3(uploader *s3manager.Uploader, localPath string, s3Path string) {
	// Open the file
	f, err := os.Open(localPath)
	if err != nil {
		log.Fatal("Unable to open file")
	}
	defer f.Close()

	// Upload the file to S3.
	_, err = uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String("kine-dmd"),
		Key:    aws.String(s3Path),
		Body:   f,
	})
	if err != nil {
		log.Fatal("Unable to upload to S3 bucket ", err)
	}
}
