package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"
)

type Message struct {
	QRMetadata  string `json:"qr_metadata"`
	ReferenceID string `json:"reference_id"`
}

func main() {
	if len(os.Args) != 3 {
		fmt.Println("Usage: go run main.go <input_file.csv> <output_file.csv>")
		return
	}

	inputFileName := os.Args[1]
	outputFileName := os.Args[2]

	inputFile, err := os.Open(inputFileName)
	if err != nil {
		fmt.Printf("Error opening input file %s: %v\n", inputFileName, err)
		return
	}
	defer func(inputFile *os.File) {
		err := inputFile.Close()
		if err != nil {

		}
	}(inputFile)

	reader := csv.NewReader(inputFile)
	records, err := reader.ReadAll()
	if err != nil {
		fmt.Printf("Error reading input CSV %s: %v\n", inputFileName, err)
		return
	}

	outputFile, err := os.Create(outputFileName)
	if err != nil {
		fmt.Printf("Error creating output file %s: %v\n", outputFileName, err)
		return
	}
	defer func(outputFile *os.File) {
		err := outputFile.Close()
		if err != nil {

		}
	}(outputFile)

	writer := csv.NewWriter(outputFile)
	defer writer.Flush()

	header := []string{"iam_id", "account_no", "timestamp", "reference_id"}
	if err := writer.Write(header); err != nil {
		fmt.Printf("Error writing header to output CSV: %v\n", err)
		return
	}

	// Load the Singapore time zone
	sgt, err := time.LoadLocation("Asia/Singapore")
	if err != nil {
		fmt.Printf("Error loading Singapore time zone: %v\n", err)
		return
	}

	for _, record := range records[1:] { // Skip the header row

		iamID := record[2]
		timestampRaw := record[0]
		messageRaw := record[1]

		// Parse the UTC timestamp
		utcTime, err := time.Parse(time.RFC3339, timestampRaw)
		if err != nil {
			fmt.Printf("Error parsing timestamp %s: %v\n", timestampRaw, err)
			continue
		}

		// Convert to SGT and format
		sgtTime := utcTime.In(sgt)
		timestampSGT := sgtTime.Format("1-2-2006 15:04:05")

		// Extract the JSON part from the message
		jsonStart := strings.Index(messageRaw, "{")
		jsonEnd := strings.LastIndex(messageRaw, "}")
		if jsonStart == -1 || jsonEnd == -1 {
			fmt.Printf("Skipping record, no valid JSON found: %s\n", messageRaw)
			continue
		}
		jsonStr := messageRaw[jsonStart : jsonEnd+1]

		// Unmarshal the entire JSON content
		var message Message
		err = json.Unmarshal([]byte(jsonStr), &message)
		if err != nil {
			fmt.Printf("Error unmarshalling JSON for record: %v\n", err)
			continue
		}

		// Extract Account No from the message
		accountNo := strings.TrimSuffix(strings.TrimPrefix(message.ReferenceID, "MQR-00"), "-PP")

		transformedRecord := []string{
			iamID,
			accountNo,
			timestampSGT,
			message.ReferenceID,
		}
		if err := writer.Write(transformedRecord); err != nil {
			fmt.Printf("Error writing record to output CSV: %v\n", err)
			return
		}
	}

	fmt.Printf("CSV transformation completed successfully. Output written to %s\n", outputFileName)
}
