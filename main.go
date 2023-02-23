package main

import (
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

const (
	AWS_S3_REGION = "us-east-1"
	AWS_S3_BUCKET = "test-data-feb-22"
)

type WorkStatement map[string]string

type Employee map[string]string

var notDetermined int = 0

var wg sync.WaitGroup

func main() {

	employeeFile := "Employees.csv"
	workStatementFile := "WorkStatements.csv"

	wg.Add(2)

	go downloadFromS3(workStatementFile)
	go downloadFromS3(employeeFile)

	wg.Wait()

	workData := ReadFile(workStatementFile)

	gracePeriod := 15

	WorkStatementList := WorkStatementListMaker(workData)

	employeeData := ReadFile(employeeFile)

	EmployeeList := EmployeeListMaker(employeeData)

	complaint := 0
	nonComplaint := 0

	now := time.Now()

	for k := range EmployeeList {
		// var ComplainceStatus string
		// var AccessStatus string

		if v, ok := WorkStatementList[k]; ok {

			if v == "" || EmployeeList[k] == "" {

				// fmt.Println("-------------------------------------------empty")
				notDetermined++
				// fmt.Printf("Employee id:= %s, Status Complaince: Not Determined, Access Allowed: No\n", k)
				continue
			}

			if !regexp.MustCompile(`^[0-9]*$`).MatchString(k) {
				// fmt.Println("Non Numeric: &&&&&&", k)
				delete(WorkStatementList, k)
				notDetermined++
				continue
			}

			if v == "Approved" {

				projectDate := StringToTime(EmployeeList[k])

				timeWithOutGracePeriod := now.AddDate(0, 0, -gracePeriod)
				difference := projectDate.Sub(timeWithOutGracePeriod)
				if difference >= 0 {
					complaint++
					// ComplainceStatus = "Complaint "
					// AccessStatus = "Yes"

				} else if difference < 0 {
					nonComplaint++
					// ComplainceStatus = "Not Complaint "
					// AccessStatus = "No"
				}

			} else {
				nonComplaint++
				// ComplainceStatus = "Not Complaint "
				// AccessStatus = "No"
			}

			delete(WorkStatementList, k)

			// fmt.Printf("Employee id:= %s, Status Complaince: %s, Access Allowed: %s\n", k, ComplainceStatus, AccessStatus)
		} else if !regexp.MustCompile(`^[0-9]*$`).MatchString(k) {
			// fmt.Println("Non Numeric: ----- ", k)
			// delete(WorkStatementList, k)
			notDetermined++
			// continue
		} else {
			nonComplaint++
			// fmt.Printf("Employee id:= %s, Status Complaince: Not Complaint, Access Allowed: No\n", k)

		}

	}

	for k := range WorkStatementList {

		if !regexp.MustCompile(`^[0-9]*$`).MatchString(k) {
			// fmt.Println("Non Numeric: **** ", k)
			notDetermined++
		} else {
			nonComplaint++
		}
		// var ComplainceStatus string = "Not Complaint"
		// var AccessStatus string = "No"

	}

	fmt.Println("Count of Complaint: ", complaint)
	fmt.Println("Count of Not Complaint: ", nonComplaint)
	fmt.Println("Count of Not Determined: ", notDetermined)

}

func downloadFromS3(item string) {

	file, err := os.Create(item)

	if err != nil {
		exitErrorf("Unable to open file %q, %v", item, err)
	}

	defer file.Close()

	// Initialize a session in us-west-2 that the SDK will use to load
	// credentials from the shared credentials file ~/.aws/credentials.

	sess, _ := session.NewSessionWithOptions(session.Options{
		Profile: "default",
		Config: aws.Config{
			Region: aws.String(AWS_S3_REGION),
		},
	})

	downloader := s3manager.NewDownloader(sess)

	numBytes, err := downloader.Download(file,
		&s3.GetObjectInput{
			Bucket: aws.String(AWS_S3_BUCKET),
			Key:    aws.String(item),
		})

	if err != nil {
		exitErrorf("Unable to download item %q, %v", item, err)
	}

	fmt.Println("Downloaded", file.Name(), numBytes, "bytes")

	wg.Done()
}

func exitErrorf(msg string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, msg+"\n", args...)
	os.Exit(1)
}

func StringToTime(datestring string) time.Time {

	date, error := time.Parse("01/02/2006", datestring)

	if error != nil {
		fmt.Println(error)
		return date
	}

	return date

}

func EmployeeListMaker(data [][]string) Employee {

	rec := make(Employee)
	for i, line := range data {
		if i > 0 { // omit header line
			id := ""
			projectedEnd := ""
			for j, field := range line {
				if j == 0 {
					id = strings.TrimSpace(field)
				} else if j == 3 {
					projectedEnd = strings.TrimSpace(field)
				}
			}

			if id == "" {
				notDetermined++
				continue
			}

			rec[id] = projectedEnd

		}
	}
	return rec

}

func ReadFile(filename string) [][]string {

	f, err := os.Open(filename)
	if err != nil {
		log.Fatal(err)
	}

	// remember to close the file at the end of the program
	defer f.Close()

	// read csv values using csv.Reader
	csvReader := csv.NewReader(f)
	data, err := csvReader.ReadAll()
	if err != nil {
		log.Fatal(err)
	}
	return data
}

func WorkStatementListMaker(data [][]string) WorkStatement {

	rec := make(WorkStatement)
	for i, line := range data {
		if i > 0 { // omit header line
			id := ""
			progress := ""
			for j, field := range line {
				if j == 0 {
					id = strings.TrimSpace(field)
				} else if j == 2 {
					progress = strings.TrimSpace(field)
				}

			}

			if id == "" {
				// fmt.Println("empyt --------")
				notDetermined++
				continue
			}

			rec[id] = progress

		}
	}
	return rec
}
