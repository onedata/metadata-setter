package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"sync"

	//"path/filepath"
	"strings"
	"time"

	"github.com/syrinsecurity/gologger"
	"github.com/valyala/fasthttp"
)

var (
	logger          = gologger.NewCustomLogger("./logs", ".txt", 20000)
	errorLogger     = gologger.NewCustomLogger("./error-logs", ".txt", 20000)
	oneproviderUrls = [2]string{
		"https://wcss3.hub.konektom.pl",
		"https://wcss1.hub.konektom.pl",
	}
	buckets = map[string]string{
		//		"mnk21":          "replication-test-mnk",
		"mnk21": "bucket-mnk",
		"bn25":  "bucket-bn",
		"bn27":  "bucket-bn",
		"bn28":  "bucket-bn",
		"bn29":  "bucket-bn",
		"bn30":  "bucket-bn",
		"bn35":  "bucket-bn",
		"bn36":  "bucket-bn",
		"bn37":  "bucket-bn",
		"bn38":  "bucket-bn",
		"bn39":  "bucket-bn",
		"bn40":  "bucket-bn",
		"bn46":  "bucket-bn",
		"nac23": "bucket-nac",
		"nac24": "bucket-nac",
		"nac26": "bucket-nac",
		"nac31": "bucket-nac",
		"nac32": "bucket-nac",
		"nac33": "bucket-nac",
		"nac34": "bucket-nac",
		"nac":   "bucket-nac",
	}
)

type Response struct {
	FileId string `json:"fileId"`
}

type MD5Response struct {
	MD5Sum string `json:"s3.content.md5"`
}

func PrettyPrint(i interface{}) string {
	s, _ := json.MarshalIndent(i, "", "\t")
	return string(s)
}

const consumerCount int = 60

func produce(jobs chan<- string, f *os.File) {
	fileScanner := bufio.NewScanner(f)
	fileScanner.Split(bufio.ScanLines)
	for fileScanner.Scan() {
		line := fileScanner.Text()
		jobs <- line
	}
	close(jobs)
}

func consume(i int, jobs <-chan string, token string, fastClient *fasthttp.Client, wg *sync.WaitGroup) {
	defer wg.Done()

	//prepare requests
	fileIdRequest := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(fileIdRequest)
	fileIdRequest.Header.SetMethod(fasthttp.MethodPost)
	fileIdRequest.Header.Set("X-Auth-Token", token)
	fileIdResponse := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(fileIdResponse)

	metadataRequest := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(metadataRequest)
	metadataRequest.Header.SetContentType("application/json")
	metadataRequest.Header.SetMethod(fasthttp.MethodPut)
	metadataRequest.Header.Set("X-Auth-Token", token)
	metadataRequest.Header.Set("X-CDMI-Specification-Version", "1.1.1")
	metadataResponse := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(metadataResponse)

	// Perform the request
	for msg := range jobs {
		splitOutput := strings.Split(msg, " ")
		md5sum := splitOutput[0]
		splitPath := strings.Split(strings.Join(splitOutput[2:], " "), "/") //there are 2 space bars between md5 and filePath
		splitPath[4] = buckets[splitPath[3]]
		filePath := strings.ReplaceAll(strings.Join(splitPath[4:], "/"), "#", "%23")
		filePath = strings.ReplaceAll(filePath, " ", "%20")
		requestURL := fmt.Sprintf("%s/api/v3/oneprovider/lookup-file-id/%s", oneproviderUrls[i%len(oneproviderUrls)], filePath)
		fileIdRequest.SetRequestURI(requestURL)

		fileId := ""
		for {
			err := fastClient.DoTimeout(fileIdRequest, fileIdResponse, time.Duration(time.Second*20))
			if err != nil {
				fmt.Printf("Client request failed: %s\n", err)
				continue
			}
			if fileIdResponse.StatusCode() != fasthttp.StatusOK {
				fmt.Printf("Expected status code %d but got %d for url %s, file path %s\n", fasthttp.StatusOK, fileIdResponse.StatusCode(), requestURL, filePath)
				continue
			}
			// Verify the content type
			contentType := fileIdResponse.Header.Peek("Content-Type")
			if bytes.Index(contentType, []byte("application/json")) != 0 {
				fmt.Printf("Expected content type application/json but got %s\n", contentType)
				continue
			}
			resBody := fileIdResponse.Body()
			if len(resBody) == 0 {
				fmt.Printf("Response body empty. Retrying file %s\n", filePath)
				continue
			}
			var result Response
			if err := json.Unmarshal(resBody, &result); err != nil { // Parse []byte to go struct pointer
				fmt.Println("Can not unmarshal JSON")
				continue
			}
			fileId = result.FileId
			break
		}

		//		metadataRequest.Header.SetMethod(fasthttp.MethodDelete)
		metadataRequest.Header.SetMethod(fasthttp.MethodPut)
		metadataRequest.SetBody([]byte(fmt.Sprintf(`{"s3.content.md5":"%s"}`, md5sum)))
		//metadataRequest.SetBody([]byte(`{"keys" : ["metadata","s3.content.md5","sha256_key","md5_key"]}`))
		requestURL = fmt.Sprintf("%s/api/v3/oneprovider/data/%s/metadata/xattrs", oneproviderUrls[i%len(oneproviderUrls)], fileId)
		metadataRequest.SetRequestURI(requestURL)
		for {
			err := fastClient.DoTimeout(metadataRequest, metadataResponse, time.Duration(time.Second*20))
			if err != nil {
				fmt.Printf("Client request failed: %s\n", err)
				continue
			}
			if metadataResponse.StatusCode() != fasthttp.StatusNoContent {
				fmt.Printf("Setting metadata: Expected status code %d but got %d for url %s, file path %s\n", fasthttp.StatusNoContent, metadataResponse.StatusCode(), requestURL, filePath)
				continue
			}
			// Verify the content type
			contentType := metadataResponse.Header.Peek("Content-Type")
			if bytes.Index(contentType, []byte("application/json")) != 0 {
				fmt.Printf("Setting metadata: Expected content type application/json but got %s\n", contentType)
				continue
			}
			break
		}
		metadataRequest.Header.SetMethod(fasthttp.MethodGet)
		requestURL = fmt.Sprintf("%s/api/v3/oneprovider/data/%s/metadata/xattrs?attribute=s3.content.md5", oneproviderUrls[i%len(oneproviderUrls)], fileId)
		//requestURL = fmt.Sprintf("%s/api/v3/oneprovider/data/%s/metadata/xattrs", oneproviderUrls[i%len(oneproviderUrls)], fileId)
		//requestURL = fmt.Sprintf("%s/cdmi/cdmi_objectid/%s?metadata:s3.content.md5", oneproviderUrls[i%len(oneproviderUrls)], fileId)
		metadataRequest.SetRequestURI(requestURL)
		for {
			err := fastClient.DoTimeout(metadataRequest, metadataResponse, time.Duration(time.Second*20))
			if err != nil {
				fmt.Printf("Client request failed: %s\n", err)
				continue
			}
			// metadataResponse.Header.VisitAll(func(key, value []byte) {
			// 	fmt.Printf("%s: %s,", key, value)
			// })
			if metadataResponse.StatusCode() != fasthttp.StatusOK {
				fmt.Printf("Getting metadata: Expected status code %d but got %d for url %s, file path %s\n", fasthttp.StatusOK, fileIdResponse.StatusCode(), requestURL, filePath)
				continue
			}
			// Verify the content type
			contentType := metadataResponse.Header.Peek("Content-Type")
			if bytes.Index(contentType, []byte("application/json")) != 0 {
				fmt.Printf("Expected content type  application/json but got %s\n", contentType)
				continue
			}
			resBody := metadataResponse.Body()
			if len(resBody) == 0 {
				fmt.Printf("Response body empty. Retrying file %s\n", filePath)
				continue
			}
			var result MD5Response
			if err := json.Unmarshal(resBody, &result); err != nil {
				fmt.Println("Can not unmarshal JSON")
				continue
			}
			logger.Write(fmt.Sprintf("%s  %s", result.MD5Sum, strings.Join(splitOutput[2:], " ")))
			break
		}
	}
}

func main() {

	token := os.Args[1]
	inputFileName := os.Args[2]
	//fmt.Println("running with %s %s", inputFileName, token)

	jobs := make(chan string)
	wc := &sync.WaitGroup{}

	//setup logger
	go logger.Service()
	go errorLogger.Service()

	//open file
	f, err := os.Open(inputFileName)
	if err != nil {
		panic(err)
	}

	//prepare http Client
	total := consumerCount
	fastClient := &fasthttp.Client{
		Dial: func(addr string) (net.Conn, error) {
			return fasthttp.DialTimeout(addr, time.Second*10)
		},
		MaxConnsPerHost: total,
	}
	//run producers and consumers
	go produce(jobs, f)
	wc.Add(consumerCount)
	for i := 1; i <= consumerCount; i++ {
		go consume(i, jobs, token, fastClient, wc) // fileIdReq, fileIdResp, metadataReq, metadataResp, wc)
	}
	wc.Wait()

	//wait for all the logs to flush
	for logger.QueueLength() != 0 {
		time.Sleep(time.Millisecond * 100)
	}
	defer logger.Close()

	for errorLogger.QueueLength() != 0 {
		time.Sleep(time.Millisecond * 100)
	}
	defer errorLogger.Close()

	f.Close()

}
