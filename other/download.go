package main

import (
	"fmt"
	"gopkg.in/mgo.v2"
	"time"
	"gopkg.in/mgo.v2/bson"
	"bytes"
	"archive/zip"
	"io"
	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	"sync"
	"net/http"
	"github.com/gorilla/mux"
	"strconv"
	"log"
)

func main() {
	//execute("22130263000251002", 0, 9999)
	log.Fatal(run())
}

var session *mgo.Session
var batch int = 1000
var bucket *oss.Bucket
func init(){
	dial_info := &mgo.DialInfo{
		Addrs:  []string{"ip1:port","ip2:port"},
		Direct: false,
		Timeout: time.Second * 1,
		Database: "test",
		Source: "admin",
		Username: "test",
		Password: "test123",
		PoolLimit: 1024,
	}
	session, _ = mgo.DialWithInfo(dial_info)
	// 创建OSSClient实例。
	client, err := oss.New("yourEndpoint", "yourAccessKeyId", "yourAccessKeySecret")
	if err != nil {
		fmt.Println("Error:", err)
	}
	// 获取存储空间。
	bucket, err = client.Bucket("yourBucketName")
	if err != nil {
		fmt.Println("Error:", err)
	}

}
type book struct {
	Cbid    string
	Ccid    string
	Sort    int
	Content string
}

func execute(responseWriter http.ResponseWriter,request *http.Request){
	t1 := time.Now().Second()
	cbid := request.FormValue("cbid")
	start,_ := strconv.Atoi(request.FormValue("start"))
	end,_ := strconv.Atoi(request.FormValue("end"))
	space := end - start + 1
	batchNum := space%batch
	if batchNum == 0 {
		batchNum = space/batch
	}else {
		batchNum = space/batch + 1
	}
	saveBuffer := make([]byte,0)
	buffer := bytes.NewBuffer(saveBuffer)
	zipWriter := zip.NewWriter(buffer)
	var start1, end1 int
	var mutex sync.Mutex
	var wg sync.WaitGroup
	for i := 1; i <= batchNum; i++ {
		if i == 1{
			start1 = start
		}
		end1 = start1 + batch - 1
		if i == batchNum {end1 = end}
		wg.Add(1)
		go task(cbid,start1,end1,zipWriter,&wg,&mutex)
		start1 = end1 + 1
	}
	//主线程阻塞
        wg.Wait()
	t2 := time.Now().Second()
	fmt.Println("zip cost",(t2-t1))
	zipWriter.Close()
	url := uploadZipStream(buffer,cbid)
	fmt.Println("upload cost",(time.Now().Second() - t2))
	io.WriteString(responseWriter,url)
}

func download(cbid string, start int, end int) *[]book{
	books := make([]book,0)
	session.DB("test").C("book_content").Find(bson.M{"cbid":cbid,"sort":bson.M{"$gte":start,"$lte":end}}).Batch(200).All(&books)
	return &books
}

func compressData(zipWriter *zip.Writer, books *[]book,cbid string,mutex *sync.Mutex){
	for _ ,book := range *books{
		mutex.Lock()
		fw,err := zipWriter.Create(cbid + "\\" + book.Ccid + ".txt")
		if err != nil{
			fmt.Println("createErroe:",err)
		}
		_,err = fw.Write([]byte(book.Content))
		if err != nil{
			fmt.Println("writeError:",err)
		}
		zipWriter.Flush()
		mutex.Unlock()
	}
}

func uploadZipStream(reader io.Reader,cbid string) string{
	// 上传文件流。
	err := bucket.PutObject("reader/zip/123de34der/" + cbid + ".zip", reader)
	if err != nil {
		fmt.Println("Error:", err)
	}
	fmt.Println("over")
	return "http://cdn.ibczy.com/reader/zip/123de34der/" + cbid + ".zip"
	//http://cdn.ibczy.com/reader/zip/123de34der/22130263000251002.zip
}

func task(cbid string,start int,end int,zipWriter *zip.Writer,wg *sync.WaitGroup,mutex *sync.Mutex){
	books := download(cbid,start,end)
	compressData(zipWriter,books,cbid,mutex)
	wg.Done()
}

func makeHandler() http.Handler{
	muxRouter := mux.NewRouter()
	muxRouter.HandleFunc("/getUrl",execute).Methods("Get")
	return muxRouter
}

func run() error{
	server := &http.Server{
		Addr: "localhost:8994",
		Handler: makeHandler(),
		ReadTimeout: 10 * time.Second,
		WriteTimeout: 10 * time.Second,
		MaxHeaderBytes: 1 << 20,
	}
	if err := server.ListenAndServe(); err != nil{
		return err
	}
	return nil
}
