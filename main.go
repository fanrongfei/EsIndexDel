package main

import (
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	log "github.com/cihub/seelog"
	cron "github.com/robfig"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
	"time"
)

var (
	esIP           string //es服务ip
	esPort         int    //es端口
	retentionCycle int    //日志保存周期
	cronTime       string //
	user           string //es账户
	password       string //
	indexArr       []string//目标索引
)

func main() {
	eip := flag.String("ip", "10.100.25.23", "elasticsearcher节点ip")
	eport := flag.Int("port", 9200, "elasticsearcher节点port")
	rc := flag.Int("rc", 14, "日志保存周期(天)")
	ct := flag.String("ct", "0 0 1 * * ?", "定时任务时间,默认每天凌晨1点执行")
	u := flag.String("user", "elastic", "elasticsearcher用户")
	p := flag.String("password", "zyzlxylx1.t2021", "elasticsearcher用户密码")
	indexarr:=flag.String("indexArr","logstash-nginx_access-*|logstash-bianque-*","定时删除的目标索引，如若添加新索引使用|隔开")
	flag.Parse()
	esIP = *eip
	esPort = *eport
	retentionCycle = *rc
	cronTime = *ct
	user = *u
	password = *p
	cronTime = *ct
	indexArr=trimeArr(strings.Split(*indexarr,"|"))
	deleteIndex()
}
//排除空白行及开头或末尾添加|
func trimeArr(arr []string)(arrN []string){
	for _,v:=range arr{
		if strings.Trim(v," ")!=""{
			arrN = append(arrN, v)
		}
	}
	return
}
//定时删除逻辑
func deleteIndex() {
	defer func() {
		if e := recover(); e != nil {
			log.Errorf("func_deleteIndex(%s)_recover:%v \n", cronTime, e)
		}
	}()
	cr := cron.New()
	cr.AddFunc(cronTime, func() {
		for _,v:=range indexArr{
			 deleteIndexFromEs(v)
		}
	})
	cr.Start()
	for {
		time.Sleep(time.Second)
	}
}

//删index
func deleteIndexFromEs(indexS string) {
	defer log.Flush()
	var index int
	for {
		//默认是某个具体的索引
		var indexName =fmt.Sprintf("http://%s:%d/%s", esIP, esPort,indexS)
		//识别是否以*结束的通配符
		if strings.HasSuffix(indexS,"*"){
			indexName= fmt.Sprintf("http://%s:%d/%s-%s", esIP, esPort,indexS,time.Now().Add(time.Hour*24*time.Duration(-retentionCycle-1-index)).Format("2006.01.02"))
		}
		//判断索引是否存在
		if indexCount(indexName) == 0 {
			log.Infof("index:%s no record and return", indexName)
			return
		}
		log.Infof("===================开始删除索引:%s====================", indexName)
		var client http.Client
		request, err := http.NewRequest("DELETE", indexName, nil)
		if err != nil {
			log.Errorf("new request error=%v", err)
			return
		}
		request.Header.Add("Authorization", fmt.Sprintf("Basic %s", base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("%s:%s", user, password)))))
		req, err := client.Do(request)
		if err != nil {
			log.Errorf("new request error=%v", err)
			return
		}
		content, _ := ioutil.ReadAll(req.Body)
		log.Infof(string(content))
		req.Body.Close()
		index++
		time.Sleep(time.Second)
	}
}
func indexCount(indexname string) (count int) {
	var client http.Client
	request, err := http.NewRequest("GET", indexname+"/_count", nil)
	if err != nil {
		log.Errorf(" indexCount_new request error=%v", err)
		return
	}
	request.Header.Add("Authorization", fmt.Sprintf("Basic %s", base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("%s:%s", user, password)))))
	resp, err := client.Do(request)
	if err != nil {
		log.Errorf("indexCount_error:%v", err)
		return
	}
	body, _ := ioutil.ReadAll(resp.Body)
	defer resp.Body.Close()
	var result struct {
		Count int `json:"count"`
	}
	json.Unmarshal(body, &result)
	return result.Count
}

//初始化配置文件及本地日志
func init() {
	if _,err:=os.Stat("./log");err!=nil{
		if err=os.MkdirAll("./log",os.ModePerm);err!=nil{
			log.Errorf("mkdir folder error:%v",err)
		}
	}
	logger, err := log.LoggerFromConfigAsFile("./log_client.xml")
	if err != nil {
		fmt.Println(err)
		log.Errorf("config_init|err:%v", err)
		return
	}
	log.ReplaceLogger(logger)
	log.Flush()
	fmt.Println("log 完成初始化")
	return
}
