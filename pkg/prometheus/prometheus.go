package prometheus

import (
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const (
	// CPU 5min内的使用率(腾讯云Mysql)
	tencentCpuUsagePromQL = "avg_over_time(qce_cdb_cpuuserate_max{vip='%s'}[5m])"
	/*
		https://cloud.tencent.com/document/product/248/50350#.E6.8C.87.E6.A0.87.E8.AF.B4.E6.98.8E
		qce_cdb_volumerate_max      磁盘利用率：磁盘使用空间/实例购买空间
		qce_cdb_capacity_max        磁盘占用空间：包括 MySQL 数据目录和  binlog、relaylog、undolog、errorlog、slowlog 日志空间
		qce_cdb_realcapacity_max    磁盘使用空间：仅包括 MySQL 数据目录，不含 binlog、relaylog、undolog、errorlog、slowlog 日志空间
	*/
	// 磁盘的使用率(腾讯云Mysql)
	tencentDiskUsagePromQL = "qce_cdb_volumerate_max{vip='%s'}"
	// 磁盘的总大小(腾讯云Mysql)
	tencentDiskTotalPromQL = "(qce_cdb_realcapacity_max{vip='%s'}*100)/qce_cdb_volumerate_max{vip='%s'}"
	// 磁盘的使用空间(数据)(腾讯云Mysql)
	tencentDiskUsedPromQL = "qce_cdb_realcapacity_max{vip='%s'}"
	// 磁盘的剩余空间(腾讯云Mysql)
	tencentDiskFreePromQL = "(qce_cdb_realcapacity_max{vip='%s'}*100)/qce_cdb_volumerate_max{vip='%s'}-qce_cdb_realcapacity_max{vip='%s'}"

	// 自建Mysql集群
	// CPU 5min内的使用率
	cpuUsagePromQL = "100-(avg(rate(node_cpu_seconds_total{instance_ip='%s',mode='idle'}[5m])))*100"
	// 磁盘的使用率（/data目录）
	diskUsagePromQL = "100-(node_filesystem_free_bytes{instance_ip='%s',mountpoint='/data',fstype=~'ext4|xfs'}/node_filesystem_size_bytes{instance_ip='%s',mountpoint='/data',fstype=~'ext4|xfs'})*100"
	// 磁盘的总大小（/data目录）
	diskTotalPromQL = "node_filesystem_size_bytes{instance_ip='%s',mountpoint='/data',fstype=~'ext4|xfs'}/1024/1024"
	// 磁盘的使用空间（/data目录）
	diskUsedPromQL = "(node_filesystem_size_bytes{instance_ip='%s',mountpoint='/data',fstype=~'ext4|xfs'}-node_filesystem_free_bytes{instance_ip='%s',mountpoint='/data',fstype=~'ext4|xfs'})/1024/1024"
	// 磁盘的剩余空间（/data目录）
	diskFreePromQL = "node_filesystem_free_bytes{instance_ip='%s',mountpoint='/data',fstype=~'ext4|xfs'}/1024/1024"

	// 磁盘的使用率（/目录）
	diskUsagePromQL1 = "100-(node_filesystem_free_bytes{instance_ip='%s',mountpoint='/',fstype=~'ext4|xfs'}/node_filesystem_size_bytes{instance_ip='%s',mountpoint='/',fstype=~'ext4|xfs'})*100"
	// 磁盘的总大小 （/目录）
	diskTotalPromQL1 = "node_filesystem_size_bytes{instance_ip='%s',mountpoint='/',fstype=~'ext4|xfs'}/1024/1024"
	// 磁盘的使用空间（/目录）
	diskUsedPromQL1 = "(node_filesystem_size_bytes{instance_ip='%s',mountpoint='/',fstype=~'ext4|xfs'}-node_filesystem_free_bytes{instance_ip='%s',mountpoint='/',fstype=~'ext4|xfs'})/1024/1024"
	// 磁盘的剩余空间（/目录）
	diskFreePromQL1 = "node_filesystem_free_bytes{instance_ip='%s',mountpoint='/',fstype=~'ext4|xfs'}/1024/1024"
)

var NoDataPointError = errors.New("no data points found")
var InvalidIP = errors.New("invalid ip address")

type Client struct {
	// 	http://thanos-realtime.xxx.com
	Url string `json:"url"`
}

type MatrixResult struct {
	Status    string     `json:"status"`
	Data      MatrixData `json:"data"`
	ErrorType string     `json:"errorType"`
	Error     string     `json:"error"`
}

type MatrixData struct {
	ResultType string `json:"resultType"`
	Result     []struct {
		Metric interface{}   `json:"metric"`
		Values []interface{} `json:"values"`
	} `json:"result"`
}

type VectorResult struct {
	Status    string     `json:"status"`
	Data      VectorData `json:"data"`
	ErrorType string     `json:"errorType"`
	Error     string     `json:"error"`
}

type VectorData struct {
	ResultType string `json:"resultType"`
	Result     []struct {
		Metric interface{}   `json:"metric"`
		Value  []interface{} `json:"value"`
	} `json:"result"`
}

func NewClient(url string) *Client {
	return &Client{
		Url: url,
	}
}

// Func 定义需要重试的函数类型
type Func func(string, time.Time) (float64, error)

// Retry 重试函数
func Retry(fn Func, t time.Time, url ...string) (ret float64, err error) {
	for _, u := range url {
		ret, err = fn(u, t)
		if err == nil {
			return
		}
	}
	return
}

func (c *Client) GeneralQuery(t time.Time, pql ...string) (float64, error) {
	return Retry(func(pql string, t time.Time) (float64, error) {
		vds, err := c.Query(pql, t)
		if err != nil {
			return 0, fmt.Errorf("query(%s) failed %s", pql, err)
		}
		ret, err := parseData(vds)
		if err != nil {
			return 0, fmt.Errorf("parse query(%s) result, %w", pql, err)
		}
		return ret, nil
	}, t, pql...)
}

// CpuUsage 查询cpu的使用率
func (c *Client) CpuUsage(ip string, t time.Time) (float64, error) {
	if ip == "" {
		return -1, InvalidIP
	}

	pql1 := fmt.Sprintf(tencentCpuUsagePromQL, ip)
	pql2 := fmt.Sprintf(cpuUsagePromQL, ip)
	return c.GeneralQuery(t, pql1, pql2)
}

// DiskUsage 查询磁盘的使用率
func (c *Client) DiskUsage(ip string, t time.Time) (float64, error) {
	if ip == "" {
		return -1, InvalidIP
	}

	pql1 := fmt.Sprintf(tencentDiskUsagePromQL, ip)
	pql2 := fmt.Sprintf(diskUsagePromQL, ip, ip)
	pql3 := fmt.Sprintf(diskUsagePromQL1, ip, ip)
	return c.GeneralQuery(t, pql1, pql2, pql3)
}

// DiskTotal 磁盘的总大小（MB）
func (c *Client) DiskTotal(ip string, t time.Time) (float64, error) {
	if ip == "" {
		return -1, InvalidIP
	}

	pql1 := fmt.Sprintf(tencentDiskTotalPromQL, ip, ip)
	pql2 := fmt.Sprintf(diskTotalPromQL, ip)
	pql3 := fmt.Sprintf(diskTotalPromQL1, ip)
	return c.GeneralQuery(t, pql1, pql2, pql3)
}

// DiskUsed 磁盘的使用大小(MB)
func (c *Client) DiskUsed(ip string, t time.Time) (float64, error) {
	if ip == "" {
		return -1, InvalidIP
	}

	pql1 := fmt.Sprintf(tencentDiskUsedPromQL, ip)
	pql2 := fmt.Sprintf(diskUsedPromQL, ip, ip)
	pql3 := fmt.Sprintf(diskUsedPromQL1, ip, ip)
	return c.GeneralQuery(t, pql1, pql2, pql3)
}

// DiskFree 磁盘剩余空间大小(MB)
func (c *Client) DiskFree(ip string, t time.Time) (float64, error) {
	if ip == "" {
		return -1, InvalidIP
	}

	pql1 := fmt.Sprintf(tencentDiskFreePromQL, ip, ip, ip)
	pql2 := fmt.Sprintf(diskFreePromQL, ip)
	pql3 := fmt.Sprintf(diskFreePromQL1, ip)
	return c.GeneralQuery(t, pql1, pql2, pql3)
}

// QueryRange 查询区间向量
func (c *Client) QueryRange(promQL string, start, end time.Time, step string) (*MatrixData, error) {
	query := make(map[string]string, 0)
	query["query"] = promQL
	query["start"] = fmt.Sprintf("%d", start.Unix())
	query["end"] = fmt.Sprintf("%d", end.Unix())
	query["step"] = step
	body, err := HttpDo(http.MethodGet, c.Url+"/api/v1/query_range", nil, query, "")
	if err != nil {
		return nil, err
	}

	result := MatrixResult{}
	err = json.Unmarshal(body, &result)
	if err != nil {
		return nil, fmt.Errorf("unmarshal [%s] to MatrixResult failed, err: %s", string(body), err)
	}

	if result.Status != "success" {
		return nil, fmt.Errorf(result.ErrorType, result.Error)
	}

	//fmt.Printf("%s", string(body))
	//fmt.Printf("%+v\n", result.Data.Result[0].Values)
	return &result.Data, nil
}

// Query 查询
func (c *Client) Query(promQL string, time time.Time) (*VectorData, error) {
	query := make(map[string]string, 0)
	query["query"] = promQL
	query["time"] = fmt.Sprintf("%d", time.Unix())
	body, err := HttpDo(http.MethodGet, c.Url+"/api/v1/query", nil, query, "")
	if err != nil {
		return nil, err
	}

	result := VectorResult{}
	err = json.Unmarshal(body, &result)
	if err != nil {
		return nil, fmt.Errorf("unmarshal [%s] to MatrixResult failed, err: %s", string(body), err)
	}

	if result.Status != "success" {
		return nil, fmt.Errorf(result.ErrorType, result.Error)
	}

	//fmt.Printf("%s", string(body))
	//fmt.Printf("%+v\n", result.Data.Result[0].Values)
	return &result.Data, nil
}

func parseData(vds *VectorData) (float64, error) {
	var err error
	value := 0.0
	if len(vds.Result) == 0 {
		return value, NoDataPointError
	}

	for _, vd := range vds.Result {
		if len(vd.Value) >= 2 {
			if s, ok := vd.Value[1].(string); ok {
				value, err = strconv.ParseFloat(s, 64)
				if err != nil {
					return value, fmt.Errorf("ParseFloat parse metric.value to float64 failed, err:%s", err)
				}
			} else {
				return value, fmt.Errorf("convert interface to string failed, value:%v", vd.Value)
			}
		} else {
			return value, fmt.Errorf("metric.value length should > 2, value:%v", vd.Value)
		}
	}
	return value, nil
}

func HttpDo(method, url string, headers, query map[string]string, b string) ([]byte, error) {
	client := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
	}

	if len(query) > 0 {
		url += "?"
		for k, v := range query {
			url += fmt.Sprintf("%s=%s&", k, v)
		}
	}

	req, err := http.NewRequest(method, url, strings.NewReader(b))
	if err != nil {
		return nil, fmt.Errorf("new http client failed, err:%s", err)
	}

	req.Header.Set("Host", req.URL.Host)
	req.Close = true

	for k, v := range headers {
		req.Header.Set(k, v)
	}
	//req.Header.Set("Content-LogType", "application/json; charset=utf-8")

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("do http request failed, %s", err)
	}

	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read http response failed, err:%s", err)
	}

	return body, nil
}
