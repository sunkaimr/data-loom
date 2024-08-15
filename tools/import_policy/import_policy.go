package main

import (
	"errors"
	"fmt"
	"github.com/goccy/go-json"
	"github.com/sunkaimr/data-loom/internal/pkg/common"
	"github.com/sunkaimr/data-loom/internal/services"
	"github.com/sunkaimr/data-loom/pkg/utils"
	"github.com/xuri/excelize/v2"
	"net/http"
	"os"
	"strconv"
)

const (
	Addr  = "http://127.0.0.1:8080"
	Token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjQ4NzEyNDUyMDcsInVzZXJpZCI6MCwidXNlcm5hbWUiOiJhZG1pbiJ9.YbVSvPQO3W6k06u9Sl0nilOFk4jcIOEBwK-eFz3l0Rk"

	fileName    = "./policy_template.xlsx"
	sourceSheet = "源端"
	policySheet = "策略"
)

func main() {
	ImportPolicies()
}

// ImportPolicies 批量导入策略
func ImportPolicies() {
	sourceHeader := map[string]int{
		"id":            0, //"A",
		"name":          1, //"B",
		"bu":            2, //"C",
		"cluster_name":  3, //"D",
		"cluster_id":    4, //"E",
		"database_name": 5, //"F",
		"tables_name":   6, //"G",
	}

	policyHeader := map[string]int{
		"id":             0, //"A",
		"name":           1, //"B",
		"bu":             2, //"C",
		"src_id":         3, //"D",
		"enable":         4, //"E",
		"execute_window": 5, //"F",
		"period":         6, //"G",
		"day":            7, //"H",
		"govern":         8, //"I",
		"condition":      9, //"J",
	}

	f, err := excelize.OpenFile(fileName)
	if err != nil {
		fmt.Printf("open file %s failed, %s", fileName, err)
		os.Exit(1)
	}
	defer func() {
		_ = f.Save()
		_ = f.Close()
	}()

	sourceRows, err := f.GetRows(sourceSheet)
	if err != nil {
		fmt.Printf("get sourceRows failed, %s", err)
		os.Exit(1)
	}

	policyRows, err := f.GetRows(policySheet)
	if err != nil {
		fmt.Printf("get sourceRows failed, %s", err)
		os.Exit(1)
	}

	rowNum := 0
	for i, _ := range sourceRows {
		rowNum++
		if i == 0 {
			continue
		}

		if len(sourceRows[i]) < 6 {
			fmt.Printf("skip row %d ...\n", i+1)
			continue
		}

		fmt.Printf("import row %d ...\n", i+1)

		clusterID := sourceRows[i][sourceHeader["cluster_id"]]
		database := sourceRows[i][sourceHeader["database_name"]]
		tables := sourceRows[i][sourceHeader["tables_name"]]

		if clusterID == "" || database == "" || tables == "" {
			continue
		}

		sourceSvc, err := createSource(clusterID, database, tables)
		if err != nil {
			_ = f.SetCellValue(sourceSheet, "I"+strconv.Itoa(rowNum), err.Error())
			continue
		}

		if clusterID == sourceSvc.ClusterID && database == sourceSvc.DatabaseName && tables == sourceSvc.TablesName {
			_ = f.SetCellValue(sourceSheet, "A"+strconv.Itoa(rowNum), sourceSvc.ID)
			_ = f.SetCellValue(sourceSheet, "B"+strconv.Itoa(rowNum), sourceSvc.Name)
			_ = f.SetCellValue(sourceSheet, "C"+strconv.Itoa(rowNum), sourceSvc.Bu)
			_ = f.SetCellValue(sourceSheet, "D"+strconv.Itoa(rowNum), sourceSvc.ClusterName)
		} else {
			_ = f.SetCellValue(sourceSheet, "I"+strconv.Itoa(rowNum), fmt.Sprintf("%+v", sourceSvc))
			continue
		}

		if len(policyRows[i]) < 8 {
			fmt.Printf("skip row %d ...\n", i+1)
			continue
		}

		enable := policyRows[i][policyHeader["enable"]] == "true"
		executeWindow := policyRows[i][policyHeader["execute_window"]]
		period := policyRows[i][policyHeader["period"]]
		day, _ := strconv.Atoi(policyRows[i][policyHeader["day"]])
		govern := policyRows[i][policyHeader["govern"]]

		condition := ""
		if len(policyRows[i]) > policyHeader["condition"] {
			condition = policyRows[i][policyHeader["condition"]]
		}

		if executeWindow == "" || period == "" || govern == "" {
			continue
		}

		var execWin []string
		_ = json.Unmarshal([]byte(executeWindow), &execWin)
		policySvc, err := createPolicy(
			sourceSvc.ID,
			enable,
			execWin,
			period,
			day,
			govern,
			condition)
		if err != nil {
			_ = f.SetCellValue(policySheet, "I"+strconv.Itoa(rowNum), err.Error())
			continue
		}

		if enable == policySvc.Enable && sourceSvc.ID == policySvc.SrcID &&
			fmt.Sprintf("%s", execWin) == fmt.Sprintf("%s", policySvc.ExecuteWindow) &&
			period == string(policySvc.Period) &&
			day == policySvc.Day &&
			govern == string(policySvc.Govern) &&
			condition == policySvc.Condition {
			_ = f.SetCellValue(policySheet, "A"+strconv.Itoa(rowNum), policySvc.ID)
			_ = f.SetCellValue(policySheet, "B"+strconv.Itoa(rowNum), policySvc.Name)
			_ = f.SetCellValue(policySheet, "C"+strconv.Itoa(rowNum), policySvc.Bu)
			_ = f.SetCellValue(policySheet, "D"+strconv.Itoa(rowNum), policySvc.SrcID)
		} else {
			_ = f.SetCellValue(policySheet, "I"+strconv.Itoa(rowNum), fmt.Sprintf("%+v", policySvc))
			continue
		}
	}
}

func createSource(clusterID, databaseName, tableName string) (*services.SourceService, error) {
	req, _ := json.Marshal(services.SourceService{
		Description:  "自动导入",
		ClusterID:    clusterID,
		DatabaseName: databaseName,
		TablesName:   tableName,
	})

	headers := make(map[string]string, 1)
	headers["Authorization"] = fmt.Sprintf("Bearer %s", Token)

	_, body, err := utils.HttpDo(http.MethodPost,
		fmt.Sprintf("%s/data-loom/api/v1/source", Addr),
		headers,
		nil,
		string(req))
	if err != nil {
		return nil, err
	}

	res := &common.Response{}
	err = json.Unmarshal(body, res)
	if err != nil {
		return nil, err
	}

	if res.Code != common.CodeOK.Code && res.Code != common.CodeSourceExist.Code {
		return nil, errors.New(res.Message)
	}

	b, _ := json.Marshal(res.Data)
	s := &services.SourceService{}
	json.Unmarshal(b, s)
	return s, nil
}

func createPolicy(srcID uint, enable bool, execWin []string, period string, day int, govern string, condition string) (*services.PolicyService, error) {
	req, _ := json.Marshal(services.PolicyService{
		Description:   "自动导入",
		SrcID:         srcID,
		Enable:        enable,
		ExecuteWindow: execWin,
		Period:        common.PeriodType(period),
		Day:           day,
		Govern:        common.GovernType(govern),
		Condition:     condition,
	})

	headers := make(map[string]string, 1)
	headers["Authorization"] = fmt.Sprintf("Bearer %s", Token)

	_, body, err := utils.HttpDo(http.MethodPost,
		fmt.Sprintf("%s/data-loom/api/v1/policy", Addr),
		headers,
		nil,
		string(req))
	if err != nil {
		return nil, err
	}

	res := &common.Response{}
	err = json.Unmarshal(body, res)
	if err != nil {
		return nil, err
	}

	if res.Code != common.CodeOK.Code {
		return nil, errors.New(res.Message)
	}

	b, _ := json.Marshal(res.Data)
	s := &services.PolicyService{}
	json.Unmarshal(b, s)
	return s, nil
}
