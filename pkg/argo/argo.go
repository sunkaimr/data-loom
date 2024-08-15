package argo

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/sunkaimr/data-loom/configs"
	"github.com/sunkaimr/data-loom/internal/pkg/common"
	"github.com/sunkaimr/data-loom/internal/workflow/types"
	"github.com/sunkaimr/data-loom/pkg/utils"
	"net/http"
	"strings"
	"text/template"
	"time"
)

const (
	SubmitWorkFlowURL = "/api/v1/workflows/%s/submit"
	WorkFlowInfoURL   = "/api/v1/workflows/%s/%s"      /* namespace / workflowName */
	WorkFlowAddr      = "%s/workflows/%s"              /* url / workflowName */
	StopWorkFlowURL   = "/api/v1/workflows/%s/%s/stop" /* namespace / workflowName */
)

type Argo struct {
	URL  string
	Auth string
}

type WorkFlowResp struct {
	Metadata WorkFlowMateData `json:"metadata"`
	Status   WorkFlowStatus   `json:"status"`
	Code     int              `json:"code"`
	Message  string           `json:"message"`
}
type WorkFlowMateData struct {
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
	Uid       string `json:"uid"`
}

type WorkFlowStatus struct {
	Phase             string    `json:"phase"`
	StartedAt         time.Time `json:"startedAt"`
	FinishedAt        time.Time `json:"finishedAt"`
	EstimatedDuration int       `json:"estimatedDuration"`
	Progress          string    `json:"progress"`
}

func (c *Argo) TruncateData(ctx *common.Context, para *types.TruncateParaStruct) (string /* workflow id*/, error) {
	log := ctx.Log
	log.Debugf("sending TruncateData request to argo, %+v", para)

	workflowNameSpace, workflowName := getWorkFlowAndNameSpace(configs.C.WorkFlow.Argo.Templates[Truncate])
	if workflowName == "" {
		return "", fmt.Errorf("config.workflow.argo.templates.truncate_data is null")
	}

	tempPara := WorkflowParas[Truncate]
	tmpl, err := template.New("template").Parse(tempPara)
	if err != nil {
		return "", err
	}

	tmpPara := struct {
		Namespace string
		WorkFlow  string
		types.TruncateParaStruct
	}{
		Namespace:          workflowNameSpace,
		WorkFlow:           workflowName,
		TruncateParaStruct: *para,
	}
	b := bytes.Buffer{}
	err = tmpl.Execute(&b, tmpPara)
	if err != nil {
		return "", fmt.Errorf("execute template(%s) failed, %s", b.String(), err)
	}

	url := fmt.Sprintf(c.URL+SubmitWorkFlowURL, workflowNameSpace)
	headers := make(map[string]string, 1)
	headers["Authorization"] = "Bearer " + c.Auth
	log.Debugf("call workflow[%s/%s] url: %s, parament: %s", workflowNameSpace, workflowName, url, b.String())

	resp, body, err := utils.HttpDo(http.MethodPost, url, headers, nil, b.String())
	if err != nil {
		return "", err
	}

	log.Debugf("got workflow[%s/%s] response code %v", workflowNameSpace, workflowName, resp.StatusCode)

	wfResp := &WorkFlowResp{}
	err = json.Unmarshal(body, wfResp)
	if err != nil {
		return "", fmt.Errorf("unmarshal failed, %s", err)
	}

	if wfResp.Code != 0 {
		return "", fmt.Errorf("fail to submit workflow(%s/%s) %s", workflowNameSpace, workflowName, wfResp.Message)
	}

	return wfResp.Metadata.Namespace + "/" + wfResp.Metadata.Name, nil
}

func (c *Argo) DeleteData(ctx *common.Context, para *types.DeleteParaStruct) (string /* workflow id*/, error) {
	log := ctx.Log
	log.Debugf("sending DeleteData request to argo, %+v", para)

	workflowNameSpace, workflowName := getWorkFlowAndNameSpace(configs.C.WorkFlow.Argo.Templates[Delete])
	if workflowName == "" {
		return "", fmt.Errorf("config.workflow.argo.templates.delete is null")
	}

	tempPara := WorkflowParas[Delete]
	tmpl, err := template.New("template").Parse(tempPara)
	if err != nil {
		return "", err
	}

	// where 条件可能包含很多特殊字符这里通过base64编码，在使用时需要先解码
	para.Condition = base64.StdEncoding.EncodeToString([]byte(para.Condition))
	tmpPara := struct {
		Namespace string
		WorkFlow  string
		types.DeleteParaStruct
	}{
		Namespace:        workflowNameSpace,
		WorkFlow:         workflowName,
		DeleteParaStruct: *para,
	}
	b := bytes.Buffer{}
	err = tmpl.Execute(&b, tmpPara)
	if err != nil {
		return "", fmt.Errorf("execute template(%s) failed, %s", b.String(), err)
	}

	url := fmt.Sprintf(c.URL+SubmitWorkFlowURL, workflowNameSpace)
	headers := make(map[string]string, 1)
	headers["Authorization"] = "Bearer " + c.Auth
	log.Debugf("call workflow[%s/%s] url: %s, parament: %s", workflowNameSpace, workflowName, url, b.String())

	resp, body, err := utils.HttpDo(http.MethodPost, url, headers, nil, b.String())
	if err != nil {
		return "", err
	}

	log.Debugf("got workflow[%s/%s] response code %v", workflowNameSpace, workflowName, resp.StatusCode)

	wfResp := &WorkFlowResp{}
	err = json.Unmarshal(body, wfResp)
	if err != nil {
		return "", fmt.Errorf("unmarshal failed, %s", err)
	}

	if wfResp.Code != 0 {
		return "", fmt.Errorf("fail to submit workflow(%s/%s) %s", workflowNameSpace, workflowName, wfResp.Message)
	}

	return wfResp.Metadata.Namespace + "/" + wfResp.Metadata.Name, nil
}

func (c *Argo) RebuildTables(ctx *common.Context, para *types.RebuildParaStruct) (string /* workflow id*/, error) {
	log := ctx.Log
	log.Debugf("sending RebuildTables request to argo, %+v", para)

	workflowNameSpace, workflowName := getWorkFlowAndNameSpace(configs.C.WorkFlow.Argo.Templates[Rebuild])
	if workflowName == "" {
		return "", fmt.Errorf("config.workflow.argo.templates.rebuild is null")
	}

	tempPara := WorkflowParas[Rebuild]
	tmpl, err := template.New("template").Parse(tempPara)
	if err != nil {
		return "", err
	}

	tmpPara := struct {
		Namespace string
		WorkFlow  string
		types.RebuildParaStruct
	}{
		Namespace:         workflowNameSpace,
		WorkFlow:          workflowName,
		RebuildParaStruct: *para,
	}
	b := bytes.Buffer{}
	err = tmpl.Execute(&b, tmpPara)
	if err != nil {
		return "", fmt.Errorf("execute template(%s) failed, %s", b.String(), err)
	}

	url := fmt.Sprintf(c.URL+SubmitWorkFlowURL, workflowNameSpace)
	headers := make(map[string]string, 1)
	headers["Authorization"] = "Bearer " + c.Auth
	log.Debugf("call workflow[%s/%s] url: %s, parament: %s", workflowNameSpace, workflowName, url, b.String())

	resp, body, err := utils.HttpDo(http.MethodPost, url, headers, nil, b.String())
	if err != nil {
		return "", err
	}

	log.Debugf("got workflow[%s/%s] response code %v", workflowNameSpace, workflowName, resp.StatusCode)

	wfResp := &WorkFlowResp{}
	err = json.Unmarshal(body, wfResp)
	if err != nil {
		return "", fmt.Errorf("unmarshal failed, %s", err)
	}

	if wfResp.Code != 0 {
		return "", fmt.Errorf("fail to submit workflow(%s/%s) %s", workflowNameSpace, workflowName, wfResp.Message)
	}

	return wfResp.Metadata.Namespace + "/" + wfResp.Metadata.Name, nil
}

func (c *Argo) ArchiveData(ctx *common.Context, para *types.ArchiveParaStruct) (string, error) {
	log := ctx.Log
	log.Debugf("sending ArchiveData request to argo, %+v", para)
	return "", nil
}

func (c *Argo) WorkFlowStatus(ctx *common.Context, workflow string) (types.WorkFlowStatusType, error) {
	log := ctx.Log

	headers := make(map[string]string, 1)
	headers["Authorization"] = "Bearer " + c.Auth

	workflowNameSpace, workflowName := getWorkFlowAndNameSpace(workflow)
	if workflowName == "" {
		return types.WorkFlowStatusUnknown, fmt.Errorf("workflow name(%s) is null", workflow)
	}

	url := fmt.Sprintf(c.URL+WorkFlowInfoURL, workflowNameSpace, workflowName)

	log.Debugf("call workflow[%s/%s] url: %s, query workflow status", workflowNameSpace, workflowName, url)

	resp, body, err := utils.HttpDo(http.MethodGet, url, headers, nil, "")
	if err != nil {
		return types.WorkFlowStatusUnknown, err
	}
	log.Debugf("got workflow[%s/%s] response code %v", workflowNameSpace, workflowName, resp.StatusCode)

	wfResp := &WorkFlowResp{}
	err = json.Unmarshal(body, wfResp)
	if err != nil {
		return types.WorkFlowStatusUnknown, fmt.Errorf("unmarshal failed, %s", err)
	}

	if wfResp.Code != 0 {
		return types.WorkFlowStatusUnknown, fmt.Errorf("fail to submit workflow(%s/%s) %s", workflowNameSpace, workflowName, wfResp.Message)
	}

	switch wfResp.Status.Phase {
	case "Pending":
		return types.WorkFlowStatusPending, nil
	case "Running":
		return types.WorkFlowStatusRunning, nil
	case "Succeeded":
		return types.WorkFlowStatusSucceeded, nil
	case "Failed":
		return types.WorkFlowStatusFailed, nil
	case "Error":
		return types.WorkFlowStatusError, nil
	default:
		return types.WorkFlowStatusUnknown, nil
	}
}

func (c *Argo) WorkFlowAddr(_ *common.Context, workflow string) (string, error) {
	if workflow == "" {
		return "", nil
	}
	return fmt.Sprintf(WorkFlowAddr, c.URL, workflow), nil
}

func (c *Argo) DeleteWorkFlow(ctx *common.Context, workflow string) error {
	log := ctx.Log

	headers := make(map[string]string, 1)
	headers["Authorization"] = "Bearer " + c.Auth

	workflowNameSpace, workflowName := getWorkFlowAndNameSpace(workflow)
	if workflowName == "" {
		return fmt.Errorf("workflow name(%s) is null", workflow)
	}

	url := fmt.Sprintf(c.URL+WorkFlowInfoURL, workflowNameSpace, workflowName)

	log.Debugf("call workflow[%s/%s] url: %s, delete workflow", workflowNameSpace, workflowName, url)

	resp, body, err := utils.HttpDo(http.MethodDelete, url, headers, nil, "")
	if err != nil {
		return err
	}
	log.Debugf("delete workflow[%s/%s] response code %v", workflowNameSpace, workflowName, resp.StatusCode)

	wfResp := &WorkFlowResp{}
	err = json.Unmarshal(body, wfResp)
	if err != nil {
		return fmt.Errorf("unmarshal failed, %s", err)
	}

	if wfResp.Code != 0 {
		return fmt.Errorf("fail to delete workflow(%s/%s) %s", workflowNameSpace, workflowName, wfResp.Message)
	}

	return nil
}

func (c *Argo) StopWorkFlow(ctx *common.Context, workflow string) error {
	log := ctx.Log

	headers := make(map[string]string, 1)
	headers["Authorization"] = "Bearer " + c.Auth

	workflowNameSpace, workflowName := getWorkFlowAndNameSpace(workflow)
	if workflowName == "" {
		return fmt.Errorf("workflow name(%s) is null", workflow)
	}

	url := fmt.Sprintf(c.URL+StopWorkFlowURL, workflowNameSpace, workflowName)

	log.Debugf("call workflow[%s/%s] url: %s, stop workflow", workflowNameSpace, workflowName, url)

	resp, body, err := utils.HttpDo(http.MethodPut, url, headers, nil, "")
	if err != nil {
		return err
	}
	log.Debugf("stop workflow[%s/%s] response code %v", workflowNameSpace, workflowName, resp.StatusCode)

	wfResp := &WorkFlowResp{}
	err = json.Unmarshal(body, wfResp)
	if err != nil {
		return fmt.Errorf("unmarshal failed, %s", err)
	}

	if wfResp.Code != 0 {
		return fmt.Errorf("fail to stop workflow(%s/%s) %s", workflowNameSpace, workflowName, wfResp.Message)
	}

	return nil
}

func getWorkFlowAndNameSpace(p string) (string, string) {
	s := strings.Split(p, "/")
	switch len(s) {
	case 0:
		return "", ""
	case 1:
		return "", s[0]
	default:
		return s[0], s[1]
	}
}
