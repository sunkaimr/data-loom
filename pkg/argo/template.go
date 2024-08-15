package argo

const Truncate = "truncate"
const Delete = "delete"
const Rebuild = "rebuild"
const Archive = "archive"

var WorkflowParas = map[string]string{
	Truncate: `
{
  "namespace": "{{.Namespace}}",
  "resourceKind": "WorkflowTemplate",
  "resourceName": "{{.WorkFlow}}",
  "labels": "task={{.TaskID}}",
  "submitOptions": {
    "entryPoint": null,
    "parameters": [
      "task_id={{.TaskID}}",
      "mysql_host={{.Host}}",
      "mysql_port={{.Port}}",
      "mysql_user={{.User}}",
      "mysql_password={{.Password}}",
      "database_name={{.Database}}",
      "table_name={{.Tables}}",
      "callback_url={{.Callback.URL}}", 
      "callback_token={{.Callback.Token}}"
	]
  }
}`,
	Delete: `
{
  "namespace": "{{.Namespace}}",
  "resourceKind": "WorkflowTemplate",
  "resourceName": "{{.WorkFlow}}",
  "labels": "task={{.TaskID}}",
  "submitOptions": {
    "entryPoint": null,
    "parameters": [
      "task_id={{.TaskID}}",
      "start_time={{.StartTime}}",
      "end_time={{.EndTime}}",
      "mysql_host={{.Host}}",
      "mysql_port={{.Port}}",
      "mysql_user={{.User}}",
      "mysql_password={{.Password}}",
      "database_name={{.Database}}",
      "table_name={{.Tables}}",
      "condition={{.Condition}}", 
      "rebuild_flag={{.RebuildFlag}}",
      "cleaning_speed={{.CleaningSpeed}}",
      "free_disk={{.FreeDisk}}", 
      "callback_url={{.Callback.URL}}", 
      "callback_token={{.Callback.Token}}"
    ]
  }
}
`,
	Rebuild: `
{
  "namespace": "{{.Namespace}}",
  "resourceKind": "WorkflowTemplate",
  "resourceName": "{{.WorkFlow}}",
  "labels": "task={{.TaskID}}",
  "submitOptions": {
    "entryPoint": null,
    "parameters": [
      "task_id={{.TaskID}}",
      "mysql_host={{.Host}}",
      "mysql_port={{.Port}}",
      "mysql_user={{.User}}",
      "mysql_password={{.Password}}",
      "database_name={{.Database}}",
      "table_name={{.Tables}}",
      "free_disk={{.FreeDisk}}", 
      "callback_url={{.Callback.URL}}", 
      "callback_token={{.Callback.Token}}"
    ]
  }
}
`,
	Archive: ``,
}
