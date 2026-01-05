package models

// LogData tüm sistemde kullanılan ortak log yapısıdır
type LogData struct {
    Service string `json:"service"`
    Level   string `json:"level"`
    Msg     string `json:"msg"`
    TS      string `json:"ts"`
}