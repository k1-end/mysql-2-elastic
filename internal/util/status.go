package util

import (
	"encoding/json"
	"io"
	"os"
)

func IsServerFree() bool {
	jsonFile, err := os.Open("data/status.json")
	if err != nil {
		panic(err)
	}
	defer jsonFile.Close()
	byteValue, _ := io.ReadAll(jsonFile)

	var status map[string]any
	json.Unmarshal(byteValue, &status)

	if status["status"] == "free" {
		return true
	}
	return false
}

func SetServerStatus(status string) error {
	statusMap := make(map[string]string)
	statusMap["status"] = status
	jsonData, err := json.Marshal(statusMap)
	if err != nil {
		return err
	}
	err = os.WriteFile("data/status.json", jsonData, 0644)

	return nil
}
