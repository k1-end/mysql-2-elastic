package util

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
)

func IsServerFree() (bool, error) {
	f, err := os.Open("data/status.json")
	if err != nil {
		return false, fmt.Errorf("failed to open status file: %w", err)
	}
	defer f.Close()

	data, err := io.ReadAll(f)
	if err != nil {
		return false, fmt.Errorf("failed to read status file: %w", err)
	}

	var status map[string]any
	if err := json.Unmarshal(data, &status); err != nil {
		return false, fmt.Errorf("failed to parse status file: %w", err)
	}
	return status["status"] == "free", nil
}

func SetServerStatus(status string) error {
	data, err := json.Marshal(map[string]string{"status": status})
	if err != nil {
		return fmt.Errorf("failed to marshal status: %w", err)
	}
	return os.WriteFile("data/status.json", data, 0644)
}
