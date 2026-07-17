package util

import (
	"errors"
	"fmt"
	"os"
)

func directoryExists(path string) (bool, error) {
	info, err := os.Stat(path)
	if err == nil {
		return info.IsDir(), nil
	}
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	return false, err
}

func CreateDirectoryIfNotExists(path string) error {
	exists, err := directoryExists(path)
	if err != nil {
		return fmt.Errorf("failed to check directory existence for %q: %w", path, err)
	}
	if exists {
		return nil
	}
	if err := os.MkdirAll(path, 0755); err != nil {
		return fmt.Errorf("failed to create directory %q: %w", path, err)
	}
	return nil
}
