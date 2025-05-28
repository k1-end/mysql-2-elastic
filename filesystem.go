package main

import (
	"errors"
	"fmt"
	"os"
)

func directoryExists(path string) (bool, error) {
	info, err := os.Stat(path)
	if err == nil {
		// Path exists, check if it's a directory
		return info.IsDir(), nil
	}
	if errors.Is(err, os.ErrNotExist) {
		// Path does not exist
		return false, nil
	}
	// Some other error occurred (e.g., permissions)
	return false, err
}

func createDirectoryIfNotExists(path string) error {
	exists, err := directoryExists(path)
	if err != nil {
		// An error occurred while checking existence (e.g., permission issues)
		return fmt.Errorf("failed to check directory existence for '%s': %w", path, err)
	}

	if exists {
		// Directory already exists, do nothing
		fmt.Printf("Directory '%s' already exists. Doing nothing.\n", path)
		return nil
	}

	// Directory does not exist, so create it
	// os.MkdirAll is used to create all necessary parent directories as well.
	// The permission 0755 means:
	// - Owner: read, write, execute (7)
	// - Group: read, execute (5)
	// - Others: read, execute (5)
	err = os.MkdirAll(path, 0755)
	if err != nil {
		return fmt.Errorf("failed to create directory '%s': %w", path, err)
	}

	fmt.Printf("Directory '%s' created successfully.\n", path)
	return nil
}
