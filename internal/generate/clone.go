package main

import (
	"fmt"
	"os"

	git "github.com/go-git/go-git/v5"
)

func cloneOpenLineage() (string, error) {
	// Tempdir to clone the repository
	dir, err := os.MkdirTemp("", "openlineage")
	if err != nil {
		return "", fmt.Errorf("create temp dir: %w", err)
	}

	opts := git.CloneOptions{
		URL:   "https://github.com/openlineage/OpenLineage.git",
		Depth: 1,
	}

	// Clones the repository into the given dir, just as a normal git clone does
	if _, err := git.PlainClone(dir, false, &opts); err != nil {
		return "", fmt.Errorf("clone repository: %w", err)
	}

	return dir, nil

}
