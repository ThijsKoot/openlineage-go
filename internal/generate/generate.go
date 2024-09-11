package main

import (
	"fmt"
	"log"
	"os"
)

var repoDir string

func main() {
	if err := generate(); err != nil {
		log.Fatal(err)
	}
}

func generate() error {
	repo, err := cloneOpenLineage()
	if err != nil {
		return fmt.Errorf("clone openlineage repo: %w", err)
	}

	repoDir = repo

	if err := facets(); err != nil {
		return fmt.Errorf("generate facets: %w", err)

	}

	if err := openLineage(); err != nil {
		return fmt.Errorf("generate openlinage: %w", err)
	}

	return nil

}

func openLineage() error {
	baseCode, err := generateOpenLineageCode()
	if err != nil {
		return err
	}

	edited, err := removeFacetBaseTypes(baseCode)
	if err != nil {
		return err
	}

	file, err := os.Create("openlineage.gen.go")
	if err != nil {
		return err
	}
	defer file.Close()

	if _, err := file.WriteString(edited); err != nil {
		return err
	}

	return nil
}

func facets() error {
	baseFacetCode, err := generateFacets()
	if err != nil {
		return err
	}

	facets, err := extractFacets(baseFacetCode)
	if err != nil {
		return err
	}

	helperCode, err := generateFacetHelpers(facets)
	if err != nil {
		return err
	}

	editedFacetCode, err := removeFacetWrappers(baseFacetCode)
	if err != nil {
		return err
	}

	facetFile, err := os.Create("pkg/facets/facets.gen.go")
	if err != nil {
		return err
	}
	defer facetFile.Close()

	if _, err := facetFile.WriteString(editedFacetCode); err != nil {
		return err
	}

	helperFile, err := os.Create("pkg/facets/facet_helpers.gen.go")
	if err != nil {
		return err
	}
	defer helperFile.Close()

	if _, err := helperFile.WriteString(helperCode); err != nil {
		return err
	}

	return nil
}
