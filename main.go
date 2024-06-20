package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/charmbracelet/huh"
)

var importPath string
var exporter bool
var tableName string

func init() {
	flag.StringVar(&tableName, "table", "", "Specify the tableName")
	flag.StringVar(&importPath, "import", "", "Import data from a file in JSON format")
	flag.Parse()
}

func usage() {
	fmt.Println(`
DynamoDB Migrator
=================

To export:

ddbm --table foo

This will print to STDOUT, so direct the output to a file:

ddbm --table foo > /path/to/file.json

To import:

ddbm --table foo --import /path/to/file.json
`)
}

func main() {
	if tableName == "" {
		usage()
		os.Exit(1)
	}

	ctx := context.Background()
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		log.Fatal(err)
	}

	client := dynamodb.NewFromConfig(cfg)

	if importPath != "" {
		err := importFromFile(ctx, client, importPath)
		if err != nil {
			log.Fatal(err)
		}

		os.Exit(0)
	} else {
		out, err := export(ctx, client)
		if err != nil {
			log.Fatal(err)
		}

		fmt.Println(out)
		os.Exit(0)
	}

	usage()
	os.Exit(1)
}

type exportFormat struct {
	TableName  string
	PrimaryKey string
	RangeKey   string

	Items []map[string]any
}

func export(ctx context.Context, client *dynamodb.Client) (string, error) {
	table, err := client.DescribeTable(ctx, &dynamodb.DescribeTableInput{
		TableName: &tableName,
	})
	if err != nil {
		return "", err
	}

	exportData := exportFormat{
		TableName: *table.Table.TableName,
	}

	for _, key := range table.Table.KeySchema {
		if key.KeyType == types.KeyTypeHash {
			exportData.PrimaryKey = *key.AttributeName
		}

		if key.KeyType == types.KeyTypeRange {
			exportData.RangeKey = *key.AttributeName
		}
	}

	paginator := dynamodb.NewScanPaginator(client, &dynamodb.ScanInput{
		TableName: &tableName,
	})

	var items []map[string]types.AttributeValue
	for paginator.HasMorePages() {
		output, err := paginator.NextPage(ctx)
		if err != nil {
			return "", err
		}

		items = append(items, output.Items...)
	}

	err = attributevalue.UnmarshalListOfMaps(items, &exportData.Items)
	if err != nil {
		return "", err
	}

	dump, err := json.Marshal(exportData)
	if err != nil {
		return "", err
	}

	return string(dump), err
}

func importFromFile(ctx context.Context, client *dynamodb.Client, path string) error {
	raw, err := os.ReadFile(path)
	if err != nil {
		return err
	}

	var data exportFormat
	err = json.Unmarshal(raw, &data)
	if err != nil {
		return err
	}

	var confirm bool
	form := huh.NewForm(huh.NewGroup(
		huh.NewConfirm().
			Title(fmt.Sprintf("This will import data into %s! Do you want to continue?", tableName)).
			Affirmative("yes").
			Negative("no").
			Value(&confirm),
	))
	form.Run()

	if !confirm {
		return nil
	}

	for _, item := range data.Items {
		mapdata, err := attributevalue.MarshalMap(item)
		if err != nil {
			return err
		}

		_, err = client.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: &tableName,
			Item:      mapdata,
		})
		if err != nil {
			return err
		}
	}

	return nil
}
