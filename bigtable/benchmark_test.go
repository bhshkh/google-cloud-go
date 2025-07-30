package bigtable

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"

	// "google.golang.org/api/option"

	"runtime/pprof"
)

const (
	preFix           = "with-"
	numRows          = 10000
	numGoRoutines    = 10
	project          = "cndb-sdk-golang-general"
	columnFamilyName = "cf1"
	columnName       = "greeting"
)

var (
	instance       = preFix + "profiling"
	cpuProfileFile = preFix + "cpu.prof"
	memProfileFile = preFix + "mem.prof"

	adminClient          *AdminClient
	client               *Client
	tableName            string
	ctx                  context.Context
	randomIndexGenerator *rand.Rand
	rowKeyPrefix         string
)

func sliceContains(list []string, target string) bool {
	for _, s := range list {
		if s == target {
			return true
		}
	}
	return false
}

func createTableIfNotExists() {
	tables, err := adminClient.Tables(ctx)
	if err != nil {
		fmt.Printf("Could not fetch table list: %v\n", err)
	}

	if !sliceContains(tables, tableName) {
		// fmt.Printf("Creating table %s\n", tableName)
		if err := adminClient.CreateTable(ctx, tableName); err != nil {
			fmt.Printf("Could not create table %s: %v\n", tableName, err)
		}
	}

	tblInfo, err := adminClient.TableInfo(ctx, tableName)
	if err != nil {
		fmt.Printf("Could not read info for table %s: %v\n", tableName, err)
	}

	if tblInfo != nil && !sliceContains(tblInfo.Families, columnFamilyName) {
		if err := adminClient.CreateColumnFamily(ctx, tableName, columnFamilyName); err != nil {
			fmt.Printf("Could not create column family %s: %v\n", columnFamilyName, err)
		}
	}
	if err = adminClient.Close(); err != nil {
		fmt.Printf("Could not close admin client: %v\n", err)
	}

}

// Write numRows to table
func applyBulk() {
	// fmt.Printf("\napplyBulk")
	tbl := client.Open(tableName)
	muts := make([]*Mutation, numRows)
	rowKeys := make([]string, numRows)
	for i := 0; i < numRows; i++ {
		muts[i] = NewMutation()
		muts[i].Set(columnFamilyName, columnName, Now(), []byte("Hello"))

		rowKey := generateDeterministicRowKey(i)
		rowKeys[i] = rowKey
	}

	rowErrs, err := tbl.ApplyBulk(ctx, rowKeys, muts)
	if err != nil {
		fmt.Printf("Could not apply bulk row mutation: %v\n", err)
	}
	if rowErrs != nil {
		for _, rowErr := range rowErrs {
			fmt.Printf("Error writing row: %v\n", rowErr)
		}
		fmt.Printf("Could not write some rows\n")
	}
}

func generateDeterministicRowKey(index int) string {
	return fmt.Sprintf("%s-%010d", rowKeyPrefix, index)
}

func readRandomRowsIndividually() {
	// fmt.Printf("\nreadRandomRowsIndividually\n")
	tbl := client.Open(tableName)
	for i := 0; i < numRows; i++ {
		// read a random row
		rowKey := generateDeterministicRowKey(i)
		_, err := tbl.ReadRow(ctx, rowKey, RowFilter(ColumnFilter(columnName)))
		if err != nil {
			fmt.Printf("Could not read row with key %s: %v\n", rowKey, err)
		}
	}
}

func profileThis() {
	var wg sync.WaitGroup
	for i := 0; i < numGoRoutines; i++ {
		wg.Add(1)
		go func(wg *sync.WaitGroup) {
			defer wg.Done()
			readRandomRowsIndividually()
		}(&wg)
	}
	wg.Wait()
}

func setup(enable bool) {
	ctx = context.Background()
	var err error
	adminClient, err = NewAdminClient(ctx, project, instance)
	if err != nil {
		fmt.Printf("Could not create admin client: %v\n", err)
	}

	config := disableMetricsConfig
	if enable {
		config = ClientConfig{}
	}
	client, err = NewClientWithConfig(ctx, project, instance, config)
	if err != nil {
		fmt.Printf("Could not create data operations client: %v\n", err)
	}

	tableName = "profile-" + uuid.NewString()
	rowKeyPrefix = "row-" + uuid.New().String()

	// create table
	createTableIfNotExists()
	adminClient.Close()
	applyBulk()
	randomIndexGenerator = rand.New(rand.NewSource(time.Now().UnixNano()))
}

func driver(enable bool) {
	otel.SetErrorHandler(&customErrorHandler{})
	setup(enable)

	// Start profiling
	f, err := os.Create(cpuProfileFile + "-" + strconv.FormatBool(enable))
	if err != nil {
		fmt.Println(err)
		return
	}

	pprof.StartCPUProfile(f)
	defer pprof.StopCPUProfile()
	profileThis()
	// fmt.Println("\n" + preFix + "DONE")
}

// customErrorHandler is a custom implementation of an error handler.
type customErrorHandler struct{}

// Handle is called for internal OpenTelemetry errors.
func (h *customErrorHandler) Handle(err error) {
	fmt.Printf("OpenTelemetry internal error: %v", err)
}

func BenchmarkReadRowsWithMetrics(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		driver(true)
	}
}

func BenchmarkReadRowsWithoutMetrics(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		driver(false)
	}
}
