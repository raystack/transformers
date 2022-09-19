package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/AlecAivazis/survey/v2"
	"github.com/googleapis/google-cloud-go-testing/bigquery/bqiface"
	"github.com/hashicorp/go-hclog"
	"github.com/mitchellh/hashstructure/v2"
	"github.com/odpf/optimus/compiler"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/plugin"
	"github.com/patrickmn/go-cache"
	"github.com/spf13/cast"
	"go.opentelemetry.io/otel/attribute"
	"golang.org/x/sync/errgroup"
)

const (
	ConfigKeyDstart = "DSTART"
	ConfigKeyDend   = "DEND"
)

var (
	Name = "bq2bq"

	// Version should be injected while building
	Version = "dev"
	Image   = "docker.io/odpf/optimus-task-bq2bq"

	validateName = survey.ComposeValidators(
		ValidatorFactory.NewFromRegex(`^[a-zA-Z0-9_\-]+$`, `invalid name (can only contain characters A-Z (in either case), 0-9, "-" or "_")`),
		survey.MinLength(3),
	)
	// a big query table can only contain the the characters [a-zA-Z0-9_].
	// https://cloud.google.com/bigquery/docs/tables
	validateTableName = survey.ComposeValidators(
		ValidatorFactory.NewFromRegex(`^[a-zA-Z0-9_-]+$`, "invalid table name (can only contain characters A-Z (in either case), 0-9, hyphen(-) or underscore (_)"),
		survey.MaxLength(1024),
		survey.MinLength(3),
	)

	tableDestinationPatterns = regexp.MustCompile("" +
		"(?i)(?:FROM)\\s*(?:/\\*\\s*([a-zA-Z0-9@_-]*)\\s*\\*/)?\\s+`?([\\w-]+)\\.([\\w-]+)\\.([\\w-]+)`?" +
		"|" +
		"(?i)(?:JOIN)\\s*(?:/\\*\\s*([a-zA-Z0-9@_-]*)\\s*\\*/)?\\s+`?([\\w-]+)\\.([\\w-]+)\\.([\\w-]+)`?" +
		"|" +
		"(?i)(?:WITH)\\s*(?:/\\*\\s*([a-zA-Z0-9@_-]*)\\s*\\*/)?\\s+`?([\\w-]+)\\.([\\w-]+)\\.([\\w-]+)`?\\s+(?:AS)")

	queryCommentPatterns = regexp.MustCompile("(--.*)|(((/\\*)+?[\\w\\W]*?(\\*/)+))")
	helperPattern        = regexp.MustCompile("(\\/\\*\\s*(@[a-zA-Z0-9_-]+)\\s*\\*\\/)")

	QueryFileName = "query.sql"

	// Deprecated
	SecretName       = "TASK_BQ2BQ"
	BqServiceAccount = "BQ_SERVICE_ACCOUNT"

	TimeoutDuration = time.Second * 180
	MaxBQApiRetries = 3
	FakeSelectStmt  = "SELECT * from `%s` WHERE FALSE LIMIT 1"

	CacheTTL         = time.Hour * 24
	CacheCleanUp     = time.Hour * 6
	ErrCacheNotFound = errors.New("item not found")

	LoadMethodMerge        = "MERGE"
	LoadMethodAppend       = "APPEND"
	LoadMethodReplace      = "REPLACE"
	LoadMethodReplaceMerge = "REPLACE_MERGE"
	LoadMethodReplaceAll   = "REPLACE_ALL"

	QueryFileReplaceBreakMarker = "\n--*--optimus-break-marker--*--\n"

	_ models.CommandLineMod        = &BQ2BQ{}
	_ models.DependencyResolverMod = &BQ2BQ{}
)

type ClientFactory interface {
	New(ctx context.Context, svcAccount string) (bqiface.Client, error)
}

type BQ2BQ struct {
	ClientFac      ClientFactory
	mu             sync.Mutex
	C              *cache.Cache
	TemplateEngine models.TemplateEngine

	logger hclog.Logger
}

func (b *BQ2BQ) PluginInfo() (*models.PluginInfoResponse, error) {
	return &models.PluginInfoResponse{
		Name:          Name,
		Description:   "BigQuery to BigQuery transformation task",
		Image:         fmt.Sprintf("%s:%s", Image, Version),
		PluginVersion: Version,
		PluginType:    models.PluginTypeTask,
		PluginMods:    []models.PluginMod{models.ModTypeCLI, models.ModTypeDependencyResolver},
	}, nil
}

func (b *BQ2BQ) GetQuestions(ctx context.Context, req models.GetQuestionsRequest) (*models.GetQuestionsResponse, error) {

	// generate defaults
	tableDefault := ""
	datasetDefault := ""
	projectDefault := ""
	nameParts := strings.Split(req.JobName, ".")
	namePartsLen := len(nameParts)
	if namePartsLen >= 1 {
		tableDefault = nameParts[namePartsLen-1]
	}
	if namePartsLen >= 2 {
		datasetDefault = nameParts[namePartsLen-2]
	}
	if namePartsLen >= 3 {
		projectDefault = nameParts[namePartsLen-3]
	}

	tQues := []models.PluginQuestion{
		{
			Name:    "Project",
			Prompt:  "Project ID",
			Help:    "Destination bigquery project ID",
			Default: projectDefault,
		},
		{
			Name:    "Dataset",
			Prompt:  "Dataset Name",
			Help:    "Destination bigquery dataset ID",
			Default: datasetDefault,
		},
		{
			Name:    "Table",
			Prompt:  "Table ID",
			Help:    "Destination bigquery table ID",
			Default: tableDefault,
		},
		{
			Name:    "LoadMethod",
			Prompt:  "Load method to use on destination",
			Default: LoadMethodAppend,
			Help: `
APPEND        - Append to existing table
REPLACE       - Deletes existing partition and insert result of select query
MERGE         - DML statements, BQ scripts
REPLACE_MERGE - [Experimental] Advanced replace using merge query
`,
			Multiselect: []string{
				LoadMethodAppend,
				LoadMethodReplace,
				LoadMethodMerge,
				LoadMethodReplaceMerge,
				LoadMethodReplaceAll},
			SubQuestions: []models.PluginSubQuestion{
				{
					IfValue: LoadMethodReplaceMerge,
					Questions: []models.PluginQuestion{
						{
							Name:   "PartitionFilter",
							Prompt: "Partition filter expression",
							Help: `
Where condition over partitioned column used to delete existing partitions
in destination table. These partitions will be replaced with sql query result.
Leave empty for optimus to automatically figure this out although it will be 
faster and cheaper to provide the exact condition.
for example: DATE(event_timestamp) >= "{{ .DSTART|Date }}" AND DATE(event_timestamp) < "{{ .DEND|Date }}"`,
						},
					},
				},
			},
		},
	}
	return &models.GetQuestionsResponse{
		Questions: tQues,
	}, nil
}

func (b *BQ2BQ) ValidateQuestion(ctx context.Context, req models.ValidateQuestionRequest) (*models.ValidateQuestionResponse, error) {
	var err error
	switch req.Answer.Question.Name {
	case "Project":
		err = validateName(req.Answer.Value)
	case "Dataset":
		err = validateName(req.Answer.Value)
	case "Table":
		err = validateTableName(req.Answer.Value)
	case "PartitionFilter":
		err = survey.Required(req.Answer.Value)
	}
	if err != nil {
		return &models.ValidateQuestionResponse{
			Success: false,
			Error:   err.Error(),
		}, nil
	}
	return &models.ValidateQuestionResponse{
		Success: true,
	}, nil
}

func findAnswerByName(name string, answers []models.PluginAnswer) (models.PluginAnswer, bool) {
	for _, ans := range answers {
		if ans.Question.Name == name {
			return ans, true
		}
	}
	return models.PluginAnswer{}, false
}

func (b *BQ2BQ) DefaultConfig(ctx context.Context, request models.DefaultConfigRequest) (*models.DefaultConfigResponse, error) {
	proj, _ := findAnswerByName("Project", request.Answers)
	dataset, _ := findAnswerByName("Dataset", request.Answers)
	tab, _ := findAnswerByName("Table", request.Answers)
	lm, _ := findAnswerByName("LoadMethod", request.Answers)

	conf := []models.PluginConfig{
		{
			Name:  "PROJECT",
			Value: proj.Value,
		},
		{
			Name:  "TABLE",
			Value: tab.Value,
		},
		{
			Name:  "DATASET",
			Value: dataset.Value,
		},
		{
			Name:  "LOAD_METHOD",
			Value: lm.Value,
		},
		{
			Name:  "SQL_TYPE",
			Value: "STANDARD",
		},
	}
	if pf, ok := findAnswerByName("PartitionFilter", request.Answers); ok {
		conf = append(conf, models.PluginConfig{
			Name:  "PARTITION_FILTER",
			Value: pf.Value,
		})
	}
	return &models.DefaultConfigResponse{
		Config: conf,
	}, nil
}

func (b *BQ2BQ) DefaultAssets(ctx context.Context, _ models.DefaultAssetsRequest) (*models.DefaultAssetsResponse, error) {
	return &models.DefaultAssetsResponse{
		Assets: []models.PluginAsset{
			{
				Name: QueryFileName,
				Value: `-- SQL query goes here

Select * from "project.dataset.table";
`,
			},
		},
	}, nil
}

func (b *BQ2BQ) CompileAssets(ctx context.Context, req models.CompileAssetsRequest) (*models.CompileAssetsResponse, error) {
	method, ok := req.Config.Get("LOAD_METHOD")
	if !ok || method.Value != LoadMethodReplace {
		return &models.CompileAssetsResponse{
			Assets: req.Assets,
		}, nil
	}

	// partition window in range
	instanceFileMap := map[string]string{}
	instanceEnvMap := map[string]interface{}{}
	if req.InstanceData != nil {
		for _, jobRunData := range req.InstanceData {
			switch jobRunData.Type {
			case models.InstanceDataTypeFile:
				instanceFileMap[jobRunData.Name] = jobRunData.Value
			case models.InstanceDataTypeEnv:
				instanceEnvMap[jobRunData.Name] = jobRunData.Value
			}
		}
	}

	// TODO: making few assumptions here, should be documented
	// assume destination table is time partitioned
	// assume table is partitioned as DAY
	partitionDelta := time.Hour * 24

	// find destination partitions
	var destinationsPartitions []struct {
		start time.Time
		end   time.Time
	}
	dstart := req.StartTime
	dend := req.EndTime
	for currentPart := dstart; currentPart.Before(dend); currentPart = currentPart.Add(partitionDelta) {
		destinationsPartitions = append(destinationsPartitions, struct {
			start time.Time
			end   time.Time
		}{
			start: currentPart,
			end:   currentPart.Add(partitionDelta),
		})
	}

	// check if window size is greater than partition delta(a DAY), if not do nothing
	if dend.Sub(dstart) <= partitionDelta {
		return &models.CompileAssetsResponse{
			Assets: req.Assets,
		}, nil
	}

	var parsedQueries []string
	var err error

	compiledAssetMap := map[string]string{}
	for _, asset := range req.Assets {
		compiledAssetMap[asset.Name] = asset.Value
	}
	// append job spec assets to list of files need to write
	fileMap := mergeStringMap(instanceFileMap, compiledAssetMap)
	for _, part := range destinationsPartitions {
		instanceEnvMap[ConfigKeyDstart] = part.start.Format(models.InstanceScheduledAtTimeLayout)
		instanceEnvMap[ConfigKeyDend] = part.end.Format(models.InstanceScheduledAtTimeLayout)
		if compiledAssetMap, err = b.TemplateEngine.CompileFiles(fileMap, instanceEnvMap); err != nil {
			return &models.CompileAssetsResponse{}, err
		}
		parsedQueries = append(parsedQueries, compiledAssetMap[QueryFileName])
	}
	compiledAssetMap[QueryFileName] = strings.Join(parsedQueries, QueryFileReplaceBreakMarker)

	taskAssets := models.PluginAssets{}
	for name, val := range compiledAssetMap {
		taskAssets = append(taskAssets, models.PluginAsset{
			Name:  name,
			Value: val,
		})
	}
	return &models.CompileAssetsResponse{
		Assets: taskAssets,
	}, nil
}

func mergeStringMap(mp1, mp2 map[string]string) (mp3 map[string]string) {
	mp3 = make(map[string]string)
	for k, v := range mp1 {
		mp3[k] = v
	}
	for k, v := range mp2 {
		mp3[k] = v
	}
	return mp3
}

// GenerateDestination uses config details to build target table
// this format should match with GenerateDependencies output
func (b *BQ2BQ) GenerateDestination(ctx context.Context, request models.GenerateDestinationRequest) (*models.GenerateDestinationResponse, error) {
	_, span := StartChildSpan(ctx, "GenerateDestination")
	defer span.End()

	proj, ok1 := request.Config.Get("PROJECT")
	dataset, ok2 := request.Config.Get("DATASET")
	tab, ok3 := request.Config.Get("TABLE")
	if ok1 && ok2 && ok3 {
		return &models.GenerateDestinationResponse{
			Destination: fmt.Sprintf("%s:%s.%s", proj.Value, dataset.Value, tab.Value),
			Type:        models.DestinationTypeBigquery,
		}, nil
	}
	return nil, errors.New("missing config key required to generate destination")
}

// GenerateDependencies uses assets to find out the source tables of this
// transformation.
// Try using BQ APIs to search for referenced tables. This work for Select stmts
// but not for Merge/Scripts, for them use regex based search and then create
// fake select stmts. Fake statements help finding actual referenced tables in
// case regex based table is a view & not actually a source table. Because this
// fn should generate the actual source as dependency
// BQ2BQ dependencies are BQ tables in format "project:dataset.table"
func (b *BQ2BQ) GenerateDependencies(ctx context.Context, request models.GenerateDependenciesRequest) (response *models.GenerateDependenciesResponse, err error) {
	spanCtx, span := StartChildSpan(ctx, "GenerateDependencies")
	defer span.End()

	response = &models.GenerateDependenciesResponse{}
	response.Dependencies = []string{}

	// check if exists in cache
	if cachedResponse, err := b.IsCached(request); err == nil {
		// cache ready
		span.AddEvent("Request found in cache")
		return cachedResponse, nil
	} else if err != ErrCacheNotFound {
		return nil, err
	}

	var svcAcc string
	accConfig, ok := request.Config.Get(BqServiceAccount)
	if !ok || len(accConfig.Value) == 0 {
		span.AddEvent("Required secret BQ_SERVICE_ACCOUNT not found in config")
		// Fallback to project for getting the secret
		svcAcc, ok = request.Project.Secret.GetByName(SecretName)
		if !ok || len(svcAcc) == 0 {
			return response, fmt.Errorf("secret %s required to generate dependencies not found for %s", SecretName, Name)
		}
	} else {
		svcAcc = accConfig.Value
	}

	queryData, ok := request.Assets.Get(QueryFileName)
	if !ok {
		return nil, errors.New("empty sql file")
	}

	selfTable, err := b.GenerateDestination(spanCtx, models.GenerateDestinationRequest{
		Config: request.Config,
		Assets: request.Assets,
	})
	if err != nil {
		return response, err
	}

	// first parse sql statement to find dependencies and ignored tables
	parsedDependencies, ignoredDependencies, err := b.FindDependenciesWithRegex(spanCtx, queryData.Value, selfTable.Destination)
	if err != nil {
		return response, err
	}

	timeoutCtx, cancel := context.WithTimeout(ctx, TimeoutDuration)
	defer cancel()

	// try to resolve referenced tables for ignoredDependencies
	var ignoredDependenciesReferencedTables []string
	for _, tableName := range ignoredDependencies {
		// ignore the tables with :
		if strings.Contains(tableName, ":") { // project:dataset.table
			continue
		}
		// find referenced tables and add it to ignoredDependenciesReferencedTables
		fakeQuery := fmt.Sprintf(FakeSelectStmt, tableName)
		deps, err := b.FindDependenciesWithRetryableDryRun(timeoutCtx, fakeQuery, svcAcc)
		if err != nil {
			return response, err
		}
		ignoredDependenciesReferencedTables = append(ignoredDependenciesReferencedTables, deps...)
	}
	ignoredDependencies = append(ignoredDependencies, ignoredDependenciesReferencedTables...)

	// try to resolve referenced tables directly from BQ APIs
	response.Dependencies, err = b.FindDependenciesWithRetryableDryRun(spanCtx, queryData.Value, svcAcc)
	if err != nil {
		// SQL query with reference to destination table such as DML and self joins will have dependency
		// cycle on dry run since the table might not be available yet. We check the error from BQ
		// to ignore if the error message contains destination table not found.
		if !strings.Contains(err.Error(), fmt.Sprintf("Not found: Table %s was not found", selfTable.Destination)) {
			return response, err
		}
	}

	if len(response.Dependencies) == 0 {
		span.AddEvent("Unable to get dependencies, query tables on regex")
		// stmt could be BQ script, find table names using regex and create
		// fake Select STMTs to find actual referenced tables

		resultChan := make(chan []string)
		eg, apiCtx := errgroup.WithContext(spanCtx) // it will stop executing further after first error
		for _, tableName := range parsedDependencies {
			fakeQuery := fmt.Sprintf(FakeSelectStmt, tableName)
			// find dependencies in parallel
			eg.Go(func() error {
				//prepare dummy query
				deps, err := b.FindDependenciesWithRetryableDryRun(spanCtx, fakeQuery, svcAcc)
				if err != nil {
					return err
				}
				select {
				case resultChan <- deps:
					return nil
				// requests to be cancelled
				case <-apiCtx.Done():
					return apiCtx.Err()
				}
			})
		}

		go func() {
			// if all done, stop waiting for results
			eg.Wait()
			close(resultChan)
		}()

		// accumulate results
		for dep := range resultChan {
			response.Dependencies = append(response.Dependencies, dep...)
		}

		// check if wait was finished because of an error
		if err := eg.Wait(); err != nil {
			return response, err
		}
	}

	response.Dependencies = removeString(response.Dependencies, selfTable.Destination)

	// before returning remove ignored tables
	for _, ignored := range ignoredDependencies {
		response.Dependencies = removeString(response.Dependencies, ignored)
	}

	// before returning wrap dependencies with datastore type
	dedupDependency := make(map[string]int)
	for _, dependency := range response.Dependencies {
		dedupDependency[fmt.Sprintf(models.DestinationURNFormat, selfTable.Type, dependency)] = 0
	}
	var dependencies []string
	for dependency := range dedupDependency {
		dependencies = append(dependencies, dependency)
	}
	response.Dependencies = dependencies

	b.Cache(request, response)
	return response, nil
}

// FindDependenciesWithRegex look for table patterns in SQL query to find
// source tables.
// Task destination is required to avoid cycles
//
// we look for certain patterns in the query source code
// in particular, we look for the following constructs
// * from {table} ...
// * join {table} ...
// * with {table} as ...
// where {table} => {project}.{dataset}.{name}
// for `from` and `join` we build a optimus.Table object and
// store it's name in a set. For `with` query we store the name in
// a separate set called `pseudoTables` that is used for filtering
// out tables from `from`/`join` matches.
// the algorithm roughly locates all from/join clauses, filters it
// in case it's a known pseudo table (since with queries come before
// either `from` or `join` queries, so they're match first).
// notice that only clauses that end in "." delimited sequences
// are matched (for instance: foo.bar.baz, but not foo.bar).
// This helps weed out pseudo tables since most of the time
// they're a single sequence of characters. But on the other hand
// this also means that otherwise valid reference to "dataset.table"
// will not be recognised.
func (b *BQ2BQ) FindDependenciesWithRegex(ctx context.Context, queryString string, destination string) ([]string, []string, error) {
	_, span := StartChildSpan(ctx, "FindDependenciesWithRegex")
	defer span.End()

	tablesFound := make(map[string]bool)
	pseudoTables := make(map[string]bool)
	var tablesIgnored []string

	// we mark destination as a pseudo table to avoid a dependency
	// cycle. This is for supporting DML queries that may also refer
	// to themselves.

	pseudoTables[destination] = true

	// remove comments from query
	matches := queryCommentPatterns.FindAllStringSubmatch(queryString, -1)
	for _, match := range matches {
		helperToken := match[2]

		// check if its a helper
		if helperPattern.MatchString(helperToken) {
			continue
		}

		// replace full match
		queryString = strings.ReplaceAll(queryString, match[0], " ")
	}

	matches = tableDestinationPatterns.FindAllStringSubmatch(queryString, -1)
	for _, match := range matches {
		var projectIdx, datasetIdx, nameIdx, ignoreUpstreamIdx int
		tokens := strings.Fields(match[0])
		clause := strings.ToLower(tokens[0])

		switch clause {
		case "from":
			ignoreUpstreamIdx, projectIdx, datasetIdx, nameIdx = 1, 2, 3, 4
		case "join":
			ignoreUpstreamIdx, projectIdx, datasetIdx, nameIdx = 5, 6, 7, 8
		case "with":
			ignoreUpstreamIdx, projectIdx, datasetIdx, nameIdx = 9, 10, 11, 12
		}

		tableName := createTableName(match[projectIdx], match[datasetIdx], match[nameIdx])

		// if upstream is ignored, don't treat it as source
		if strings.TrimSpace(match[ignoreUpstreamIdx]) == "@ignoreupstream" {
			// make sure to handle both the conventions
			tablesIgnored = append(tablesIgnored, tableName)
			tablesIgnored = append(tablesIgnored, createTableNameWithColon(match[projectIdx], match[datasetIdx], match[nameIdx]))
			continue
		}

		if clause == "with" {
			pseudoTables[tableName] = true
		} else {
			tablesFound[tableName] = true
		}
	}
	var tables []string
	for table := range tablesFound {
		if pseudoTables[table] {
			continue
		}
		tables = append(tables, table)
	}
	return tables, tablesIgnored, nil
}

func (b *BQ2BQ) FindDependenciesWithRetryableDryRun(ctx context.Context, query, svcAccSecret string) ([]string, error) {
	spanCtx, span := StartChildSpan(ctx, "FindDependenciesWithRetryableDryRun")
	defer span.End()

	for try := 1; try <= MaxBQApiRetries; try++ {
		client, err := b.ClientFac.New(spanCtx, svcAccSecret)
		if err != nil {
			return nil, errors.New("failed to create bigquery client")
		}
		deps, err := b.FindDependenciesWithDryRun(spanCtx, client, query)
		if err != nil {
			if strings.Contains(err.Error(), "net/http: TLS handshake timeout") ||
				strings.Contains(err.Error(), "unexpected EOF") ||
				strings.Contains(err.Error(), "i/o timeout") ||
				strings.Contains(err.Error(), "connection reset by peer") {
				// retry
				continue
			}

			return nil, err
		}
		return deps, nil
	}
	return nil, errors.New("bigquery api retries exhausted")
}

func (b *BQ2BQ) FindDependenciesWithDryRun(ctx context.Context, client bqiface.Client, query string) ([]string, error) {
	spanCtx, span := StartChildSpan(ctx, "FindDependenciesWithDryRun")
	defer span.End()
	span.SetAttributes(attribute.String("kind", "client"), attribute.String("client.type", "bigquery"))

	q := client.Query(query)
	q.SetQueryConfig(bqiface.QueryConfig{
		QueryConfig: bigquery.QueryConfig{
			Q:      query,
			DryRun: true,
		},
	})

	job, err := q.Run(spanCtx)
	if err != nil {
		return nil, fmt.Errorf("query run: %w", err)
	}
	// Dry run is not asynchronous, so get the latest status and statistics.
	status := job.LastStatus()
	if err := status.Err(); err != nil {
		return nil, fmt.Errorf("query status: %w", err)
	}

	details, ok := status.Statistics.Details.(*bigquery.QueryStatistics)
	if !ok {
		return nil, errors.New("failed to cast to Query Statistics")
	}

	tables := []string{}
	for _, tab := range details.ReferencedTables {
		tables = append(tables, tab.FullyQualifiedName())
	}
	return tables, nil
}

func createTableName(proj, dataset, table string) string {
	return fmt.Sprintf("%s.%s.%s", proj, dataset, table)
}

func createTableNameWithColon(proj, dataset, table string) string {
	return fmt.Sprintf("%s:%s.%s", proj, dataset, table)
}

func removeString(s []string, match string) []string {
	if len(s) == 0 {
		return s
	}
	idx := -1
	for i, tab := range s {
		if tab == match {
			idx = i
			break
		}
	}
	// not found
	if idx < 0 {
		return s
	}
	s[len(s)-1], s[idx] = s[idx], s[len(s)-1]
	return s[:len(s)-1]
}

func (b *BQ2BQ) IsCached(request models.GenerateDependenciesRequest) (*models.GenerateDependenciesResponse, error) {
	if b.C == nil {
		return nil, ErrCacheNotFound
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	requestHash, err := hashstructure.Hash(request, hashstructure.FormatV2, nil)
	if err != nil {
		return nil, err
	}
	hashString := cast.ToString(requestHash)
	if item, ok := b.C.Get(hashString); ok {
		return item.(*models.GenerateDependenciesResponse), nil
	}
	return nil, ErrCacheNotFound
}

func (b *BQ2BQ) Cache(request models.GenerateDependenciesRequest, response *models.GenerateDependenciesResponse) error {
	if b.C == nil {
		return nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	requestHash, err := hashstructure.Hash(request, hashstructure.FormatV2, nil)
	if err != nil {
		return err
	}
	hashString := cast.ToString(requestHash)
	b.C.Set(hashString, response, cache.DefaultExpiration)
	return nil
}

func main() {
	var tracingAddr string
	flag.StringVar(&tracingAddr, "t", "", "endpoint for traces collector")
	flag.Parse()

	var cleanupFunc func()
	plugin.Serve(func(log hclog.Logger) interface{} {
		var err error
		log.Info("Telemetry setup with", tracingAddr)
		cleanupFunc, err = InitTelemetry(log, tracingAddr)
		if err != nil {
			log.Warn("Error while telemetry init")
		}

		return &BQ2BQ{
			ClientFac:      &DefaultBQClientFactory{},
			C:              cache.New(CacheTTL, CacheCleanUp),
			TemplateEngine: compiler.NewGoEngine(),
			logger:         log,
		}
	})
	cleanupFunc()
}
