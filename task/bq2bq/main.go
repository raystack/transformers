package main

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/odpf/optimus/instance"
	"github.com/odpf/optimus/plugin"

	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/plugin/task"

	hplugin "github.com/hashicorp/go-plugin"
	"github.com/mitchellh/hashstructure/v2"
	"github.com/patrickmn/go-cache"
	"github.com/spf13/cast"

	"cloud.google.com/go/bigquery"
	"github.com/AlecAivazis/survey/v2"
	"github.com/googleapis/google-cloud-go-testing/bigquery/bqiface"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

var (
	Name = "bq2bq"

	// should be injected while building
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
		"(?i)(?:FROM)\\s*(?:/\\*\\s*([a-zA-Z0-9@_-]*)\\s*\\*/)?\\s+`?([\\w-]+)\\.([\\w-]+)\\.(\\w+)`?" +
		"|" +
		"(?i)(?:JOIN)\\s*(?:/\\*\\s*([a-zA-Z0-9@_-]*)\\s*\\*/)?\\s+`?([\\w-]+)\\.([\\w-]+)\\.(\\w+)`?" +
		"|" +
		"(?i)(?:WITH)\\s*(?:/\\*\\s*([a-zA-Z0-9@_-]*)\\s*\\*/)?\\s+`?([\\w-]+)\\.([\\w-]+)\\.(\\w+)`?\\s+(?:AS)")

	queryCommentPatterns = regexp.MustCompile("(--.*)|(((/\\*)+?[\\w\\W]*?(\\*/)+))")
	helperPattern        = regexp.MustCompile("(\\/\\*\\s*(@[a-zA-Z0-9_-]+)\\s*\\*\\/)")

	QueryFileName = "query.sql"

	// Required secret
	SecretName = "TASK_BQ2BQ"

	TimeoutDuration = time.Second * 180
	MaxBQApiRetries = 3
	FakeSelectStmt  = "SELECT * from `%s` WHERE FALSE LIMIT 1"

	CacheTTL         = time.Hour * 24
	CacheCleanUp     = time.Hour * 1
	ErrCacheNotFound = errors.New("item not found")

	LoadMethodMerge        = "MERGE"
	LoadMethodAppend       = "APPEND"
	LoadMethodReplace      = "REPLACE"
	LoadMethodReplaceMerge = "REPLACE_MERGE"

	QueryFileReplaceBreakMarker = "\n--*--optimus-break-marker--*--\n"
)

type ClientFactory interface {
	New(ctx context.Context, svcAccount string) (bqiface.Client, error)
}

type BQ2BQ struct {
	ClientFac      ClientFactory
	mu             sync.Mutex
	C              *cache.Cache
	TemplateEngine models.TemplateEngine
}

func (b *BQ2BQ) GetTaskSchema(ctx context.Context, req models.GetTaskSchemaRequest) (models.GetTaskSchemaResponse, error) {
	return models.GetTaskSchemaResponse{
		Name:        Name,
		Description: "BigQuery to BigQuery transformation task",
		Image:       fmt.Sprintf("%s:%s", Image, Version),
		SecretPath:  "/tmp/auth.json",
	}, nil
}

func (b *BQ2BQ) GetTaskQuestions(ctx context.Context, req models.GetTaskQuestionsRequest) (models.GetTaskQuestionsResponse, error) {
	tQues := []models.PluginQuestion{
		{
			Name:   "Project",
			Prompt: "Project ID",
			Help:   "Bigquery Project ID",
		},
		{
			Name:   "Dataset",
			Prompt: "Dataset Name",
			Help:   "Bigquery Dataset ID",
		},
		{
			Name:   "Table",
			Prompt: "Table ID",
			Help:   "Bigquery Table ID",
		},
		{
			Name:   "LoadMethod",
			Prompt: "Load method to use on destination",
			Help: `
REPLACE       - Deletes existing partition and insert result of select query
MERGE         - DML statements, BQ scripts
APPEND        - Append to existing table
REPLACE_MERGE - [Experimental] Advanced replace using merge query
`,
			Multiselect:         []string{LoadMethodReplace, LoadMethodMerge, LoadMethodAppend, LoadMethodReplaceMerge},
			SubQuestionsIfValue: LoadMethodReplaceMerge,
			SubQuestions: []models.PluginQuestion{
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
	}
	return models.GetTaskQuestionsResponse{
		Questions: tQues,
	}, nil
}

func (b *BQ2BQ) ValidateTaskQuestion(ctx context.Context, req models.ValidateTaskQuestionRequest) (models.ValidateTaskQuestionResponse, error) {
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
		return models.ValidateTaskQuestionResponse{
			Success: false,
			Error:   err.Error(),
		}, nil
	}
	return models.ValidateTaskQuestionResponse{
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

func (b *BQ2BQ) DefaultTaskConfig(ctx context.Context, request models.DefaultTaskConfigRequest) (models.DefaultTaskConfigResponse, error) {
	proj, _ := findAnswerByName("Project", request.Answers)
	dataset, _ := findAnswerByName("Table", request.Answers)
	tab, _ := findAnswerByName("Dataset", request.Answers)
	lm, _ := findAnswerByName("LoadMethod", request.Answers)

	conf := []models.TaskPluginConfig{
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
		conf = append(conf, models.TaskPluginConfig{
			Name:  "PARTITION_FILTER",
			Value: pf.Value,
		})
	}
	return models.DefaultTaskConfigResponse{
		Config: conf,
	}, nil
}

func (b *BQ2BQ) DefaultTaskAssets(ctx context.Context, _ models.DefaultTaskAssetsRequest) (models.DefaultTaskAssetsResponse, error) {
	return models.DefaultTaskAssetsResponse{
		Assets: []models.TaskPluginAsset{
			{
				Name: QueryFileName,
				Value: `-- SQL query goes here

Select * from "project.dataset.table";
`,
			},
		},
	}, nil
}

func (b *BQ2BQ) CompileTaskAssets(ctx context.Context, req models.CompileTaskAssetsRequest) (models.CompileTaskAssetsResponse, error) {
	method, ok := req.Config.Get("LOAD_METHOD")
	if !ok || method.Value != LoadMethodReplace {
		return models.CompileTaskAssetsResponse{
			Assets: req.Assets,
		}, nil
	}

	// TODO: making few assumptions here, should be documented
	// assume destination table is time partitioned
	// assume table is partitioned as DAY

	// check if window size is greater than a DAY, if not do nothing
	partitionDelta := time.Hour * 24
	if req.TaskWindow.Size <= partitionDelta {
		return models.CompileTaskAssetsResponse{
			Assets: req.Assets,
		}, nil
	}

	// partition window in range
	instanceFileMap := map[string]string{}
	instanceEnvMap := map[string]string{}
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

	// find destination partitions
	var destinationsPartitions []struct {
		start time.Time
		end   time.Time
	}
	dstart := req.TaskWindow.GetStart(req.InstanceSchedule)
	dend := req.TaskWindow.GetEnd(req.InstanceSchedule)
	for currentPart := dstart; currentPart.Before(dend); currentPart = currentPart.Add(partitionDelta) {
		destinationsPartitions = append(destinationsPartitions, struct {
			start time.Time
			end   time.Time
		}{
			start: currentPart,
			end:   currentPart.Add(partitionDelta),
		})
	}

	parsedQueries := []string{}
	var err error

	compiledAssetMap := map[string]string{}
	for _, asset := range req.Assets {
		compiledAssetMap[asset.Name] = asset.Value
	}
	// append job spec assets to list of files need to write
	fileMap := instance.MergeStringMap(instanceFileMap, compiledAssetMap)
	for _, part := range destinationsPartitions {
		instanceEnvMap[instance.ConfigKeyDstart] = part.start.Format(models.InstanceScheduledAtTimeLayout)
		instanceEnvMap[instance.ConfigKeyDend] = part.end.Format(models.InstanceScheduledAtTimeLayout)
		if compiledAssetMap, err = b.TemplateEngine.CompileFiles(fileMap, instance.ConvertStringToInterfaceMap(instanceEnvMap)); err != nil {
			return models.CompileTaskAssetsResponse{}, err
		}
		parsedQueries = append(parsedQueries, compiledAssetMap[QueryFileName])
	}
	compiledAssetMap[QueryFileName] = strings.Join(parsedQueries, QueryFileReplaceBreakMarker)

	taskAssets := models.TaskPluginAssets{}
	for name, val := range compiledAssetMap {
		taskAssets = append(taskAssets, models.TaskPluginAsset{
			Name:  name,
			Value: val,
		})
	}
	return models.CompileTaskAssetsResponse{
		Assets: taskAssets,
	}, nil
}

// GenerateTaskDestination uses config details to build target table
// this format should match with GenerateTaskDependencies output
func (b *BQ2BQ) GenerateTaskDestination(ctx context.Context, request models.GenerateTaskDestinationRequest) (models.GenerateTaskDestinationResponse, error) {
	proj, ok1 := request.Config.Get("PROJECT")
	dataset, ok2 := request.Config.Get("DATASET")
	tab, ok3 := request.Config.Get("TABLE")
	if ok1 && ok2 && ok3 {
		return models.GenerateTaskDestinationResponse{
			Destination: fmt.Sprintf("%s:%s.%s", proj.Value, dataset.Value, tab.Value),
		}, nil
	}
	return models.GenerateTaskDestinationResponse{}, errors.New("missing config key required to generate destination")
}

// GenerateTaskDependencies uses assets to find out the source tables of this
// transformation.
// Try using BQ APIs to search for referenced tables. This work for Select stmts
// but not for Merge/Scripts, for them use regex based search and then create
// fake select stmts. Fake statements help finding actual referenced tables in
// case regex based table is a view & not actually a source table. Because this
// fn should generate the actual source as dependency
// BQ2BQ dependencies are BQ tables in format "project:dataset.table"
func (b *BQ2BQ) GenerateTaskDependencies(ctx context.Context, request models.GenerateTaskDependenciesRequest) (response models.GenerateTaskDependenciesResponse, err error) {
	response.Dependencies = []string{}

	// check if exists in cache
	if cachedResponse, err := b.IsCached(request); err == nil {
		// cache ready
		return cachedResponse, nil
	} else if err != ErrCacheNotFound {
		return models.GenerateTaskDependenciesResponse{}, err
	}

	timeoutCtx, cancel := context.WithTimeout(ctx, TimeoutDuration)
	defer cancel()

	svcAcc, ok := request.Project.Secret.GetByName(SecretName)
	if !ok || len(svcAcc) == 0 {
		return response, errors.New(fmt.Sprintf("secret %s required to generate dependencies not found for %s", SecretName, Name))
	}

	queryData, ok := request.Assets.Get(QueryFileName)
	if !ok {
		return models.GenerateTaskDependenciesResponse{}, errors.New("empty sql file")
	}

	// first parse sql statement to find dependencies and ignored tables
	parsedDependencies, ignoredDependencies, err := b.FindDependenciesWithRegex(ctx, request)
	if err != nil {
		return response, err
	}

	// try to resolve referenced tables directly from BQ APIs
	response.Dependencies, err = b.FindDependenciesWithRetryableDryRun(timeoutCtx, queryData.Value, svcAcc)
	if err != nil {
		return response, err
	}

	if len(response.Dependencies) == 0 {
		// stmt could be BQ script, find table names using regex and create
		// fake Select STMTs to find actual referenced tables

		resultChan := make(chan []string)
		eg, apiCtx := errgroup.WithContext(timeoutCtx) // it will stop executing further after first error
		for _, tableName := range parsedDependencies {
			fakeQuery := fmt.Sprintf(FakeSelectStmt, tableName)
			// find dependencies in parallel
			eg.Go(func() error {
				//prepare dummy query
				deps, err := b.FindDependenciesWithRetryableDryRun(timeoutCtx, fakeQuery, svcAcc)
				if err != nil {
					return err
				}
				select {
				case resultChan <- deps:
					return nil
				// timeoutCtx requests to be cancelled
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

	// before returning remove self
	selfTable, err := b.GenerateTaskDestination(ctx, models.GenerateTaskDestinationRequest{
		Config:  request.Config,
		Assets:  request.Assets,
		Project: request.Project,
	})
	if err != nil {
		return response, err
	}
	response.Dependencies = removeString(response.Dependencies, selfTable.Destination)

	// before returning remove ignored tables
	for _, ignored := range ignoredDependencies {
		response.Dependencies = removeString(response.Dependencies, ignored)
	}

	b.Cache(request, response)
	return response, nil
}

// FindDependenciesWithRegex look for table patterns in SQL query to find
// source tables.
// Config is required to generate destination and avoid cycles
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
func (b *BQ2BQ) FindDependenciesWithRegex(ctx context.Context, request models.GenerateTaskDependenciesRequest) ([]string, []string, error) {

	queryData, ok := request.Assets.Get(QueryFileName)
	if !ok {
		return nil, nil, errors.New("empty sql file")
	}
	queryString := queryData.Value
	tablesFound := make(map[string]bool)
	pseudoTables := make(map[string]bool)
	var tablesIgnored []string

	// we mark destination as a pseudo table to avoid a dependency
	// cycle. This is for supporting DML queries that may also refer
	// to themselves.
	dest, err := b.GenerateTaskDestination(ctx, models.GenerateTaskDestinationRequest{
		Config:  request.Config,
		Assets:  request.Assets,
		Project: request.Project,
	})
	if err != nil {
		return nil, nil, err
	}
	pseudoTables[dest.Destination] = true

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
	for try := 1; try <= MaxBQApiRetries; try++ {
		client, err := b.ClientFac.New(ctx, svcAccSecret)
		if err != nil {
			return nil, errors.New("bigquery client")
		}
		deps, err := b.FindDependenciesWithDryRun(ctx, client, query)
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
	q := client.Query(query)
	q.SetQueryConfig(bqiface.QueryConfig{
		QueryConfig: bigquery.QueryConfig{
			Q:      query,
			DryRun: true,
		},
	})

	job, err := q.Run(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "query run")
	}
	// Dry run is not asynchronous, so get the latest status and statistics.
	status := job.LastStatus()
	if err := status.Err(); err != nil {
		return nil, errors.Wrap(err, "query status")
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

func deduplicateStrings(in []string) []string {
	if len(in) == 0 {
		return in
	}

	sort.Strings(in)
	j := 0
	for i := 1; i < len(in); i++ {
		if in[j] == in[i] {
			continue
		}
		j++
		// preserve the original data
		// in[i], in[j] = in[j], in[i]
		// only set what is required
		in[j] = in[i]
	}
	return in[:j+1]
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

func (b *BQ2BQ) IsCached(request models.GenerateTaskDependenciesRequest) (models.GenerateTaskDependenciesResponse, error) {
	if b.C == nil {
		return models.GenerateTaskDependenciesResponse{}, ErrCacheNotFound
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	requestHash, err := hashstructure.Hash(request, hashstructure.FormatV2, nil)
	if err != nil {
		return models.GenerateTaskDependenciesResponse{}, err
	}
	hashString := cast.ToString(requestHash)
	if item, ok := b.C.Get(hashString); ok {
		return item.(models.GenerateTaskDependenciesResponse), nil
	}
	return models.GenerateTaskDependenciesResponse{}, ErrCacheNotFound
}

func (b *BQ2BQ) Cache(request models.GenerateTaskDependenciesRequest, response models.GenerateTaskDependenciesResponse) error {
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
	bq2bq := &BQ2BQ{
		ClientFac:      &DefaultBQClientFactory{},
		C:              cache.New(CacheTTL, CacheCleanUp),
		TemplateEngine: instance.NewGoEngine(),
	}

	var handshakeConfig = hplugin.HandshakeConfig{
		ProtocolVersion:  1,
		MagicCookieKey:   plugin.MagicCookieKey,
		MagicCookieValue: plugin.MagicCookieValue,
	}
	hplugin.Serve(&hplugin.ServeConfig{
		HandshakeConfig: handshakeConfig,
		Plugins: map[string]hplugin.Plugin{
			plugin.TaskPluginName: task.NewPlugin(bq2bq),
		},
		// A non-nil value here enables gRPC serving for this plugin...
		GRPCServer: hplugin.DefaultGRPCServer,
	})
}
