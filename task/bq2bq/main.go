package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/googleapis/google-cloud-go-testing/bigquery/bqiface"
	oplugin "github.com/goto/optimus/plugin"
	"github.com/goto/optimus/sdk/plugin"
	"github.com/hashicorp/go-hclog"
	"github.com/mitchellh/hashstructure/v2"
	"github.com/patrickmn/go-cache"
	"github.com/spf13/cast"

	"github.com/goto/transformers/task/bq2bq/upstream"
)

const (
	ConfigKeyDstart = "DSTART"
	ConfigKeyDend   = "DEND"

	dataTypeEnv             = "env"
	dataTypeFile            = "file"
	destinationTypeBigquery = "bigquery"
	scheduledAtTimeLayout   = time.RFC3339
)

var (
	Name = "bq2bq"

	// Version should be injected while building
	Version = "dev"

	QueryFileName = "query.sql"

	BqServiceAccount = "BQ_SERVICE_ACCOUNT"

	TimeoutDuration = time.Second * 180
	MaxBQApiRetries = 3
	FakeSelectStmt  = "SELECT * from `%s` WHERE FALSE LIMIT 1"

	CacheTTL         = time.Hour * 24
	CacheCleanUp     = time.Hour * 6
	ErrCacheNotFound = errors.New("item not found")

	LoadMethod        = "LOAD_METHOD"
	LoadMethodReplace = "REPLACE"

	QueryFileReplaceBreakMarker = "\n--*--optimus-break-marker--*--\n"

	_ plugin.DependencyResolverMod = &BQ2BQ{}
)

type ClientFactory interface {
	New(ctx context.Context, svcAccount string) (bqiface.Client, error)
}

type UpstreamExtractor interface {
	ExtractUpstreams(ctx context.Context, query string, resourcesToIgnore []upstream.Resource) ([]*upstream.Upstream, error)
}

type ExtractorFactory interface {
	New(client bqiface.Client) (UpstreamExtractor, error)
}

type BQ2BQ struct {
	ClientFac    ClientFactory
	ExtractorFac ExtractorFactory
	mu           sync.Mutex
	C            *cache.Cache
	Compiler     *Compiler

	logger hclog.Logger
}

func (*BQ2BQ) GetName(_ context.Context) (string, error) {
	return Name, nil
}

func (b *BQ2BQ) CompileAssets(ctx context.Context, req plugin.CompileAssetsRequest) (*plugin.CompileAssetsResponse, error) {
	method, ok := req.Config.Get(LoadMethod)
	if !ok || method.Value != LoadMethodReplace {
		return &plugin.CompileAssetsResponse{
			Assets: req.Assets,
		}, nil
	}

	// partition window in range
	instanceFileMap := map[string]string{}
	instanceEnvMap := map[string]interface{}{}
	if req.InstanceData != nil {
		for _, jobRunData := range req.InstanceData {
			switch jobRunData.Type {
			case dataTypeFile:
				instanceFileMap[jobRunData.Name] = jobRunData.Value
			case dataTypeEnv:
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
		return &plugin.CompileAssetsResponse{
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
		instanceEnvMap[ConfigKeyDstart] = part.start.Format(scheduledAtTimeLayout)
		instanceEnvMap[ConfigKeyDend] = part.end.Format(scheduledAtTimeLayout)
		if compiledAssetMap, err = b.Compiler.Compile(fileMap, instanceEnvMap); err != nil {
			return &plugin.CompileAssetsResponse{}, err
		}
		parsedQueries = append(parsedQueries, compiledAssetMap[QueryFileName])
	}
	compiledAssetMap[QueryFileName] = strings.Join(parsedQueries, QueryFileReplaceBreakMarker)

	taskAssets := plugin.Assets{}
	for name, val := range compiledAssetMap {
		taskAssets = append(taskAssets, plugin.Asset{
			Name:  name,
			Value: val,
		})
	}
	return &plugin.CompileAssetsResponse{
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
func (b *BQ2BQ) GenerateDestination(ctx context.Context, request plugin.GenerateDestinationRequest) (*plugin.GenerateDestinationResponse, error) {
	_, span := StartChildSpan(ctx, "GenerateDestination")
	defer span.End()

	proj, ok1 := request.Config.Get("PROJECT")
	dataset, ok2 := request.Config.Get("DATASET")
	tab, ok3 := request.Config.Get("TABLE")
	if ok1 && ok2 && ok3 {
		return &plugin.GenerateDestinationResponse{
			Destination: fmt.Sprintf("%s:%s.%s", proj.Value, dataset.Value, tab.Value),
			Type:        destinationTypeBigquery,
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
func (b *BQ2BQ) GenerateDependencies(ctx context.Context, request plugin.GenerateDependenciesRequest) (response *plugin.GenerateDependenciesResponse, err error) {
	spanCtx, span := StartChildSpan(ctx, "GenerateDependencies")
	defer span.End()

	response = &plugin.GenerateDependenciesResponse{}
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
		return response, fmt.Errorf("secret BQ_SERVICE_ACCOUNT required to generate dependencies not found for %s", Name)
	} else {
		svcAcc = accConfig.Value
	}

	queryData, ok := request.Assets.Get(QueryFileName)
	if !ok {
		return nil, errors.New("empty sql file")
	}

	selfTable, err := b.GenerateDestination(spanCtx, plugin.GenerateDestinationRequest{
		Config: request.Config,
		Assets: request.Assets,
	})
	if err != nil {
		return response, err
	}

	destinationResource, err := b.destinationToResource(selfTable)
	if err != nil {
		return response, fmt.Errorf("error getting destination resource: %w", err)
	}

	upstreams, err := b.extractUpstreams(spanCtx, queryData.Value, svcAcc, []upstream.Resource{destinationResource})
	if err != nil {
		return response, fmt.Errorf("error extracting upstreams: %w", err)
	}

	flattenedUpstreams := upstream.FlattenUpstreams(upstreams)
	formattedUpstreams := b.formatUpstreams(flattenedUpstreams, func(r upstream.Resource) string {
		name := fmt.Sprintf("%s:%s.%s", r.Project, r.Dataset, r.Name)
		return fmt.Sprintf(plugin.DestinationURNFormat, selfTable.Type, name)
	})

	response.Dependencies = formattedUpstreams

	b.Cache(request, response)
	return response, nil
}

func (b *BQ2BQ) extractUpstreams(ctx context.Context, query, svcAccSecret string, resourcesToIgnore []upstream.Resource) ([]*upstream.Upstream, error) {
	spanCtx, span := StartChildSpan(ctx, "extractUpstreams")
	defer span.End()

	for try := 1; try <= MaxBQApiRetries; try++ {
		client, err := b.ClientFac.New(spanCtx, svcAccSecret)
		if err != nil {
			return nil, fmt.Errorf("error creating bigquery client: %w", err)
		}

		extractor, err := b.ExtractorFac.New(client)
		if err != nil {
			return nil, fmt.Errorf("error initializing upstream extractor: %w", err)
		}

		upstreams, err := extractor.ExtractUpstreams(spanCtx, query, resourcesToIgnore)
		if err != nil {
			if strings.Contains(err.Error(), "net/http: TLS handshake timeout") ||
				strings.Contains(err.Error(), "unexpected EOF") ||
				strings.Contains(err.Error(), "i/o timeout") ||
				strings.Contains(err.Error(), "connection reset by peer") {
				// retry
				continue
			}

			return nil, fmt.Errorf("error extracting upstreams: %w", err)
		}

		return upstreams, nil
	}
	return nil, errors.New("bigquery api retries exhausted")
}

func (b *BQ2BQ) destinationToResource(destination *plugin.GenerateDestinationResponse) (upstream.Resource, error) {
	splitDestination := strings.Split(destination.Destination, ":")
	if len(splitDestination) != 2 {
		return upstream.Resource{}, fmt.Errorf("cannot get project from destination [%s]", destination.Destination)
	}

	project, datasetTable := splitDestination[0], splitDestination[1]

	splitDataset := strings.Split(datasetTable, ".")
	if len(splitDataset) != 2 {
		return upstream.Resource{}, fmt.Errorf("cannot get dataset and table from [%s]", datasetTable)
	}

	return upstream.Resource{
		Project: project,
		Dataset: splitDataset[0],
		Name:    splitDataset[1],
	}, nil
}

func (b *BQ2BQ) formatUpstreams(upstreams []upstream.Resource, fn func(r upstream.Resource) string) []string {
	var output []string
	for _, u := range upstreams {
		output = append(output, fn(u))
	}

	return output
}

func (b *BQ2BQ) IsCached(request plugin.GenerateDependenciesRequest) (*plugin.GenerateDependenciesResponse, error) {
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
		return item.(*plugin.GenerateDependenciesResponse), nil
	}
	return nil, ErrCacheNotFound
}

func (b *BQ2BQ) Cache(request plugin.GenerateDependenciesRequest, response *plugin.GenerateDependenciesResponse) error {
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
	oplugin.Serve(func(log hclog.Logger) interface{} {
		var err error
		log.Info("Telemetry setup with", tracingAddr)
		cleanupFunc, err = InitTelemetry(log, tracingAddr)
		if err != nil {
			log.Warn("Error while telemetry init")
		}

		return &BQ2BQ{
			ClientFac:    &DefaultBQClientFactory{},
			ExtractorFac: &DefaultUpstreamExtractorFactory{},
			C:            cache.New(CacheTTL, CacheCleanUp),
			Compiler:     NewCompiler(),
			logger:       log,
		}
	})
	cleanupFunc()
}
