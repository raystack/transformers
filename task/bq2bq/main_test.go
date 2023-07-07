package main

import (
	"context"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/googleapis/google-cloud-go-testing/bigquery/bqiface"
	"github.com/goto/optimus/sdk/plugin"
	"github.com/patrickmn/go-cache"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/goto/transformers/task/bq2bq/upstream"
)

type bqClientMock struct {
	mock.Mock
	bqiface.Client
}

type bqClientFactoryMock struct {
	mock.Mock
}

func (fac *bqClientFactoryMock) New(ctx context.Context, svcAcc string) (bqiface.Client, error) {
	args := fac.Called(ctx, svcAcc)
	return args.Get(0).(bqiface.Client), args.Error(1)
}

type extractorFactoryMock struct {
	mock.Mock
}

func (e *extractorFactoryMock) New(client bqiface.Client) (UpstreamExtractor, error) {
	args := e.Called(client)

	r1, ok := args.Get(0).(UpstreamExtractor)
	if !ok {
		return nil, args.Error(1)
	}

	return r1, args.Error(1)
}

type extractorMock struct {
	mock.Mock
}

func (e *extractorMock) ExtractUpstreams(ctx context.Context, query string, resourcesToIgnore []upstream.Resource) ([]*upstream.Upstream, error) {
	args := e.Called(ctx, query, resourcesToIgnore)

	r1, ok := args.Get(0).([]*upstream.Upstream)
	if !ok {
		return nil, args.Error(1)
	}

	return r1, args.Error(1)
}

func TestBQ2BQ(t *testing.T) {
	ctx := context.Background()

	t.Run("GetName", func(t *testing.T) {
		t.Run("should return name bq2bq", func(t *testing.T) {
			b2b := BQ2BQ{}
			actualName, err := b2b.GetName(ctx)
			assert.NoError(t, err)
			assert.Equal(t, "bq2bq", actualName)
		})
	})

	t.Run("CompileAssets", func(t *testing.T) {
		t.Run("should not compile assets if load method is not replace", func(t *testing.T) {
			compileRequest := plugin.CompileAssetsRequest{
				Config: plugin.Configs{
					{
						Name:  "LOAD_METHOD",
						Value: "MERGE",
					},
				},
				Assets: plugin.Assets{
					{
						Name:  "query.sql",
						Value: `Select * from table where ts > "{{.DSTART}}"`,
					},
				},
				InstanceData: []plugin.JobRunSpecData{},
			}
			b2b := &BQ2BQ{}
			resp, err := b2b.CompileAssets(ctx, compileRequest)
			assert.Nil(t, err)
			assert.NotNil(t, resp)
			compAsset, _ := compileRequest.Assets.Get("query.sql")
			respAsset, _ := resp.Assets.Get("query.sql")
			assert.Equal(t, compAsset.Value, respAsset.Value)
		})
		t.Run("should not compile assets if load method is replace but window size is less than equal partition delta", func(t *testing.T) {
			startTime := time.Date(2022, 5, 1, 0, 0, 0, 0, time.UTC)
			endTime := time.Date(2022, 5, 2, 0, 0, 0, 0, time.UTC)
			compileRequest := plugin.CompileAssetsRequest{
				Options: plugin.Options{
					DryRun: false,
				},
				Config: plugin.Configs{
					{
						Name:  "LOAD_METHOD",
						Value: "REPLACE",
					},
				},
				Assets: plugin.Assets{
					{
						Name:  "query.sql",
						Value: `Select * from table where ts > "{{.DSTART}}"`,
					},
				},
				InstanceData: []plugin.JobRunSpecData{},
				StartTime:    startTime,
				EndTime:      endTime,
			}
			b2b := &BQ2BQ{
				Compiler: NewCompiler(),
			}
			resp, err := b2b.CompileAssets(ctx, compileRequest)
			assert.Nil(t, err)
			assert.NotNil(t, resp)
			compAsset, _ := compileRequest.Assets.Get("query.sql")
			respAsset, _ := resp.Assets.Get("query.sql")
			assert.Equal(t, compAsset.Value, respAsset.Value)
		})
		t.Run("should compile assets if load method is replace and break the query into multiple parts", func(t *testing.T) {
			startTime := time.Date(2021, 1, 10, 0, 0, 0, 0, time.UTC)
			endTime := time.Date(2021, 1, 17, 0, 0, 0, 0, time.UTC)
			compileRequest := plugin.CompileAssetsRequest{
				Options: plugin.Options{
					DryRun: false,
				},
				Config: plugin.Configs{
					{
						Name:  "LOAD_METHOD",
						Value: "REPLACE",
					},
				},
				Assets: plugin.Assets{
					{
						Name:  "query.sql",
						Value: `Select * from table where ts > "{{.DSTART}}"`,
					},
				},
				InstanceData: []plugin.JobRunSpecData{},
				StartTime:    startTime,
				EndTime:      endTime,
			}
			b2b := &BQ2BQ{
				Compiler: NewCompiler(),
			}
			resp, err := b2b.CompileAssets(ctx, compileRequest)
			assert.Nil(t, err)
			assert.NotNil(t, resp)
			compAsset := `Select * from table where ts > "2021-01-10T00:00:00Z"
--*--optimus-break-marker--*--
Select * from table where ts > "2021-01-11T00:00:00Z"
--*--optimus-break-marker--*--
Select * from table where ts > "2021-01-12T00:00:00Z"
--*--optimus-break-marker--*--
Select * from table where ts > "2021-01-13T00:00:00Z"
--*--optimus-break-marker--*--
Select * from table where ts > "2021-01-14T00:00:00Z"
--*--optimus-break-marker--*--
Select * from table where ts > "2021-01-15T00:00:00Z"
--*--optimus-break-marker--*--
Select * from table where ts > "2021-01-16T00:00:00Z"`
			respAsset, _ := resp.Assets.Get("query.sql")
			assert.Equal(t, compAsset, respAsset.Value)
		})

	})

	t.Run("GenerateDestination", func(t *testing.T) {
		t.Run("should properly generate a destination provided correct config inputs", func(t *testing.T) {
			b2b := &BQ2BQ{}
			dst, err := b2b.GenerateDestination(ctx, plugin.GenerateDestinationRequest{
				Config: plugin.Configs{
					{
						Name:  "PROJECT",
						Value: "proj",
					},
					{
						Name:  "DATASET",
						Value: "datas",
					},
					{
						Name:  "TABLE",
						Value: "tab",
					},
				},
			})
			assert.Nil(t, err)
			assert.Equal(t, "proj:datas.tab", dst.Destination)
			assert.Equal(t, "bigquery", dst.Type)
		})
		t.Run("should throw an error if any on of the config is missing to generate destination", func(t *testing.T) {
			b2b := &BQ2BQ{}
			_, err := b2b.GenerateDestination(ctx, plugin.GenerateDestinationRequest{
				Config: plugin.Configs{
					{
						Name:  "DATASET",
						Value: "datas",
					},
					{
						Name:  "TABLE",
						Value: "tab",
					},
				},
			})
			assert.NotNil(t, err)
		})
	})

	t.Run("GenerateDependencies", func(t *testing.T) {
		t.Run("should generate dependencies for select statements", func(t *testing.T) {
			expectedDeps := []string{"bigquery://proj:dataset.table1"}
			query := "Select * from proj.dataset.table1"
			data := plugin.GenerateDependenciesRequest{
				Assets: plugin.Assets{
					{
						Name:  QueryFileName,
						Value: query,
					},
				},
				Config: plugin.Configs{
					{
						Name:  "PROJECT",
						Value: "proj",
					},
					{
						Name:  "DATASET",
						Value: "datas",
					},
					{
						Name:  "TABLE",
						Value: "tab",
					},
					{
						Name:  BqServiceAccount,
						Value: "BQ_ACCOUNT_SECRET",
					},
				},
			}

			client := new(bqClientMock)

			bqClientFac := new(bqClientFactoryMock)
			bqClientFac.On("New", mock.Anything, "BQ_ACCOUNT_SECRET").Return(client, nil)
			defer bqClientFac.AssertExpectations(t)

			destination := upstream.Resource{
				Project: "proj",
				Dataset: "datas",
				Name:    "tab",
			}

			extractor := new(extractorMock)
			extractor.On("ExtractUpstreams", mock.Anything, query, []upstream.Resource{destination}).
				Return([]*upstream.Upstream{
					{
						Resource: upstream.Resource{
							Project: "proj",
							Dataset: "dataset",
							Name:    "table1",
						},
					},
				}, nil)

			extractorFac := new(extractorFactoryMock)
			extractorFac.On("New", client).Return(extractor, nil)

			b := &BQ2BQ{
				ClientFac:    bqClientFac,
				ExtractorFac: extractorFac,
			}
			got, err := b.GenerateDependencies(ctx, data)
			if err != nil {
				t.Errorf("error = %v", err)
				return
			}
			sort.Strings(got.Dependencies)
			if !reflect.DeepEqual(got.Dependencies, expectedDeps) {
				t.Errorf("got = %v, want %v", got, expectedDeps)
			}
		})

		t.Run("should generate unique dependencies for select statements", func(t *testing.T) {
			expectedDeps := []string{"bigquery://proj:dataset.table1"}
			query := "Select * from proj.dataset.table1 t1 join proj.dataset.table1 t2 on t1.col1 = t2.col1"
			data := plugin.GenerateDependenciesRequest{
				Assets: plugin.Assets{
					{
						Name:  QueryFileName,
						Value: query,
					},
				},
				Config: plugin.Configs{
					{
						Name:  "PROJECT",
						Value: "proj",
					},
					{
						Name:  "DATASET",
						Value: "datas",
					},
					{
						Name:  "TABLE",
						Value: "tab",
					},
					{
						Name:  BqServiceAccount,
						Value: "BQ_ACCOUNT_SECRET",
					},
				},
			}

			client := new(bqClientMock)

			bqClientFac := new(bqClientFactoryMock)
			bqClientFac.On("New", mock.Anything, "BQ_ACCOUNT_SECRET").Return(client, nil)
			defer bqClientFac.AssertExpectations(t)

			destination := upstream.Resource{
				Project: "proj",
				Dataset: "datas",
				Name:    "tab",
			}

			extractor := new(extractorMock)
			extractor.On("ExtractUpstreams", mock.Anything, query, []upstream.Resource{destination}).
				Return([]*upstream.Upstream{
					{
						Resource: upstream.Resource{
							Project: "proj",
							Dataset: "dataset",
							Name:    "table1",
						},
					},
				}, nil)

			extractorFac := new(extractorFactoryMock)
			extractorFac.On("New", client).Return(extractor, nil)

			b := &BQ2BQ{
				ClientFac:    bqClientFac,
				ExtractorFac: extractorFac,
			}
			got, err := b.GenerateDependencies(ctx, data)
			if err != nil {
				t.Errorf("error = %v", err)
				return
			}
			sort.Strings(got.Dependencies)
			if !reflect.DeepEqual(got.Dependencies, expectedDeps) {
				t.Errorf("got = %v, want %v", got, expectedDeps)
			}
		})

		t.Run("should generate dependencies for select statements but ignore if asked explicitly", func(t *testing.T) {
			var expectedDeps []string
			query := "Select * from /* @ignoreupstream */ proj.dataset.table1"
			data := plugin.GenerateDependenciesRequest{
				Assets: plugin.Assets{
					{
						Name:  QueryFileName,
						Value: query,
					},
				},
				Config: plugin.Configs{
					{
						Name:  "PROJECT",
						Value: "proj",
					},
					{
						Name:  "DATASET",
						Value: "datas",
					},
					{
						Name:  "TABLE",
						Value: "tab",
					},
					{
						Name:  BqServiceAccount,
						Value: "BQ_ACCOUNT_SECRET",
					},
				},
			}

			client := new(bqClientMock)

			bqClientFac := new(bqClientFactoryMock)
			bqClientFac.On("New", mock.Anything, "BQ_ACCOUNT_SECRET").Return(client, nil)
			defer bqClientFac.AssertExpectations(t)

			destination := upstream.Resource{
				Project: "proj",
				Dataset: "datas",
				Name:    "tab",
			}

			extractor := new(extractorMock)
			extractor.On("ExtractUpstreams", mock.Anything, query, []upstream.Resource{destination}).
				Return([]*upstream.Upstream{}, nil)

			extractorFac := new(extractorFactoryMock)
			extractorFac.On("New", client).Return(extractor, nil)

			b := &BQ2BQ{
				ClientFac:    bqClientFac,
				ExtractorFac: extractorFac,
			}
			got, err := b.GenerateDependencies(ctx, data)
			if err != nil {
				t.Errorf("error = %v", err)
				return
			}
			sort.Strings(got.Dependencies)
			if !reflect.DeepEqual(got.Dependencies, expectedDeps) {
				t.Errorf("got = %v, want %v", got, expectedDeps)
			}
		})

		t.Run("should generate dependencies for select statements but ignore if asked explicitly for view", func(t *testing.T) {
			expectedDeps := []string{"bigquery://proj:dataset.table1"}
			query := "Select * from proj.dataset.table1 t1 left join /* @ignoreupstream */ proj.dataset.view1 v1 on t1.date=v1.date"
			data := plugin.GenerateDependenciesRequest{
				Assets: plugin.Assets{
					{
						Name:  QueryFileName,
						Value: query,
					},
				},
				Config: plugin.Configs{
					{
						Name:  "PROJECT",
						Value: "proj",
					},
					{
						Name:  "DATASET",
						Value: "datas",
					},
					{
						Name:  "TABLE",
						Value: "tab",
					},
					{
						Name:  BqServiceAccount,
						Value: "BQ_ACCOUNT_SECRET",
					},
				},
			}

			client := new(bqClientMock)

			bqClientFac := new(bqClientFactoryMock)
			bqClientFac.On("New", mock.Anything, "BQ_ACCOUNT_SECRET").Return(client, nil)
			defer bqClientFac.AssertExpectations(t)

			destination := upstream.Resource{
				Project: "proj",
				Dataset: "datas",
				Name:    "tab",
			}

			extractor := new(extractorMock)
			extractor.On("ExtractUpstreams", mock.Anything, query, []upstream.Resource{destination}).
				Return([]*upstream.Upstream{
					{
						Resource: upstream.Resource{
							Project: "proj",
							Dataset: "dataset",
							Name:    "table1",
						},
					},
				}, nil)

			extractorFac := new(extractorFactoryMock)
			extractorFac.On("New", client).Return(extractor, nil)

			b := &BQ2BQ{
				ClientFac:    bqClientFac,
				ExtractorFac: extractorFac,
			}
			got, err := b.GenerateDependencies(ctx, data)
			if err != nil {
				t.Errorf("error = %v", err)
				return
			}
			sort.Strings(got.Dependencies)
			if !reflect.DeepEqual(got.Dependencies, expectedDeps) {
				t.Errorf("got = %v, want %v", got, expectedDeps)
			}
		})

		t.Run("should generate dependencies for select statements then reuse cache for the next time", func(t *testing.T) {
			expectedDeps := []string{"bigquery://proj:dataset.table1"}
			query := "Select * from proj.dataset.table1"
			data := plugin.GenerateDependenciesRequest{
				Assets: plugin.Assets{
					{
						Name:  QueryFileName,
						Value: query,
					},
				},
				Config: plugin.Configs{
					{
						Name:  "PROJECT",
						Value: "proj",
					},
					{
						Name:  "DATASET",
						Value: "datas",
					},
					{
						Name:  "TABLE",
						Value: "tab",
					},
					{
						Name:  BqServiceAccount,
						Value: "BQ_ACCOUNT_SECRET",
					},
				},
			}

			client := new(bqClientMock)

			bqClientFac := new(bqClientFactoryMock)
			bqClientFac.On("New", mock.Anything, "BQ_ACCOUNT_SECRET").Return(client, nil)
			defer bqClientFac.AssertExpectations(t)

			destination := upstream.Resource{
				Project: "proj",
				Dataset: "datas",
				Name:    "tab",
			}

			extractor := new(extractorMock)
			extractor.On("ExtractUpstreams", mock.Anything, query, []upstream.Resource{destination}).
				Return([]*upstream.Upstream{
					{
						Resource: upstream.Resource{
							Project: "proj",
							Dataset: "dataset",
							Name:    "table1",
						},
					},
				}, nil)

			extractorFac := new(extractorFactoryMock)
			extractorFac.On("New", client).Return(extractor, nil)

			b := &BQ2BQ{
				ClientFac:    bqClientFac,
				ExtractorFac: extractorFac,
				C:            cache.New(CacheTTL, CacheCleanUp),
			}
			got, err := b.GenerateDependencies(ctx, data)
			if err != nil {
				t.Errorf("error = %v", err)
				return
			}
			sort.Strings(got.Dependencies)
			if !reflect.DeepEqual(got.Dependencies, expectedDeps) {
				t.Errorf("got = %v, want %v", got, expectedDeps)
			}

			// should be cached
			got, err = b.GenerateDependencies(ctx, data)
			if err != nil {
				t.Errorf("error = %v", err)
				return
			}
			if !reflect.DeepEqual(got.Dependencies, expectedDeps) {
				t.Errorf("got = %v, want %v", got, expectedDeps)
			}
		})
	})
}
