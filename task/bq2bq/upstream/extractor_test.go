package upstream_test

import (
	"context"
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/googleapis/google-cloud-go-testing/bigquery/bqiface"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/api/iterator"

	"github.com/goto/transformers/task/bq2bq/upstream"
)

func TestNewExtractor(t *testing.T) {
	t.Run("should return nil and error if client is nil", func(t *testing.T) {
		var client bqiface.Client

		actualExtractor, actualError := upstream.NewExtractor(client)

		assert.Nil(t, actualExtractor)
		assert.EqualError(t, actualError, "client is nil")
	})

	t.Run("should return extractor and nil if no error is encountered", func(t *testing.T) {
		client := new(ClientMock)

		actualExtractor, actualError := upstream.NewExtractor(client)

		assert.NotNil(t, actualExtractor)
		assert.NoError(t, actualError)
	})
}

func TestExtractor(t *testing.T) {
	t.Run("ExtractUpstreams", func(t *testing.T) {
		t.Run("should pass the existing spec", func(t *testing.T) {
			testCases := []struct {
				Message           string
				QueryRequest      string
				ExpectedUpstreams []*upstream.Upstream
			}{
				{
					Message:      "should return upstreams and generate dependencies for select statements",
					QueryRequest: "Select * from proj.dataset.table1",
					ExpectedUpstreams: []*upstream.Upstream{
						{
							Resource: upstream.Resource{
								Project: "proj",
								Dataset: "dataset",
								Name:    "table1",
							},
						},
					},
				},
				{
					Message:      "should return unique upstreams and nil for select statements",
					QueryRequest: "Select * from proj.dataset.table1 t1 join proj.dataset.table1 t2 on t1.col1 = t2.col1",
					ExpectedUpstreams: []*upstream.Upstream{
						{
							Resource: upstream.Resource{
								Project: "proj",
								Dataset: "dataset",
								Name:    "table1",
							},
						},
					},
				},
				{
					Message:           "should return filtered upstreams for select statements with ignore statement",
					QueryRequest:      "Select * from /* @ignoreupstream */ proj.dataset.table1",
					ExpectedUpstreams: []*upstream.Upstream{},
				},
				{
					Message:      "should return filtered upstreams for select statements with ignore statement for view",
					QueryRequest: "Select * from proj.dataset.table1 t1 join proj.dataset.table1 t2 on t1.col1 = t2.col1",
					ExpectedUpstreams: []*upstream.Upstream{
						{
							Resource: upstream.Resource{
								Project: "proj",
								Dataset: "dataset",
								Name:    "table1",
							},
						},
					},
				},
			}

			for _, test := range testCases {
				client := new(ClientMock)
				query := new(QueryMock)
				rowIterator := new(RowIteratorMock)
				resourcestoIgnore := []upstream.Resource{
					{
						Project: "proj",
						Dataset: "datas",
						Name:    "tab",
					},
				}

				extractor, err := upstream.NewExtractor(client)
				assert.NotNil(t, extractor)
				assert.NoError(t, err)

				ctx := context.Background()

				client.On("Query", mock.Anything).Return(query)

				query.On("Read", mock.Anything).Return(rowIterator, nil)

				rowIterator.On("Next", mock.Anything).Run(func(args mock.Arguments) {
					v := args.Get(0).(*[]bigquery.Value)
					*v = []bigquery.Value{"proj", "dataset", "table1", "BASE TABLE", "select 1;"}
				}).Return(nil).Once()
				rowIterator.On("Next", mock.Anything).Return(iterator.Done).Once()

				actualUpstreams, actualError := extractor.ExtractUpstreams(ctx, test.QueryRequest, resourcestoIgnore)

				assert.EqualValues(t, test.ExpectedUpstreams, actualUpstreams, test.Message)
				assert.NoError(t, actualError, test.Message)
			}
		})

		t.Run("should return upstreams with its nested ones if found any", func(t *testing.T) {
			client := new(ClientMock)
			query := new(QueryMock)
			rowIterator := new(RowIteratorMock)
			resourcestoIgnore := []upstream.Resource{
				{
					Project: "project_test_0",
					Dataset: "dataset_test_0",
					Name:    "name_test_0",
				},
			}

			extractor, err := upstream.NewExtractor(client)
			assert.NotNil(t, extractor)
			assert.NoError(t, err)

			ctx := context.Background()
			queryRequest := "select * from `project_test_1.dataset_test_1.name_test_1`"

			client.On("Query", mock.Anything).Return(query)

			query.On("Read", mock.Anything).Return(rowIterator, nil)

			rowIterator.On("Next", mock.Anything).Run(func(args mock.Arguments) {
				v := args.Get(0).(*[]bigquery.Value)
				*v = []bigquery.Value{"project_test_1", "dataset_test_1", "name_test_1", "BASE TABLE", "select 1;"}
			}).Return(nil).Once()
			rowIterator.On("Next", mock.Anything).Run(func(args mock.Arguments) {
				v := args.Get(0).(*[]bigquery.Value)
				*v = []bigquery.Value{"project_test_2", "dataset_test_2", "name_test_2", "VIEW", "select * from project_test_3.dataset_test_3.name_test_3;"}
			}).Return(nil).Once()
			rowIterator.On("Next", mock.Anything).Return(iterator.Done).Once()
			rowIterator.On("Next", mock.Anything).Run(func(args mock.Arguments) {
				v := args.Get(0).(*[]bigquery.Value)
				*v = []bigquery.Value{"project_test_3", "dataset_test_3", "name_test_3", "BASE TABLE", "select 1"}
			}).Return(nil).Once()
			rowIterator.On("Next", mock.Anything).Return(iterator.Done).Once()

			expectedUpstreams := []*upstream.Upstream{
				{
					Resource: upstream.Resource{
						Project: "project_test_1",
						Dataset: "dataset_test_1",
						Name:    "name_test_1",
					},
				},
				{
					Resource: upstream.Resource{
						Project: "project_test_2",
						Dataset: "dataset_test_2",
						Name:    "name_test_2",
					},
					Upstreams: []*upstream.Upstream{
						{
							Resource: upstream.Resource{
								Project: "project_test_3",
								Dataset: "dataset_test_3",
								Name:    "name_test_3",
							},
						},
					},
				},
			}

			actualUpstreams, actualError := extractor.ExtractUpstreams(ctx, queryRequest, resourcestoIgnore)

			assert.EqualValues(t, expectedUpstreams, actualUpstreams)
			assert.NoError(t, actualError)
		})
	})
}
