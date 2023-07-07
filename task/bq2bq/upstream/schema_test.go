package upstream_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/googleapis/google-cloud-go-testing/bigquery/bqiface"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/api/iterator"

	"github.com/goto/transformers/task/bq2bq/upstream"
)

func TestReadSchemasUnderGroup(t *testing.T) {
	t.Run("should return nil and error if failed reading from query", func(t *testing.T) {
		client := new(ClientMock)
		queryStatement := new(QueryMock)

		ctx := context.Background()
		group := &upstream.ResourceGroup{
			Project: "project_test",
			Dataset: "dataset_test",
			Names:   []string{"table_test"},
		}

		queryContent := buildQuery(group)
		client.On("Query", queryContent).Return(queryStatement)

		unexpectedError := errors.New("unexpected error")
		queryStatement.On("Read", ctx).Return(nil, unexpectedError)

		actualSchemas, actualError := upstream.ReadSchemasUnderGroup(ctx, client, group)

		assert.Nil(t, actualSchemas)
		assert.ErrorContains(t, actualError, unexpectedError.Error())
	})

	t.Run("should return nil and error if failed getting next value iterator", func(t *testing.T) {
		client := new(ClientMock)
		queryStatement := new(QueryMock)
		rowIterator := new(RowIteratorMock)

		ctx := context.Background()
		group := &upstream.ResourceGroup{
			Project: "project_test",
			Dataset: "dataset_test",
			Names:   []string{"table_test"},
		}

		queryContent := buildQuery(group)
		client.On("Query", queryContent).Return(queryStatement)

		queryStatement.On("Read", ctx).Return(rowIterator, nil)

		unexpectedError := errors.New("unexpected error")
		rowIterator.On("Next", mock.Anything).Return(unexpectedError)

		actualSchemas, actualError := upstream.ReadSchemasUnderGroup(ctx, client, group)

		assert.Nil(t, actualSchemas)
		assert.ErrorContains(t, actualError, unexpectedError.Error())
	})

	t.Run("should return nil and nil if row iterator results in zero value", func(t *testing.T) {
		client := new(ClientMock)
		queryStatement := new(QueryMock)
		rowIterator := new(RowIteratorMock)

		ctx := context.Background()
		group := &upstream.ResourceGroup{
			Project: "project_test",
			Dataset: "dataset_test",
			Names:   []string{"table_test"},
		}

		queryContent := buildQuery(group)
		client.On("Query", queryContent).Return(queryStatement)

		queryStatement.On("Read", ctx).Return(rowIterator, nil)

		rowIterator.On("Next", mock.Anything).Run(func(args mock.Arguments) {
			v := args.Get(0).(*[]bigquery.Value)
			*v = []bigquery.Value{}
		}).Return(nil).Once()
		rowIterator.On("Next", mock.Anything).Return(iterator.Done).Once()

		actualSchemas, actualError := upstream.ReadSchemasUnderGroup(ctx, client, group)

		assert.Nil(t, actualSchemas)
		assert.NoError(t, actualError)
	})

	t.Run("should return nil and error if row iterator cannot be converted to schema", func(t *testing.T) {
		testCases := []struct {
			IteratorValues []bigquery.Value
			ErrorMessage   string
		}{
			{
				IteratorValues: []bigquery.Value{"project"},
				ErrorMessage:   "unexpected number of row length",
			},
			{
				IteratorValues: []bigquery.Value{0, "dataset_test", "name_test", "view", "select 1;"},
				ErrorMessage:   "error casting project",
			},
			{
				IteratorValues: []bigquery.Value{"project_test", 0, "name_test", "view", "select 1;"},
				ErrorMessage:   "error casting dataset",
			},
			{
				IteratorValues: []bigquery.Value{"project_test", "dataset_test", 0, "view", "select 1;"},
				ErrorMessage:   "error casting name",
			},
			{
				IteratorValues: []bigquery.Value{"project_test", "dataset_test", "name_test", 0, "select 1;"},
				ErrorMessage:   "error casting _type",
			},
			{
				IteratorValues: []bigquery.Value{"project_test", "dataset_test", "name_test", "view", 0},
				ErrorMessage:   "error casting ddl",
			},
		}

		ctx := context.Background()
		group := &upstream.ResourceGroup{
			Project: "project_test",
			Dataset: "dataset_test",
			Names:   []string{"table_test"},
		}

		queryContent := buildQuery(group)

		for _, test := range testCases {
			client := new(ClientMock)
			queryStatement := new(QueryMock)
			rowIterator := new(RowIteratorMock)

			client.On("Query", queryContent).Return(queryStatement)

			queryStatement.On("Read", ctx).Return(rowIterator, nil)

			rowIterator.On("Next", mock.Anything).Run(func(args mock.Arguments) {
				v := args.Get(0).(*[]bigquery.Value)
				*v = test.IteratorValues
			}).Return(nil).Once()
			rowIterator.On("Next", mock.Anything).Return(iterator.Done).Once()

			actualSchemas, actualError := upstream.ReadSchemasUnderGroup(ctx, client, group)

			assert.Nil(t, actualSchemas)
			assert.ErrorContains(t, actualError, test.ErrorMessage)
		}
	})

	t.Run("should return schemas and nil if no error is encountered", func(t *testing.T) {
		client := new(ClientMock)
		queryStatement := new(QueryMock)
		rowIterator := new(RowIteratorMock)

		ctx := context.Background()
		group := &upstream.ResourceGroup{
			Project: "project_test",
			Dataset: "dataset_test",
			Names:   []string{"table_test"},
		}

		queryContent := buildQuery(group)
		client.On("Query", queryContent).Return(queryStatement)

		queryStatement.On("Read", ctx).Return(rowIterator, nil)

		rowIterator.On("Next", mock.Anything).Run(func(args mock.Arguments) {
			v := args.Get(0).(*[]bigquery.Value)
			*v = []bigquery.Value{"project_test", "dataset_test", "name_test", "VIEW", "select 1;"}
		}).Return(nil).Once()
		rowIterator.On("Next", mock.Anything).Return(iterator.Done).Once()

		expectedSchemas := []*upstream.Schema{
			{
				Resource: upstream.Resource{
					Project: "project_test",
					Dataset: "dataset_test",
					Name:    "name_test",
				},
				Type: upstream.View,
				DDL:  "select 1;",
			},
		}

		actualSchemas, actualError := upstream.ReadSchemasUnderGroup(ctx, client, group)

		assert.Equal(t, expectedSchemas, actualSchemas)
		assert.NoError(t, actualError)
	})
}

func buildQuery(group *upstream.ResourceGroup) string {
	modifiedNames := make([]string, len(group.Names))
	for i, n := range group.Names {
		modifiedNames[i] = "'" + n + "'"
	}

	return "SELECT table_catalog, table_schema, table_name, table_type, ddl\n" +
		fmt.Sprintf("FROM `%s.%s.INFORMATION_SCHEMA.TABLES`\n", group.Project, group.Dataset) +
		fmt.Sprintf("WHERE table_name in (%s);", strings.Join(modifiedNames, ", "))
}

type ClientMock struct {
	mock.Mock
	bqiface.Client
}

func (*ClientMock) Location() string {
	panic("unimplemented")
}

func (*ClientMock) SetLocation(string) {
	panic("unimplemented")
}

func (*ClientMock) Close() error {
	panic("unimplemented")
}

func (*ClientMock) Dataset(string) bqiface.Dataset {
	panic("unimplemented")
}

func (*ClientMock) DatasetInProject(string, string) bqiface.Dataset {
	panic("unimplemented")
}

func (*ClientMock) Datasets(context.Context) bqiface.DatasetIterator {
	panic("unimplemented")
}

func (*ClientMock) DatasetsInProject(context.Context, string) bqiface.DatasetIterator {
	panic("unimplemented")
}

func (c *ClientMock) Query(q string) bqiface.Query {
	return c.Called(q).Get(0).(bqiface.Query)
}

func (*ClientMock) JobFromID(context.Context, string) (bqiface.Job, error) {
	panic("unimplemented")
}

func (*ClientMock) JobFromIDLocation(context.Context, string, string) (bqiface.Job, error) {
	panic("unimplemented")
}

func (*ClientMock) Jobs(context.Context) bqiface.JobIterator {
	panic("unimplemented")
}

func (*ClientMock) embedToIncludeNewMethods() {
	panic("not implemented")
}

type QueryMock struct {
	mock.Mock
	bqiface.Query
}

func (*QueryMock) JobIDConfig() *bigquery.JobIDConfig {
	panic("unimplemented")
}

func (*QueryMock) SetQueryConfig(bqiface.QueryConfig) {
	panic("unimplemented")
}

func (*QueryMock) Run(context.Context) (bqiface.Job, error) {
	panic("unimplemented")
}

func (q *QueryMock) Read(ctx context.Context) (bqiface.RowIterator, error) {
	args := q.Called(ctx)

	var ret0 bqiface.RowIterator
	if args[0] != nil {
		ret0 = args[0].(bqiface.RowIterator)
	}

	return ret0, args.Error(1)
}

func (*QueryMock) embedToIncludeNewMethods() {
	panic("unimplemented")
}

type RowIteratorMock struct {
	mock.Mock
	bqiface.RowIterator
}

func (*RowIteratorMock) SetStartIndex(uint64) {
	panic("unimplemented")
}

func (*RowIteratorMock) Schema() bigquery.Schema {
	panic("unimplemented")
}

func (*RowIteratorMock) TotalRows() uint64 {
	panic("unimplemented")
}

func (r *RowIteratorMock) Next(v interface{}) error {
	return r.Called(v).Error(0)
}

func (*RowIteratorMock) PageInfo() *iterator.PageInfo {
	panic("unimplemented")
}

func (*RowIteratorMock) embedToIncludeNewMethods() {
	panic("unimplemented")
}
