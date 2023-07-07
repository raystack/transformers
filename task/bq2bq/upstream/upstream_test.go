package upstream_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/goto/transformers/task/bq2bq/upstream"
)

func TestFlattenUpstreams(t *testing.T) {
	t.Run("should return flattened upstream in the form of resource", func(t *testing.T) {
		upstreams := []*upstream.Upstream{
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
					{
						Resource: upstream.Resource{
							Project: "project_test_4",
							Dataset: "dataset_test_4",
							Name:    "name_test_4",
						},
					},
				},
			},
		}

		expectedResources := []upstream.Resource{
			{
				Project: "project_test_1",
				Dataset: "dataset_test_1",
				Name:    "name_test_1",
			},
			{
				Project: "project_test_2",
				Dataset: "dataset_test_2",
				Name:    "name_test_2",
			},
			{
				Project: "project_test_3",
				Dataset: "dataset_test_3",
				Name:    "name_test_3",
			},
			{
				Project: "project_test_4",
				Dataset: "dataset_test_4",
				Name:    "name_test_4",
			},
		}

		actualResources := upstream.FlattenUpstreams(upstreams)

		assert.Equal(t, expectedResources, actualResources)
	})
}
