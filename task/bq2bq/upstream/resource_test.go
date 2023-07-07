package upstream_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/goto/transformers/task/bq2bq/upstream"
)

func TestFilterResources(t *testing.T) {
	input := []upstream.Resource{
		{
			Project: "project_test",
			Dataset: "dataset_test",
			Name:    "name_1_test",
		},
		{
			Project: "project_test",
			Dataset: "dataset_test",
			Name:    "name_2_test",
		},
		{
			Project: "project_test",
			Dataset: "dataset_test",
			Name:    "name_3_test",
		},
	}
	excludeIfContains3 := func(r upstream.Resource) bool {
		return strings.Contains(r.Name, "3")
	}

	expectedResult := input[:2]

	actualResult := upstream.FilterResources(input, excludeIfContains3)

	assert.Equal(t, expectedResult, actualResult)
}

func TestUniqueFilterResources(t *testing.T) {
	input := []upstream.Resource{
		{
			Project: "project_test",
			Dataset: "dataset_1_test",
			Name:    "name_1_test",
		},
		{
			Project: "project_test",
			Dataset: "dataset_1_test",
			Name:    "name_2_test",
		},
		{
			Project: "project_test",
			Dataset: "dataset_1_test",
			Name:    "name_1_test",
		},
		{
			Project: "project_test",
			Dataset: "dataset_1_test",
			Name:    "name_2_test",
		},
	}

	expectedResult := input[:2]

	actualResult := upstream.UniqueFilterResources(input)

	assert.Equal(t, expectedResult, actualResult)
}

func TestGroupResources(t *testing.T) {
	input := []upstream.Resource{
		{
			Project: "project_test",
			Dataset: "dataset_1_test",
			Name:    "name_1_test",
		},
		{
			Project: "project_test",
			Dataset: "dataset_1_test",
			Name:    "name_2_test",
		},
		{
			Project: "project_test",
			Dataset: "dataset_2_test",
			Name:    "name_1_test",
		},
		{
			Project: "project_test",
			Dataset: "dataset_2_test",
			Name:    "name_2_test",
		},
	}

	expectedResult := []*upstream.ResourceGroup{
		{
			Project: "project_test",
			Dataset: "dataset_1_test",
			Names:   []string{"name_1_test", "name_2_test"},
		},
		{
			Project: "project_test",
			Dataset: "dataset_2_test",
			Names:   []string{"name_1_test", "name_2_test"},
		},
	}

	actualResult := upstream.GroupResources(input)

	assert.Equal(t, expectedResult, actualResult)
}
