package upstream

import (
	"regexp"
	"strings"
)

type QueryParser func(query string) []Resource

var (
	topLevelUpstreamsPattern = regexp.MustCompile("" +
		"(?i)(?:FROM)\\s*(?:/\\*\\s*([a-zA-Z0-9@_-]*)\\s*\\*/)?\\s+`?([\\w-]+)\\.([\\w-]+)\\.([\\w-]+)`?" +
		"|" +
		"(?i)(?:JOIN)\\s*(?:/\\*\\s*([a-zA-Z0-9@_-]*)\\s*\\*/)?\\s+`?([\\w-]+)\\.([\\w-]+)\\.([\\w-]+)`?" +
		"|" +
		"(?i)(?:WITH)\\s*(?:/\\*\\s*([a-zA-Z0-9@_-]*)\\s*\\*/)?\\s+`?([\\w-]+)\\.([\\w-]+)\\.([\\w-]+)`?\\s+(?:AS)")

	queryCommentPatterns = regexp.MustCompile("(--.*)|(((/\\*)+?[\\w\\W]*?(\\*/)+))")
	helperPattern        = regexp.MustCompile("(\\/\\*\\s*(@[a-zA-Z0-9_-]+)\\s*\\*\\/)")
)

func ParseTopLevelUpstreamsFromQuery(query string) []Resource {
	cleanedQuery := cleanQueryFromComment(query)

	resourcesFound := make(map[Resource]bool)
	pseudoResources := make(map[Resource]bool)

	matches := topLevelUpstreamsPattern.FindAllStringSubmatch(cleanedQuery, -1)

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

		project := match[projectIdx]
		dataset := match[datasetIdx]
		name := match[nameIdx]

		if project == "" || dataset == "" || name == "" {
			continue
		}

		if strings.TrimSpace(match[ignoreUpstreamIdx]) == "@ignoreupstream" {
			continue
		}

		resource := Resource{
			Project: project,
			Dataset: dataset,
			Name:    name,
		}

		if clause == "with" {
			pseudoResources[resource] = true
		} else {
			resourcesFound[resource] = true
		}
	}

	var output []Resource

	for resource := range resourcesFound {
		if pseudoResources[resource] {
			continue
		}
		output = append(output, resource)
	}

	return output
}

func cleanQueryFromComment(query string) string {
	matches := queryCommentPatterns.FindAllStringSubmatch(query, -1)
	for _, match := range matches {
		helperToken := match[2]

		if helperPattern.MatchString(helperToken) {
			continue
		}

		query = strings.ReplaceAll(query, match[0], " ")
	}

	return query
}

// TODO: check if ddl requires custom query, if not then we can remove this function
func ParseNestedUpsreamsFromDDL(ddl string) []Resource {
	return ParseTopLevelUpstreamsFromQuery(ddl)
}
