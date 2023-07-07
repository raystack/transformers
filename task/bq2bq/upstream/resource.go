package upstream

type Resource struct {
	Project string
	Dataset string
	Name    string
}

func (r Resource) URN() string {
	return r.Project + "." + r.Dataset + "." + r.Name
}

type ResourceGroup struct {
	Project string
	Dataset string
	Names   []string
}

func (r ResourceGroup) URN() string {
	return r.Project + "." + r.Dataset
}

func FilterResources(input []Resource, isToExcludeFn func(Resource) bool) []Resource {
	var output []Resource

	for _, r := range input {
		if !isToExcludeFn(r) {
			output = append(output, r)
		}
	}

	return output
}

func UniqueFilterResources(input []Resource) []Resource {
	ref := make(map[string]Resource)
	for _, i := range input {
		key := i.URN()
		ref[key] = i
	}

	var output []Resource
	for _, r := range ref {
		output = append(output, r)
	}

	return output
}

func GroupResources(infos []Resource) []*ResourceGroup {
	ref := make(map[string]*ResourceGroup)

	for _, info := range infos {
		key := info.Project + "." + info.Dataset

		if _, ok := ref[key]; ok {
			ref[key].Names = append(ref[key].Names, info.Name)
		} else {
			ref[key] = &ResourceGroup{
				Project: info.Project,
				Dataset: info.Dataset,
				Names:   []string{info.Name},
			}
		}
	}

	var output []*ResourceGroup
	for _, r := range ref {
		output = append(output, r)
	}

	return output
}
