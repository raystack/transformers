package main

import (
	"bytes"
	"fmt"
	"strings"
	"text/template"
	"time"
)

const (
	ISOTimeFormate = time.RFC3339
	ISODateLayout  = "2006-01-02"
)

type Compiler struct {
	baseTemplate *template.Template
}

func NewCompiler() *Compiler {
	baseTemplate := template.
		New("bq2bq_template_compiler").
		Funcs(map[string]any{
			"Date": dateFn,
		})

	return &Compiler{
		baseTemplate: baseTemplate,
	}
}

func (c *Compiler) Compile(templateMap map[string]string, context map[string]any) (map[string]string, error) {
	rendered := map[string]string{}

	for name, content := range templateMap {
		tmpl, err := c.baseTemplate.New(name).Parse(content)
		if err != nil {
			return nil, fmt.Errorf("unable to parse template content: %w", err)
		}

		var buf bytes.Buffer
		err = tmpl.Execute(&buf, context)
		if err != nil {
			return nil, fmt.Errorf("unable to render template: %w", err)
		}
		rendered[name] = strings.TrimSpace(buf.String())
	}
	return rendered, nil
}

func dateFn(timeStr string) (string, error) {
	t, err := time.Parse(ISOTimeFormate, timeStr)
	if err != nil {
		return "", err
	}
	return t.Format(ISODateLayout), nil
}
