package main

import (
	"fmt"
	"reflect"
	"regexp"

	"github.com/AlecAivazis/survey/v2"
)

// validatorFactory, name abbreviated so that
// the global implementation can be called 'validatorFactory'
type vFactory struct{}

func (f *vFactory) NewFromRegex(re, message string) survey.Validator {
	var regex = regexp.MustCompile(re)
	return func(v interface{}) error {
		k := reflect.ValueOf(v).Kind()
		if k != reflect.String {
			return fmt.Errorf("was expecting a string, got %s", k.String())
		}
		val := v.(string)
		if !regex.Match([]byte(val)) {
			return fmt.Errorf(message)
		}
		return nil
	}
}

var ValidatorFactory = new(vFactory)
