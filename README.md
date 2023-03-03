# Transformers

[![test workflow](https://github.com/goto/transformers/actions/workflows/test.yml/badge.svg)](test)
[![build workflow](https://github.com/goto/transformers/actions/workflows/build.yml/badge.svg)](build)

Optimus's transformation plugins are implementations of Task and Hook interfaces that allows
execution of arbitrary jobs in optimus.

## To install plugins via homebrew
```shell
brew tap goto/taps
brew install optimus-plugins-goto
```

## To install plugins via shell

```shell
curl -sL ${PLUGIN_RELEASE_URL} | tar xvz
chmod +x optimus-*
mv optimus-* /usr/bin/
```