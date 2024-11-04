# Contribution guide
Thanks for wanting to help out!

Please follow the [code of conduct](./CODE_OF_CONDUCT.md).

## Development environment set-up
```shell
pip install -e .
```

## Testing
Run the example from the README and make sure the output looks correct.

## Building documentation
```shell
make -C docs
```

View with a static file server, eg (hosting at http://127.0.0.1:8042/):

```shell
python3 -m http.server -d docs/build/html/ -b 127.0.0.1 8042
```

## Building package
```shell
pip install build
pyproject-build
```

## Submitting changes
Make sure the above test is successful, then make a pull-request on GitHub.
