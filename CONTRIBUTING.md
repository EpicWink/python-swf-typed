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

## Building package
```shell
pip install build
pyproject-build
```

## Submitting changes
Make sure the above test is successful, then make a pull-request on GitHub.
