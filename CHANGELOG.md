# Changelog

`swf_typed` release notes. `swf_typed` follows
[semantic versioning](https://semver.org/).

## Unreleased

### Features

* Workflow and activity type deletion (type must be deprecated)
* Execution state workflow type reference

### Fixes

* Use correct workflow type undeprecation SDK method
* Use local concurrency executor in decision poll result history page iteration, fixing
  `AttributeError`
* Execution open-counts are (mostly) not defaulted
  * Lambda task count is optional
* Execution workflow ID and workflow type filter models are now concrete, not abstract

### Miscellaneous

* Move Python project metadata and configuration to [pyproject.toml](./pyproject.toml)
* Add Trove classifiers to Python project
* Require `setuptools` <81, to ensure legacy `license` metadata support

## 1.1.2 - 2024-06-11

### Improvements

* Iteration methods now don't share a single thread for API calls

## 1.1.1 - 2022-10-26

### Fixes

* Support execution config with no task priority
* Fix args to count-tasks methods

## 1.1 - 2022-09-07

### Features

* Add method to get last history event swf_typed.get_last_execution_history_event

### Miscellaneous

* Improve online documentation

## 1.0.1 - 2022-02-02

### Fixes

* Fix `WorkflowExecutionCancelRequested` and `WorkflowExecutionSignaled` events
  deserialisation when the event has no `externalWorkflowExecution` attribute (ie when
  not from external execution)

## 1.0 - 2022-01-05

Initial stable public release

## Features

* Type annotations
* Explicit exceptions
* Execution state construction
* Consistent method/attribute/parameter names
* Consistent model structure
* Automatic flattening of paged-list responses
  * next-page calls are run concurrently and on-demand
* Better execution filtering
