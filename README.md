[![GoDoc](https://godoc.org/github.com/jyu617/piper?status.svg)](https://godoc.org/github.com/jyu617/piper)
[![Circle CI](https://circleci.com/gh/jyu617/piper/tree/master.svg?style=svg&circle-token=db7ecbf336144a81e1c77ae8d9c91d820e9234c5)](https://circleci.com/gh/jyu617/piper/tree/master)
[![Coverage Status](https://coveralls.io/repos/github/jyu617/piper/badge.svg?branch=master)](https://coveralls.io/github/jyu617/piper?branch=master)

# piper

Piper is a customizable data pipeline processing library written in Go.

Piper abstracts the components of a data pipeline into chained processes, which run concurrent batch jobs to execute user-supplied callback functions.  Each successful job within a batch moves through the pipeline to the next process until completion.  Each failed job within a batch gets retried up to the maximum number of retry attempts.  Failed jobs that have exceed the maximum number of retry attempts are passed to a user-supplied callback function, which could be sent to a dead-letter queue for instance.

## Getting Started 

### Installation

Install the piper library with the following command:

```
go install github.com/jyu617/piper
```

Then import the piper package in your application as such:

```
import "github.com/jyu617/piper"
```

### Usage

To create a batch process, do the following:
 1) define a data structure (containing the fields pertinent to the data pipeline) which implements the `piper.DataIF` interface 
 2) define a batch function which implements the `piper.BatchExecutable` interface
 3) use the `piper.NewProcess` API to create a new batch process
 4) initialize the process using the `process.Start` function
 5) enqueue data for processing by using the `process.ProcessData` function

To create a pipeline (two or more processes chained together), do the following:
 1) follow steps 1-3 above to define a new process for each process in the pipeline
 2) use the `piper.NewPipeline` API to create a new pipeline
 3) initialize the process using the `process.Start()`function
 4) enqueue data for processing by using the `process.ProcessData` function

## Tests

There are no dependencies required to run any of the unit tests.  

To run the tests from the command line:

```sh
go test -v -cover -race github.com/jyu617/piper
```

## Contributing

Pull requests for bug fixes, new features or performance enhancements are welcome!

## Versioning

This project follows the [semantic versioning guidelines](http://semver.org/) for versioning.  For the versions available, see the [tags on this repository](https://github.com/jyu617/tags). 

## Authors

* Jason Yu

See also the list of [contributors](https://github.com/jyu617/contributors) who participated in this project.

## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details
