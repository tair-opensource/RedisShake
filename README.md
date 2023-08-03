# pybbt: Python Black Box Testing Tool

pybbt is a Python-based tool that simplifies black box testing of software. It provides a range of auxiliary functions and the ability to filter test cases, making your testing
process more efficient and manageable.

## Projects Tested with pybbt

- [Tair](https://www.alibabacloud.com/product/tair): Tair is a Redis-compatible in-memory database service that provides a variety of data structures and enterprise-level
  capabilities, such as Global Distributed Cache, data flashback, and Transparent Data Encryption (TDE).
- [RedisShake](https://github.com/tair-opensource/RedisShake): RedisShake is a tool for data migration between Redis instances.

## Features

- **Test Case Execution:** Run individual or multiple test cases in a directory or subdirectories.
- **Test Case Filtering:** Filter test cases using tags. This allows for more targeted testing based on specific conditions.
- **Flag Passing:** Pass flags to the test cases for more flexible testing.
- **Error Management:** Choose to continue running all cases even if some fail.
- **Verbose Logging:** Toggle verbose logging to see all logs for detailed debugging.
- **Parallel Execution:** Run test cases in parallel to save time.

## Installation (coming soon)

pybbt requires Python 3.9 or later. You can install pybbt using pip:

```shell
pip install pybbt
```

Run the following command to test the installation:

```shell
pybbt -h
```

## Usage

Here are some examples of how to use pybbt:

```text
usage: pybbt [-h] [--filter FILTER] [--flags FLAGS [FLAGS ...]] [--parallel PARALLEL] [--start-from START_FROM] [--dont-stop] [--verbose] file_or_dir

Python Black Box Test

positional arguments:
  file_or_dir           test file path or directory path of test cases

optional arguments:
  -h, --help            show this help message and exit
  --filter FILTER       filter the test cases
  --flags FLAGS [FLAGS ...]
                        flags that can be obtained through pybbt.get_flags() in the code
  --parallel PARALLEL   run cases in parallel
  --start-from START_FROM
                        start from the case, example: --start-from cases/test/test0.py
  --dont-stop           wont stop when error occurs
  --verbose             show all the log

Example:
# simple usage
pybbt cases/test/test0.py   # run test0.py
pybbt cases/test/           # run all the test cases in test/
pybbt cases/                # run all the test cases in cases/ and sub directories
# filter
pybbt cases/ --filter tag0                                      # run all the test cases with tag0
pybbt cases/ --filter "not tag0"                                # run all the test cases without tag0
pybbt cases/ --filter "tag0 and tag1"                           # run all the test cases with tag0 and tag1
pybbt cases/ --filter "tag0 or tag1"                            # run cases that contain tag0 or tag1
pybbt cases/ --filter "not (tag0 or tag1)"                      # equivalent to "not tag0 and not tag1"
pybbt cases/ --filter "not (tag0 and tag1)"                     # equivalent to "not tag0 or not tag1"
pybbt cases/ --filter "(tag0 or tag1) and not (tag2 or tag3)"   # equivalent to "(tag0 or tag1) and not tag2 and not tag3"
# other options
pybbt cases/ --flags flag0 flag1    # pass flags0 and flags1 to the test cases
pybbt cases/ --dont-stop            # run all the cases even if some of them fail
pybbt cases/ --verbose              # show all the log
```

Feel free to experiment with different combinations of options to suit your testing needs.

## Contributing

Your contributions are highly appreciated! If you have ideas on how to improve pybbt or want to help enhance its features, please feel free to submit a Pull Request.

To test your modifications, execute the following commands in the project's root directory:

```shell
python -m build  # You may need to install 'build' first: pip install build
pip install --force-reinstall dist/pybbt-x.y.z-py3-none-any.whl  # Replace 'dist/pybbt-x.y.z-py3-none-any.whl' with the correct file name
```