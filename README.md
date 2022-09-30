# target-s3-jsonl

[![GitHub - License](https://img.shields.io/github/license/ome9ax/target-s3-jsonl)](https://github.com/ome9ax/target-s3-jsonl/blob/main/LICENSE)
[![Python package builder](https://github.com/ome9ax/target-s3-jsonl/workflows/Python%20package/badge.svg)](https://github.com/ome9ax/target-s3-jsonl/actions)
[![codecov](https://codecov.io/gh/ome9ax/target-s3-jsonl/branch/main/graph/badge.svg?token=KV0cn4jKs2)](https://codecov.io/gh/ome9ax/target-s3-jsonl)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/target-s3-jsonl.svg)](https://pypi.org/project/target-s3-jsonl)
[![PyPI version](https://badge.fury.io/py/target-s3-jsonl.svg)](https://badge.fury.io/py/target-s3-jsonl)
[![PyPi project installs](https://img.shields.io/pypi/dm/target-s3-jsonl.svg?maxAge=2592000&label=installs&color=%2327B1FF)](https://pypistats.org/packages/target-s3-jsonl)

[Singer](https://www.singer.io/) target that uploads loads data to S3 in JSONL format
following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/docs/SPEC.md).

## How to use it

`target-s3-jsonl` is a [Singer](https://singer.io) Target which intend to work with regular [Singer](https://singer.io) Tap. It take the output of the tap and export it as a [JSON Lines](http://jsonlines.org/) files into an AWS S3 bucket.

This package is built over the [`target-core`](https://gitlab.com/singer-core/target-core).

## Install

First, make sure Python 3 is installed on your system or follow these
installation instructions for [Mac](http://docs.python-guide.org/en/latest/starting/install3/osx/) or
[Ubuntu](https://www.digitalocean.com/community/tutorials/how-to-install-python-3-and-set-up-a-local-programming-environment-on-ubuntu-16-04).

It's recommended to use a virtualenv:

### Defaults
```bash
python -m venv venv
. venv/bin/activate
pip install --upgrade pip
pip install target-s3-jsonl
```

### Head
```bash
python -m venv venv
. venv/bin/activate
pip install --upgrade pip
pip install --upgrade git+https://github.com/ome9ax/target-s3-jsonl.git@main
```

### Isolated virtual environment
```bash
python -m venv ~/.virtualenvs/target-s3-jsonl
~/.virtualenvs/target-s3-jsonl/bin/pip install target-s3-jsonl
```

Alternative
```bash
python -m venv ~/.virtualenvs/target-s3-jsonl
source ~/.virtualenvs/target-s3-jsonl/bin/activate
pip install target-s3-jsonl
deactivate
```

### To run

Like any other target that's following the singer specificiation:

`some-singer-tap | target-s3-jsonl --config [config.json]`

It's reading incoming messages from STDIN and using the properites in `config.json` to upload data into Postgres.

**Note**: To avoid version conflicts run `tap` and `targets` in separate virtual environments.

### Configuration settings

Running the the target connector requires a `config.json` file. An example with the minimal settings:

```json
{
    "s3_bucket": "my_bucket"
}
```

### Profile based authentication

Profile based authentication used by default using the `default` profile. To use another profile set `aws_profile` parameter in `config.json` or set the `AWS_PROFILE` environment variable.

### Non-Profile based authentication

For non-profile based authentication set `aws_access_key_id` , `aws_secret_access_key` and optionally the `aws_session_token` parameter in the `config.json`. Alternatively you can define them out of `config.json` by setting `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` and `AWS_SESSION_TOKEN` environment variables.


Full list of options in `config.json`:

#### Inherited from `target-core`

| Property                            | Type    | Mandatory? | Description                                                   |
|-------------------------------------|---------|------------|---------------------------------------------------------------|
| path_template                   | String  |            | (Default: None) Custom naming convention of the s3 key. Replaces tokens `date`, `stream`, and `timestamp` with the appropriate values.<br><br>Supports datetime and other python advanced string formatting e.g. `{stream:_>8}_{timestamp:%Y%m%d_%H%M%S}.json` or `{stream}/{timestamp:%Y}/{timestamp:%m}/{timestamp:%d}/{timestamp:%Y%m%d_%H%M%S_%f}.json`.<br><br>Supports "folders" in s3 keys e.g. `folder/folder2/{stream}/export_date={date}/{timestamp}.json`.<br><br>Honors the `s3_key_prefix`,  if set, by prepending the "filename". E.g. path_template = `folder1/my_file.json` and s3_key_prefix = `prefix_` results in `folder1/prefix_my_file.json` |
| timezone_offset                     | Integer |            | Offset value in hour. Use offset `0` hours is you want the `path_template` to use `utc` time zone. The `null` values is used by default. |
| memory_buffer                       | Integer |            | Memory buffer's size used for non partitioned files before storing the data into the temporary file. 64Mb used by default if unspecified. |
| file_size                           | Integer |            | File partitinoning by `size_limit`. File parts will be created. The `path_template` must contain a part section for the part number. Example `"path_template": "{stream}_{date_time:%Y%m%d_%H%M%S}_part_{part:0>3}.json"`. |
| work_dir                            | String  |            | (Default: platform-dependent) Directory of temporary JSONL files with RECORD messages. |
| compression                         | String  |            | The type of compression to apply before uploading. Supported options are `none` (default), `gzip`, and `lzma`. For gzipped files, the file extension will automatically be changed to `.json.gz` for all files. For `lzma` compression, the file extension will automatically be changed to `.json.xz` for all files. |

#### Specific For `target-s3-jsonl`

| Property                            | Type    | Mandatory? | Description                                                   |
|-------------------------------------|---------|------------|---------------------------------------------------------------|
| local                               | Boolean |            | Keep the file in the `work_dir` directory without uploading the files on `s3`. |
| s3_bucket                           | String  | Yes        | S3 Bucket name                                                |
| aws_profile                         | String  |            | AWS profile name for profile based authentication. If not provided, `AWS_PROFILE` environment variable will be used. |
| aws_endpoint_url                    | String  |            | AWS endpoint URL. |
| aws_access_key_id                   | String  |            | S3 Access Key Id. If not provided, `AWS_ACCESS_KEY_ID` environment variable will be used. |
| aws_secret_access_key               | String  |            | S3 Secret Access Key. If not provided, `AWS_SECRET_ACCESS_KEY` environment variable will be used. |
| aws_session_token                   | String  |            | AWS Session token. If not provided, `AWS_SESSION_TOKEN` environment variable will be used. |
| encryption_type                     | String  |            | (Default: 'none') The type of encryption to use. Current supported options are: 'none' and 'KMS'. |
| encryption_key                      | String  |            | A reference to the encryption key to use for data encryption. For KMS encryption, this should be the name of the KMS encryption key ID (e.g. '1234abcd-1234-1234-1234-1234abcd1234'). This field is ignored if 'encryption_type' is none or blank. |
| role_arn                            | String  |            | The ARN of the role to assume |

## Test
Install the tools
```bash
pip install .[test,lint]
```

Run pytest
```bash
pytest -p no:cacheprovider
```

## Lint
```bash
flake8 --show-source --statistics --count --extend-exclude .virtualenvs
```

## Release
1. Update the version number at the beginning of `target-s3-jsonl/target_s3_jsonl/__init__.py`
2. Merge the changes PR into `main`
3. Create a tag `git tag -a 1.0.0 -m 'Release version 1.0.0'`
4. Release the new version in github

## License

Apache License Version 2.0
