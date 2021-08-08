# target-s3-jsonl

<!-- [![License: Apache2](https://img.shields.io/badge/License-Apache2-blue.svg)](https://opensource.org/licenses/Apache-2.0) -->
<!-- ![PyPI - License](https://img.shields.io/pypi/l/target-s3-jsonl) -->
![GitHub - License](https://img.shields.io/github/license/ome9ax/target-s3-jsonl)
[![Python package builder](https://github.com/ome9ax/target-s3-jsonl/workflows/Python%20package/badge.svg)](https://github.com/ome9ax/target-s3-jsonl)
[![codecov](https://codecov.io/gh/ome9ax/target-s3-jsonl/branch/main/graph/badge.svg?token=KV0cn4jKs2)](https://codecov.io/gh/ome9ax/target-s3-jsonl)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/target-s3-jsonl.svg)](https://pypi.org/project/target-s3-jsonl/)
[![PyPI version](https://badge.fury.io/py/target-s3-jsonl.svg)](https://badge.fury.io/py/target-s3-jsonl)
[![PyPi project installs](https://img.shields.io/pypi/dm/target-s3-jsonl.svg?maxAge=2592000&label=installs&color=%2327B1FF)](https://pypi.org/project/target-s3-jsonl)
<!-- [![PyPi project version](https://img.shields.io/pypi/v/target-s3-jsonl.svg)](https://pypi.org/project/target-s3-jsonl) -->

[Singer](https://www.singer.io/) target that uploads loads data to S3 in JSONL format
following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/docs/SPEC.md).

## How to use it

`target-s3-jsonl` is a [Singer](https://singer.io) Target which intend to work with regular [Singer](https://singer.io) Tap. It take the output of the tap and export it as a [JSON Lines](http://jsonlines.org/) files.

It re-uses [PipelineWise `pipelinewise-target-s3-csv`](https://transferwise.github.io/pipelinewise) s3 client logic to upload the JSONL file directly to S3.

## Install

First, make sure Python 3 is installed on your system or follow these
installation instructions for [Mac](http://docs.python-guide.org/en/latest/starting/install3/osx/) or
[Ubuntu](https://www.digitalocean.com/community/tutorials/how-to-install-python-3-and-set-up-a-local-programming-environment-on-ubuntu-16-04).

It's recommended to use a virtualenv:

```bash
python3 -m venv venv
pip install target-s3-jsonl
```

or

```bash
python3 -m venv venv
. venv/bin/activate
pip install --upgrade pip
pip install .
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

| Property                            | Type    | Required?  | Description                                                   |
|-------------------------------------|---------|------------|---------------------------------------------------------------|
| aws_access_key_id                   | String  | No         | S3 Access Key Id. If not provided, `AWS_ACCESS_KEY_ID` environment variable will be used. |
| aws_secret_access_key               | String  | No         | S3 Secret Access Key. If not provided, `AWS_SECRET_ACCESS_KEY` environment variable will be used. |
| aws_session_token                   | String  | No         | AWS Session token. If not provided, `AWS_SESSION_TOKEN` environment variable will be used. |
| aws_endpoint_url                    | String  | No         | AWS endpoint URL. |
| aws_profile                         | String  | No         | AWS profile name for profile based authentication. If not provided, `AWS_PROFILE` environment variable will be used. |
| s3_bucket                           | String  | Yes        | S3 Bucket name                                                |
| s3_key_prefix                       | String  |            | (Default: None) A static prefix before the generated S3 key names. Using prefixes you can 
| encryption_type                     | String  | No         | (Default: 'none') The type of encryption to use. Current supported options are: 'none' and 'KMS'. |
| encryption_key                      | String  | No         | A reference to the encryption key to use for data encryption. For KMS encryption, this should be the name of the KMS encryption key ID (e.g. '1234abcd-1234-1234-1234-1234abcd1234'). This field is ignored if 'encryption_type' is none or blank. |
| compression                         | String  | No         | The type of compression to apply before uploading. Supported options are `none` (default) and `gzip`. For gzipped files, the file extension will automatically be changed to `.jsonl.gz` for all files. |
| naming_convention                   | String  | No         | (Default: None) Custom naming convention of the s3 key. Replaces tokens `date`, `stream`, and `timestamp` with the appropriate values. <br><br>Supports "folders" in s3 keys e.g. `folder/folder2/{stream}/export_date={date}/{timestamp}.jsonl`. <br><br>Honors the `s3_key_prefix`,  if set, by prepending the "filename". E.g. naming_convention = `folder1/my_file.jsonl` and s3_key_prefix = `prefix_` results in `folder1/prefix_my_file.jsonl` |
| temp_dir                            | String  |            | (Default: platform-dependent) Directory of temporary JSONL files with RECORD messages. |

## License

Apache License Version 2.0

See [LICENSE](LICENSE) to see the full text.
