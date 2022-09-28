# Change Log

## [1.2.2](https://github.com/ome9ax/target-s3-jsonl/tree/1.2.2) (2022-09-01)

### What's Changed
* #69 decimal.DivisionImpossible raised when handling schema items with high levels of precision by @ome9ax in #75

**Full Changelog**: https://github.com/ome9ax/target-s3-jsonl/compare/1.2.1...1.2.2

## [1.2.1](https://github.com/ome9ax/target-s3-jsonl/tree/1.2.1) (2022-07-13)

### What's Changed
* Added optional config parameter `role_arn`, which allows assuming additional roles. by @haleemur

**Full Changelog**: https://github.com/ome9ax/target-s3-jsonl/compare/1.2.0...1.2.1

## [1.2.0](https://github.com/ome9ax/target-s3-jsonl/tree/1.2.0) (2022-04-11)

### What's Changed
* Upgrade version to 1.2.0: changelog by @ome9ax in https://github.com/ome9ax/target-s3-jsonl/pull/33
* [jsonschema] Remove the deprecated custom exception to Handle `multipleOf` overflow fixed in jsonschema v4.0.0 by @ome9ax in https://github.com/ome9ax/target-s3-jsonl/pull/34
* [jsonschema] remove validation exception catching by @ome9ax in https://github.com/ome9ax/target-s3-jsonl/pull/36
* [persist_lines] save_records argument by @ome9ax in https://github.com/ome9ax/target-s3-jsonl/pull/37

**Full Changelog**: https://github.com/ome9ax/target-s3-jsonl/compare/1.1.0...1.2.0

## [1.1.0](https://github.com/ome9ax/target-s3-jsonl/tree/1.1.0) (2022-04-07)

### What's Changed
* [Python] Advanced string formatting by @ome9ax in https://github.com/ome9ax/target-s3-jsonl/pull/31
* Upgrade version to 1.1.0 by @ome9ax in https://github.com/ome9ax/target-s3-jsonl/pull/32

**Full Changelog**: https://github.com/ome9ax/target-s3-jsonl/compare/1.0.1...1.1.0

## [1.0.1](https://github.com/ome9ax/target-s3-jsonl/tree/1.0.1) (2022-04-06)
[Full Changelog](https://github.com/ome9ax/target-s3-jsonl/compare/1.0.0...1.0.1)

### Closed issues:
- introduce 3.10
- Bump jsonschema from 3.2.0 to 4.4.0
- Bump boto3 from 1.18.22 to 1.21.33

### Merged pull requests:
- [[Python] introduce 3.10](https://github.com/ome9ax/target-s3-jsonl/pull/24)
- [Bump boto3 from 1.21.24 to 1.21.33](https://github.com/ome9ax/target-s3-jsonl/pull/29)
- [Bump jsonschema from 3.2.0 to 4.4.0](https://github.com/ome9ax/target-s3-jsonl/pull/23)
- [Bump boto3 from 1.18.22 to 1.21.24](https://github.com/ome9ax/target-s3-jsonl/pull/22)

## [1.0.0](https://github.com/ome9ax/target-s3-jsonl/tree/1.0.0) (2021-08-18)
[Full Changelog](https://github.com/ome9ax/target-s3-jsonl/compare/0.0.7...1.0.0)

### Closed issues:
- release version 1.0.0 bump 🥳🥂🍾 tests & spec, `100%` complete coverage

### Merged pull requests:
- [[coverage] release version 1.0.0 bump 🥳🥂🍾 tests & spec, `100%` complete coverage](https://github.com/ome9ax/target-s3-jsonl/pull/17)

## [0.0.7](https://github.com/ome9ax/target-s3-jsonl/tree/0.0.7) (2021-08-18)
[Full Changelog](https://github.com/ome9ax/target-s3-jsonl/compare/0.0.6...0.0.7)

### Closed issues:
- Much more specs and tests, coverage increased to `98.09%`

### Merged pull requests:
- [[coverage] tests & spec, 98.09% further coverage](https://github.com/ome9ax/target-s3-jsonl/pull/16)

## [0.0.6](https://github.com/ome9ax/target-s3-jsonl/tree/0.0.6) (2021-08-17)
[Full Changelog](https://github.com/ome9ax/target-s3-jsonl/compare/0.0.5.2...0.0.6)

### Closed issues:
- Much more specs and tests, coverage increased to `96.91%`

### Merged pull requests:
- [[coverage] bump version 0.0.6 changelog update: Much more specs and tests, coverage increased to `96.91%`](https://github.com/ome9ax/target-s3-jsonl/pull/15)

## [0.0.5.2](https://github.com/ome9ax/target-s3-jsonl/tree/0.0.5.2) (2021-08-13)
[Full Changelog](https://github.com/ome9ax/target-s3-jsonl/compare/0.0.5.1...0.0.5.2)

### New features:
- replace `io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')` with `sys.stdin` as it's already natively defined as `<_io.TextIOWrapper name='<stdin>' mode='r' encoding='utf-8'>`

### Merged pull requests:
- [[readlines] replace `io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')` with `sys.stdin`](https://github.com/ome9ax/target-s3-jsonl/pull/13)

## [0.0.5.1](https://github.com/ome9ax/target-s3-jsonl/tree/0.0.5.1) (2021-08-12)
[Full Changelog](https://github.com/ome9ax/target-s3-jsonl/compare/0.0.5...0.0.5.1)

### Fixed bugs:
- Issue to decompress archived files

### Closed issues:
- See PR

### Merged pull requests:
- [[compression] fix compression management](https://github.com/ome9ax/target-s3-jsonl/pull/12)

## [0.0.5](https://github.com/ome9ax/target-s3-jsonl/tree/0.0.5) (2021-08-12)
[Full Changelog](https://github.com/ome9ax/target-s3-jsonl/compare/0.0.4...0.0.5)

### New features:
- I now store the rows in an Array on memory, and unload the Array into the file by batches. By default the batch size is 64Mb configurable with the `memory_buffer` config option.
- I also extended the compression config option to the `lzma` compression algorithm, storing the output file under the .xz extension.
- Update the message parsing to handle the metadata through the `add_metadata_columns` config option according to the [stitch documentation](https://www.stitchdata.com/docs/data-structure/integration-schemas#sdc-columns).
- `timezone_offset` config option added to use `utc` timestamp in the `naming_convention`
- `local` configuration option to store the data on `tmp_dir` local dir without uploading to `s3`
- More specs and tests

### Merged pull requests:
- [[File load buffer] unload the data from a 64Mb memory buffer](https://github.com/ome9ax/target-s3-jsonl/pull/8)
- [[Metadata] manage tap Metadata _sdc columns according to the stitch documentation](https://github.com/ome9ax/target-s3-jsonl/pull/9)

## [0.0.4](https://github.com/ome9ax/target-s3-jsonl/tree/0.0.4) (2021-08-09)
[Full Changelog](https://github.com/ome9ax/target-s3-jsonl/compare/0.0.0...0.0.4)

### New features:
- Initial release
