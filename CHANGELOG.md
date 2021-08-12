# Change Log

## [v0.0.5.1](https://github.com/ome9ax/target-s3-jsonl/tree/v0.0.5.1) (2021-08-09)
[Full Changelog](https://github.com/ome9ax/target-s3-jsonl/tree/v0.0.5...v0.0.5.1)

**Fixed bugs:**
- Issue to decompress archived files

**Closed issues:**
- See PR

**Merged pull requests:**
- [[compression] fix compression management](https://github.com/ome9ax/target-s3-jsonl/pull/12)

## [v0.0.5](https://github.com/ome9ax/target-s3-jsonl/tree/v0.0.5) (2021-08-12)
[Full Changelog](https://github.com/ome9ax/target-s3-jsonl/tree/v0.0.4...v0.0.5)

**New features:**
- I now store the rows in an Array on memory, and unload the Array into the file by batches. By default the batch size is 64Mb configurable with the `memory_buffer` config option.
- I also extended the compression config option to the `lzma` compression algorithm, storing the output file under the .xz extension.
- Update the message parsing to handle the metadata through the `add_metadata_columns` config option according to the [stitch documentation](https://www.stitchdata.com/docs/data-structure/integration-schemas#sdc-columns).
- `timezone_offset` config option added to use `utc` timestamp in the `naming_convention`
- `local` configuration option to store the data on `tmp_dir` local dir without uploading to `s3`
- More specs and tests

**Merged pull requests:**
- [[File load buffer] unload the data from a 64Mb memory buffer](https://github.com/ome9ax/target-s3-jsonl/pull/8)
- [[Metadata] manage tap Metadata _sdc columns according to the stitch documentation](https://github.com/ome9ax/target-s3-jsonl/pull/9)

## [v0.0.4](https://github.com/ome9ax/target-s3-jsonl/tree/v0.0.4) (2021-08-09)

**New features:**
- Initial release
