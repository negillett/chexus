# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Added BucketItem's "get_headers" method
- Added tests for BucketItem's "get_headers" method

### Changed
- Added "headers" attribute to BucketItem
- Updated existing tests to incorporate "headers" attribute
- Made upload use new BucketItem "headers" attribute

## [2.1.0] - 2020-02-07

### Added
- Implemented search and download methods
- Added tests and example scripts for search and download

### Changed
- Updated documentation for Client and models
- Improved processing dates, time in TableItems
- Added "key" attribute to TableItem
- Made upload use new TableItem key attribute

## [2.0.0] - 2020-01-31

### Added
- Added pidiff job to travis

### Changed
- Renamed model classes
- Removed "push" method from Client
- Revised TableItem (formerly PublishItem)
- Improved "publish" method
- Updated documentation and examples accordingly

## [1.0.2] - 2020-01-29

### Added
- Introduced API documentation
- Added pidiff tox environment/travis ci job

### Changed
- Removed changelog compare link for initial release
- Updated README.md with PyPI information

### Fixed
- Corrected changelog compare link for v1.0.0...v1.0.1
- Various fixes related to publishing documentation

## [1.0.1] - 2020-01-29

### Fixed
- Corrected version
- Updated changelog

## 1.0.0 - 2020-01-29

- Initial release to PyPI

[Unreleased]: https://github.com/nathanegillett/chexus/compare/v2.1.0...HEAD
[2.1.0]: https://github.com/nathanegillett/chexus/compare/v2.0.0...v2.1.0
[2.0.0]: https://github.com/nathanegillett/chexus/compare/v1.0.2...v2.0.0
[1.0.2]: https://github.com/nathanegillett/chexus/compare/v1.0.1...v1.0.2
[1.0.1]: https://github.com/nathanegillett/chexus/compare/v1.0.0...v1.0.1
