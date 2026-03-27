# Security Policy

## About This Project

This is a single-purpose, offline migration script that reads local files from Mariner Paperless and writes to a local Receipts Space library. It has no server components, no network connectivity, and no external dependencies beyond the Python standard library.

## Reporting a Vulnerability

If you discover a security issue (e.g. a bug that could cause data loss or corruption of your Receipts Space library), please open a [GitHub Issue](../../issues) describing the problem.

There is no formal security response process for this project — it is maintained by a single contributor on a best-effort basis. However all reports will be read and taken seriously.

## General Advice

- Always run `--dry-run` before a full migration to verify the script behaves as expected
- Keep a backup of your Receipts Space library before running the script
- Review the source code before running any third-party script on your data
