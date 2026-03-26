# Mariner Paperless → Receipts Space Migration

A Python script to migrate receipts from Mariner Paperless (also known as ReceiptWallet) to [Receipts Space](https://receipts-space.com) by Dirk Holtwick.

## Background

Mariner Software went out of business, leaving Paperless users with a large library of receipts in a proprietary format with no migration path. This script migrates your metadata (vendors, categories, amounts, dates, payment dates, notes, and tags) into Receipts Space.

**Important:** The script only migrates metadata — not the PDF files themselves. You need to export and import the PDFs separately (see Step 1 below).

Successfully migrated ~23,000 receipts across three libraries (EUR, USD, AUD).

## Tested With

- Mariner Paperless v3.0.80
- Receipts Space v3.3

## Prerequisites

- Python 3.9+
- Mariner Paperless still installed and accessible on your Mac
- [Receipts Space](https://receipts-space.com) installed

## Migration Steps

### Step 1: Export your PDFs from Paperless

1. Open Mariner Paperless
2. Select all receipts: **Edit → Select All** (⌘A)
3. Export: **File → Save Individual Receipts**
4. Choose a folder to save all PDFs (e.g. `~/Desktop/Paperless Export/`)
5. Wait for the export to complete — this may take a while for large libraries

### Step 2: Export your metadata as CSV

1. Still in Paperless, with all receipts selected
2. **File → Export as CSV...**
3. Save the CSV file (e.g. `~/Desktop/Documents.csv`)

### Step 3: Create a new Receipts Space library

1. Open Receipts Space
2. **File → New Library** — give it a name (e.g. "My Receipts")
3. Import all your PDFs: drag the entire export folder onto Receipts Space
4. Wait for RS to finish importing and processing all PDFs

### Step 4: Find your Client ID

1. In Finder, navigate to your RS library folder
2. Open `info.json` in a text editor — copy the value of `createClientId`

### Step 5: Run the migration script

Always do a dry run first to verify everything looks correct:

```bash
python3 paperless_to_receipts.py \
    --csv ~/Desktop/Documents.csv \
    --library "/path/to/Your Library.receipts-space" \
    --client-id <clientId> \
    --currency EUR \
    --dry-run
```

Check the output — it shows how many receipts were matched and what metadata would be written. When you are happy, run without `--dry-run`:

```bash
python3 paperless_to_receipts.py \
    --csv ~/Desktop/Documents.csv \
    --library "/path/to/Your Library.receipts-space" \
    --client-id <clientId> \
    --currency EUR
```

Receipts Space can stay open while the script runs. For small libraries changes appear almost instantly. For large migrations, closing RS first is recommended to avoid any conflicts.

### Step 6: Verify

Check a few receipts in Receipts Space to confirm vendors, amounts, dates and categories look correct.

## Arguments

| Argument | Required | Description |
|----------|----------|-------------|
| `--csv` | Yes | Path to Paperless CSV export |
| `--library` | Yes | Path to Receipts Space library |
| `--client-id` | Yes | Client ID (see Step 4) |
| `--currency` | | Currency code: EUR, USD, AUD, etc. (default: EUR) |
| `--db-tag` | | Tag added to every migrated entry, useful for filtering |
| `--dry-run` | | Preview without writing any files |

## Features

- Migrates vendors, categories, amounts, dates, payment dates, notes and tags
- Handles EU and US number formats (e.g. 1.234,56 and 1,234.56)
- Normalises filenames (double extensions, multiple spaces, etc.)
- Auto-normalises vendor name spelling variations (majority-wins)
- Skips receipts that were deleted in Paperless
- Supports multiple currency libraries via --currency

## Limitations

- PDFs must be imported into Receipts Space first — the script only writes metadata
- Account and Posted are optional custom fields in Paperless. If present, Account is migrated as a tag and Posted as the payment date. If absent, the script skips them without errors
- Receipts Space does not currently support custom fields
- Each Paperless library (if you have multiple currencies) needs to be migrated separately with its own --currency flag

## Caveats

- Receipts Space can stay open while the script runs. For small libraries changes appear almost instantly. For large migrations, closing RS first is recommended to avoid any conflicts
- If entries do not appear updated, quit RS and delete the cache:
  ~/Library/Application Support/de.holtwick.mac.homebrew.Receipts2/data/<workspaceId>.*
  Then reopen Receipts Space — it will rebuild from the transaction files
- Temporarily disable duplicate detection in Receipts Space during migration
- Always test with --dry-run before running the full migration

## Contact Normalisation

If your Paperless data has spelling variants of the same vendor (e.g. "Starbucks" and "Starbucks Coffee"), edit the CONTACT_NORMALISE dictionary at the top of the script:

```python
CONTACT_NORMALISE = {
    "Starbucks Coffee": "Starbucks",
}
```

The script also auto-detects variants by grouping case-insensitive duplicates and picking the majority spelling.

## License

GNU General Public License v3.0 — see LICENSE for details.
