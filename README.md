# Mariner Paperless → Receipts Space Migration

A Python script to migrate receipts from Mariner Paperless (also known as ReceiptWallet) to [Receipts Space](https://receipts-space.com) by Dirk Holtwick.

## Background

Mariner Software went out of business, leaving Paperless users with a large library of receipts in a proprietary format with no migration path. This script migrates your metadata (vendors, categories, amounts, dates, payment dates, notes, and tags) into Receipts Space.

**Important:** The script only migrates metadata — not the PDF files themselves. You need to export and import the PDFs separately (see Step 1 below).

Successfully migrated ~24,000 receipts across four libraries (EUR, USD, AUD).

## If you ran a version of this script before September 2026

Two bugs in the transaction-writing code were fixed on 2026-09-09. Both needed
specific conditions to bite, and no damage from either has ever been observed —
the five libraries migrated with this tool were re-verified afterwards, and all
36 transaction streams pass a full hash-chain check with zero faults. Worth
knowing about anyway:

- **The next-index calculation was wrong past 1000 files.** Receipts Space
  splits a transaction stream into `2/1/`, `2/2/`, … once it passes 1000 files,
  and each folder restarts at `0.dat`. The script took its starting index from
  the highest *filename*, which sticks at 999 forever. It only matters if you
  point `--client-id` at a stream that ALREADY holds more than 1000 files — it
  would then resume at 1000 and overwrite from `2/1/0.dat` on. The `createClientId`
  this README tells you to use is normally a near-empty stream, so the path that
  triggers it was the script's own `--help` text, which used to say "largest
  folder in transactions/". That contradiction is now gone.
- **The chain hash used the previous file's `c` instead of hashing the whole
  file.** This one only ever existed in this public version, not in the script
  the original migrations were run with. It fails loudly rather than quietly:
  Receipts Space reports "Previous hash mismatch" on the first read.

If you did point `--client-id` at a stream with more than 1000 files, check
`transactions/<clientId>/2/1/` before assuming all is well. And if Receipts Space
ever offers **Repair Library**, don't take it — repair *prunes* a broken chain
rather than mending it, so it discards rather than recovers.

## Tested With

- Mariner Paperless v3.0.80
- Receipts Space v3.3, and re-verified against v3.6 (September 2026)

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

A library can hold several `transactions/<clientId>/` folders, and Receipts Space
merges them all, so any of them works. Use `createClientId`: it is usually not the
folder Receipts Space is busy writing to, which keeps the migration out of its way.

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
| `--posted-column` | | CSV column holding the payment date (default: `Posted`) |
| `--credit-payment-method` | | Payment method that means income, not expense. Repeatable |
| `--db-tag` | | Tag added to every migrated entry, useful for filtering |
| `--dry-run` | | Preview without writing any files |

### Income vs expense

Paperless has no sign convention: income is exported as a **positive** amount just
like an expense, and only the payment method distinguishes the two. Without
`--credit-payment-method` every entry migrates as an expense. Pass the flag once
per method that means money coming in:

```bash
--credit-payment-method "Bij-boeking" --credit-payment-method "Deposit"
```

### Payment date column

Account and Posted are user-named custom fields in Paperless, so the payment-date
column is called something different in every library (`Posted`, `Bij/af d.d.`, …).
Point `--posted-column` at yours — the dry run reports how many payment dates it
found, so a wrong name shows up as `0/N` rather than failing silently.

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
