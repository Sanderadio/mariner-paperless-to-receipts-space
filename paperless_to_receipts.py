#!/usr/bin/env python3
"""
Paperless (ReceiptWallet) → Receipts Space Migration Script
============================================================
Migrates receipts exported from Mariner Paperless (aka ReceiptWallet) into
a Receipts Space library by writing transaction files directly into the
library's internal format — no import required.

What it does:
  - Reads a Paperless CSV export and matches entries to already-imported PDFs
    in a Receipts Space library (by filename)
  - Creates contact, category and tag records
  - Writes correct amounts, currencies, dates, payment dates and notes
  - Auto-normalises contact name spelling variations (majority-wins)
  - Handles all Paperless amount formats: European (1.234,56), US (1,234.56),
    plain decimals, and the double-minus negative bug (--12.34 → -12.34)
  - Handles .rtf.pdf double extensions (RTF files converted to PDF)
  - Supports a dry run to preview changes before writing

Prerequisites:
  1. Export your Paperless database: File > Export > CSV + PDFs
  2. Create a new Receipts Space library and import all PDFs
     (drag the folder onto Receipts Space, or use File > Import)
  3. Find the client ID: look in the library's transactions/ folder —
     it's the largest subfolder name (most .dat files)
  4. Note the library path (the .receipts-space package or folder)

Usage:
  # Always dry run first to check matches:
  python3 paperless_to_receipts.py \\
      --csv MyReceipts.csv \\
      --library "/path/to/My Library.receipts-space" \\
      --client-id <clientId> \\
      --currency EUR \\
      --dry-run

  # Full run:
  python3 paperless_to_receipts.py \\
      --csv MyReceipts.csv \\
      --library "/path/to/My Library.receipts-space" \\
      --client-id <clientId> \\
      --currency EUR

  # After the script completes:
  # Receipts Space should pick up the changes automatically.
  # If receipts don't appear updated, quit RS and delete the cache:
  #    ~/Library/Application Support/de.holtwick.mac.homebrew.Receipts2/data/<workspaceId>*
  # Then reopen Receipts Space — it will rebuild from the transaction files.

Arguments:
  --csv         Path to Paperless CSV export
  --library     Path to Receipts Space library (.receipts-space or folder)
  --client-id   Client ID to write transactions under (largest folder in transactions/)
  --currency    Currency code for this library: EUR, USD, AUD, etc. (default: EUR)
  --db-tag      Optional tag added to every entry (e.g. "MyLibrary") for filtering
  --dry-run     Preview changes without writing any files

CSV columns used (standard Paperless export):
  Date          DD/MM/YYYY — document date
  Title/Merchant — vendor/contact name
  Currency      — ignored; use --currency instead
  Amount        — gross amount (handles EU/US number formats)
  Category      — mapped to RS category field
  Payment Method — mapped to a tag
  Notes         — mapped to RS notes field
  Tags          — space-separated tags
  Account       — mapped to a tag
  Posted        — YYYY-MM-DD payment date

Contact normalisation:
  Edit CONTACT_NORMALISE below to fix known spelling variants in your data.
  The script also auto-detects variants by grouping case-insensitive duplicates
  and picking the majority spelling — manual overrides always win.
"""

import argparse
import unicodedata
import re
import csv
import hashlib
import json
import sys
import time
import uuid
from base64 import urlsafe_b64encode
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from urllib.parse import unquote


# ── Contact normalisation ──────────────────────────────────────────────────────
# Add entries here to fix known spelling variants in your Paperless data.
# Format: "wrong spelling": "correct spelling"
# These override the automatic majority-wins normalisation.

CONTACT_NORMALISE = {
    # Example: "Starbucks Coffee": "Starbucks",
}

# Names that should never be auto-normalised (always kept as-is even if minority):
CONTACT_NORMALISE_PROTECTED = set()


# ── Helpers ────────────────────────────────────────────────────────────────────

def parse_amount(raw):
    """
    Parse amount string, handling multiple number formats and Paperless bugs.
    
    Supported formats:
      - European: 1.234,56 (dot=thousands, comma=decimal)
      - US:       1,234.56 (comma=thousands, dot=decimal)
      - Plain:    1234.56 or 1234,56
      - Negative: -12.34 or Paperless bug format --12.34
    """
    if not raw or not raw.strip():
        return 0.0
    cleaned = raw.strip()
    # Paperless exports negative amounts as '--X.XX' instead of '-X.XX'
    if cleaned.startswith("--"):
        cleaned = "-" + cleaned[2:]
    if "," in cleaned and "." in cleaned:
        last_comma = cleaned.rfind(",")
        last_dot = cleaned.rfind(".")
        if last_comma > last_dot:
            # European: dot=thousands, comma=decimal → "1.234,56"
            cleaned = cleaned.replace(".", "").replace(",", ".")
        else:
            # US: comma=thousands, dot=decimal → "1,234.56"
            cleaned = cleaned.replace(",", "")
    elif "," in cleaned:
        # Comma-only → decimal separator → "9,50"
        cleaned = cleaned.replace(",", ".")
    try:
        return float(cleaned)
    except ValueError:
        return 0.0


def parse_date_csv(raw):
    """DD/MM/YYYY → (DD-MM-YYYY string for filename, YYYYMMDD int for RS)"""
    try:
        dt = datetime.strptime(raw.strip(), "%d/%m/%Y")
        return dt.strftime("%d-%m-%Y"), int(dt.strftime("%Y%m%d"))
    except ValueError:
        return "", 0


def parse_date_posted(raw):
    """YYYY-MM-DD → YYYYMMDD int for RS paymentDate field"""
    if not raw or not raw.strip():
        return 0
    try:
        dt = datetime.strptime(raw.strip(), "%Y-%m-%d")
        return int(dt.strftime("%Y%m%d"))
    except ValueError:
        return 0


def make_filename(vendor, date_str, suffix=0):
    """Build the PDF filename as Paperless would have named it."""
    # Paperless replaces "/" with "-" in filenames
    safe_vendor = vendor.replace("/", "-")
    base = f"{safe_vendor} - {date_str}"
    if suffix > 1:
        base = f"{base} {suffix}"
    return f"{base}.pdf"


def new_id():
    return uuid.uuid4().hex[:26].ljust(26, "0")


def sha256_b64(data):
    digest = hashlib.sha256(data).digest()
    return urlsafe_b64encode(digest).rstrip(b"=").decode()


def extract_filename_from_url(url):
    """Extract and normalise filename from a RS asset URL."""
    if not url:
        return ""
    filename = unquote(url.split("?")[0].split("/")[-1])
    filename = unicodedata.normalize("NFC", filename)
    # RTF files converted to PDF keep a double extension — normalise it
    if filename.endswith(".rtf.pdf"):
        filename = filename[:-8] + ".pdf"
    # Paperless sometimes exports filenames with multiple spaces
    filename = re.sub(r" {2,}", " ", filename)
    return filename


# ── Step 1: Build lookup table from CSV ────────────────────────────────────────

def build_lookup(csv_path):
    """
    Parse the Paperless CSV export and build a filename → metadata lookup.
    
    Handles duplicate entries (same vendor + date): Paperless sorts them by
    amount ascending, naming them Vendor - DD-MM-YYYY.pdf,
    Vendor - DD-MM-YYYY 2.pdf, Vendor - DD-MM-YYYY 3.pdf, etc.
    """
    rows = []
    with open(csv_path, newline="", encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            rows.append(row)

    # Auto-detect contact name variants: group by lowercase, pick majority spelling
    raw_counts = Counter(r.get("Title/Merchant", "").strip() for r in rows if r.get("Title/Merchant", "").strip())
    lower_groups = defaultdict(list)
    for name, count in raw_counts.items():
        lower_groups[name.lower()].append((name, count))
    auto_normalise = {}
    for variants in lower_groups.values():
        if len(variants) > 1:
            canonical = max(variants, key=lambda x: x[1])[0]
            if any(name in CONTACT_NORMALISE_PROTECTED for name, _ in variants):
                continue
            for name, _ in variants:
                if name != canonical:
                    auto_normalise[name] = canonical
    normalise_map = {**auto_normalise, **CONTACT_NORMALISE}
    if normalise_map:
        print(f"\n   Contact normalisation ({len(normalise_map)} corrections):")
        for src, dst in sorted(normalise_map.items()):
            print(f"     {src!r} → {dst!r}")

    groups = defaultdict(list)
    for row in rows:
        vendor_raw = row.get("Title/Merchant", "").strip()
        vendor = normalise_map.get(vendor_raw, vendor_raw)
        date_raw = row.get("Date", "").strip()
        filename_date, rs_date = parse_date_csv(date_raw)
        if not vendor or not filename_date:
            continue
        amount = parse_amount(row.get("Amount", "0"))
        category = row.get("Category", "").strip()
        notes = row.get("Notes", "").strip()

        # Build tags from Account, Tags, and Payment Method columns
        account_tags = [t.strip() for t in row.get("Account", "").split(",") if t.strip()]
        csv_tags = [t.strip() for t in row.get("Tags", "").split(" ") if t.strip()]
        pm = row.get("Payment Method", "").strip()
        payment_tags = [pm] if pm else []
        all_tags = list(dict.fromkeys(account_tags + csv_tags + payment_tags))

        groups[(vendor, filename_date)].append({
            "vendor": vendor,
            "filename_date": filename_date,
            "rs_date": rs_date,
            "amount": amount,
            "category": category,
            "notes": notes,
            "tags_raw": all_tags,
            "posted": parse_date_posted(row.get("Posted", "")),
        })

    lookup = {}
    for (vendor, filename_date), group in groups.items():
        # Sort by amount ascending — Paperless duplicate ordering
        group_sorted = sorted(group, key=lambda r: r["amount"])
        for i, entry in enumerate(group_sorted):
            suffix = 0 if i == 0 else i + 1
            filename = make_filename(vendor, filename_date, suffix)
            filename = unicodedata.normalize("NFC", filename)
            entry["expected_filename"] = filename
            lookup[filename] = entry

    print(f"\n✅ Built lookup table: {len(lookup)} entries")
    return lookup, normalise_map


# ── Step 2: Collect unique entities ────────────────────────────────────────────

def collect_entities(lookup, db_tag=""):
    contacts, categories, tags = set(), set(), set()
    for entry in lookup.values():
        if entry.get("vendor"):
            contacts.add(entry["vendor"])
        if entry.get("category"):
            categories.add(entry["category"])
        for tag in entry.get("tags_raw", []):
            if tag:
                tags.add(tag)
    if db_tag:
        tags.add(db_tag)
    return sorted(contacts), sorted(categories), sorted(tags)


# ── Step 3: Read existing RS library ───────────────────────────────────────────

def read_library(library_path):
    """
    Read all transaction .dat files in the library.
    Returns doc_entries (filename→record), and existing entity name→id maps.
    Uses highest _v (version) record per _id as the winner.
    Skips deleted records (_deleted: true).
    """
    tx_root = Path(library_path) / "transactions"
    client_dirs = [d for d in tx_root.iterdir() if d.is_dir()]
    print(f"   Found {len(client_dirs)} clientId folder(s)")

    all_records = {}
    dat_files = list(tx_root.rglob("*.dat"))
    print(f"   Reading {len(dat_files)} transaction files...")

    for dat_file in dat_files:
        try:
            content = dat_file.read_text(encoding="utf-8")
            lines = content.split("\n", 1)
            if len(lines) < 2:
                continue
            for line in lines[1].split("\n"):
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                    rid = record.get("_id")
                    if not rid:
                        continue
                    existing = all_records.get(rid)
                    if existing is None:
                        all_records[rid] = record
                    else:
                        has_url = bool(record.get("url"))
                        existing_has_url = bool(existing.get("url"))
                        if has_url and not existing_has_url:
                            all_records[rid] = record
                        elif not has_url and existing_has_url:
                            pass
                        elif record.get("_v", 0) > existing.get("_v", 0):
                            all_records[rid] = record
                except json.JSONDecodeError:
                    continue
        except Exception:
            continue

    doc_entries = {}
    existing_contacts, existing_categories, existing_tags = {}, {}, {}

    for rid, record in all_records.items():
        if record.get("_deleted", False):
            continue
        rtype = record.get("_type", "")
        if rtype == "doc":
            url = record.get("url", "")
            filename = extract_filename_from_url(url)
            if filename:
                doc_entries[filename] = record
        elif rtype == "contact":
            name = record.get("name") or record.get("title", "")
            if name:
                existing_contacts[name] = rid
        elif rtype == "category":
            name = record.get("title", "")
            if name:
                existing_categories[name] = rid
        elif rtype == "tag":
            name = record.get("title", "")
            if name:
                existing_tags[name] = rid

    print(f"✅ Found {len(doc_entries)} doc entries")
    print(f"   Existing contacts: {len(existing_contacts)}, "
          f"categories: {len(existing_categories)}, tags: {len(existing_tags)}")
    return doc_entries, existing_contacts, existing_categories, existing_tags


# ── Step 4: Match CSV rows to RS doc entries ────────────────────────────────────

def match_entries(lookup, doc_entries):
    """
    Match RS doc entries to CSV rows by filename (case-insensitive).
    Reports unmatched entries on both sides.
    """
    matches = []
    unmatched_csv = set(lookup.keys())
    unmatched_rs = []
    lookup_lower = {k.lower(): k for k in lookup}

    for filename, rs_record in doc_entries.items():
        canonical = lookup_lower.get(filename.lower())
        if canonical is not None:
            match = {
                "rs_id": rs_record["_id"],
                "rs_current_gross": rs_record.get("gross"),
                "rs_current_tags": rs_record.get("tags", {}),
                "filename": filename,
            }
            match.update(lookup[canonical])
            matches.append(match)
            unmatched_csv.discard(canonical)
        else:
            unmatched_rs.append(filename)

    print(f"\n✅ Matched: {len(matches)}")
    if unmatched_rs:
        print(f"⚠️  {len(unmatched_rs)} RS entries had no CSV match:")
        for fn in sorted(unmatched_rs)[:10]:
            print(f"   - {fn}")
        if len(unmatched_rs) > 10:
            print(f"   ... and {len(unmatched_rs) - 10} more")
    if unmatched_csv:
        print(f"⚠️  {len(unmatched_csv)} CSV rows had no RS entry:")
        for fn in sorted(unmatched_csv)[:10]:
            print(f"   - {fn}")
        if len(unmatched_csv) > 10:
            print(f"   ... and {len(unmatched_csv) - 10} more")
    return matches


# ── RS transaction file writing ────────────────────────────────────────────────

def dat_subpath(index):
    """
    Compute the .dat file path for a given transaction index.
    RS distributes files in depth-prefixed folders of 1000:
      0–999   → 1/0.dat .. 1/999.dat
      1000    → 2/1/0.dat
      2000    → 2/2/0.dat
    """
    names = [str(index % 1000)]
    remaining = index // 1000
    while remaining > 0:
        names.append(str(remaining % 1000))
        remaining //= 1000
    depth = len(names)
    parts = [str(depth)] + list(reversed(names[1:])) + [names[0]]
    return Path(*parts).with_suffix(".dat")


def get_next_index(tx_client_dir):
    dat_files = list(tx_client_dir.rglob("*.dat"))
    if not dat_files:
        return 0
    indices = []
    for f in dat_files:
        try:
            parts = list(f.relative_to(tx_client_dir).parts)
            indices.append(int(parts[-1].replace(".dat", "")))
        except (ValueError, IndexError):
            continue
    return max(indices) + 1 if indices else 0


def get_prev_hash(tx_client_dir, index):
    if index == 0:
        return None
    prev_path = tx_client_dir / dat_subpath(index - 1)
    if not prev_path.exists():
        return None
    try:
        content = prev_path.read_bytes()
        header_line = content.split(b"\n", 1)[0]
        header = json.loads(header_line)
        return header.get("c")
    except Exception:
        return None


def write_transaction(tx_client_dir, index, records, timestamp, device_id, dry_run=False):
    content = "\n".join(json.dumps(r, ensure_ascii=False, separators=(",", ":")) for r in records)
    content_bytes = content.encode("utf-8")
    prev_hash = get_prev_hash(tx_client_dir, index)
    header = {
        "app": "rs",
        "b": 33002,
        "c": sha256_b64(content_bytes),
        "did": device_id,
        "s": len(content_bytes),
        "t": timestamp,
        "v": 1,
    }
    if prev_hash:
        header["p"] = prev_hash
    full_content = json.dumps(header, ensure_ascii=False, separators=(",", ":")) + "\n" + content
    full_path = tx_client_dir / dat_subpath(index)
    if not dry_run:
        full_path.parent.mkdir(parents=True, exist_ok=True)
        full_path.write_text(full_content, encoding="utf-8")
    return full_path


# ── Step 5: Write entity and doc records ───────────────────────────────────────

def write_all(matches, library_path, client_id, device_id,
              contacts_needed, categories_needed, tags_needed,
              existing_contacts, existing_categories, existing_tags,
              dry_run=False, db_tag="", currency="EUR"):

    tx_client_dir = Path(library_path) / "transactions" / client_id
    if not tx_client_dir.exists():
        print(f"❌ Cannot find: {tx_client_dir}")
        sys.exit(1)

    next_idx = get_next_index(tx_client_dir)
    now = int(time.time())
    idx = next_idx
    prefix = "🔍 DRY RUN — " if dry_run else ""

    # Phase A: Create any missing contacts, categories, tags
    contact_ids = dict(existing_contacts)
    category_ids = dict(existing_categories)
    tag_ids = dict(existing_tags)

    new_contacts = [c for c in contacts_needed if c not in contact_ids]
    new_categories = [c for c in categories_needed if c not in category_ids]
    new_tags = [t for t in tags_needed if t not in tag_ids]

    print(f"\n{prefix}Phase A: Creating entity records...")
    print(f"   Contacts to create: {len(new_contacts)}")
    print(f"   Categories to create: {len(new_categories)}")
    print(f"   Tags to create: {len(new_tags)}")

    entity_records = []
    for name in new_contacts:
        rid = new_id()
        contact_ids[name] = rid
        entity_records.append({"_deleted": False, "_id": rid, "_type": "contact", "_v": 1, "name": name, "title": name})
    for name in new_categories:
        rid = new_id()
        category_ids[name] = rid
        entity_records.append({"_deleted": False, "_id": rid, "_type": "category", "_v": 1, "title": name})
    for name in new_tags:
        rid = new_id()
        tag_ids[name] = rid
        entity_records.append({"_deleted": False, "_id": rid, "_type": "tag", "_v": 1, "title": name})

    if entity_records:
        path = write_transaction(tx_client_dir, idx, entity_records, now, device_id, dry_run)
        action = "would write" if dry_run else "wrote"
        print(f"   {action}: {path.name} ({len(entity_records)} entity records)")
        idx += 1

    with open("/tmp/entity_ids.json", "w", encoding="utf-8") as f:
        json.dump({"contacts": contact_ids, "categories": category_ids, "tags": tag_ids},
                  f, ensure_ascii=False, indent=2)
    print(f"   💾 Entity IDs saved to /tmp/entity_ids.json")

    # Phase B: Update doc entries with metadata from CSV
    print(f"\n{prefix}Phase B: Updating {len(matches)} doc entries...")

    written = errors = 0
    for i, match in enumerate(matches):
        contact_id = contact_ids.get(match.get("vendor", ""))
        category_id = category_ids.get(match.get("category", ""))
        all_tags = list(match.get("tags_raw", []))
        if db_tag and db_tag not in all_tags:
            all_tags.append(db_tag)
        tag_ids_for_entry = {tag_ids[t]: True for t in all_tags if t in tag_ids}

        amount = match["amount"]
        is_credit = amount < 0
        record = {
            "_deleted": False,
            "_id": match["rs_id"],
            "_type": "doc",
            "_v": 20,  # High version to override RS OCR results
            "currency": currency,
            "gross": abs(amount),
            "credit": is_credit,
            "tags": tag_ids_for_entry,
        }
        if match.get("rs_date"):
            record["date"] = match["rs_date"]
        if match.get("posted"):
            record["paymentDate"] = match["posted"]
        if match.get("notes"):
            record["notes"] = match["notes"][:2000]
        if contact_id:
            record["contact"] = contact_id
        if category_id:
            record["category"] = category_id

        try:
            path = write_transaction(tx_client_dir, idx + i, [record], now, device_id, dry_run)
            action = "would write" if dry_run else "wrote"
            if dry_run or i < 5 or i % 500 == 0:
                curr = match.get("rs_current_gross", "?")
                print(f"   {action}: {match['filename']} "
                      f"{currency} {curr} → {currency} {abs(amount):.2f} "
                      f"contact={match.get('vendor', '?')} cat={match.get('category', '?')}")
            written += 1
        except Exception as e:
            print(f"   ❌ {match['filename']}: {e}")
            errors += 1

    action = "Would write" if dry_run else "Wrote"
    print(f"\n{action}: {written} doc updates, Errors: {errors}")

    if not dry_run:
        print("\n✅ NEXT STEPS:")
        print("  1. Receipts Space should pick up the changes automatically.")
        print("  2. If entries don't appear updated, quit RS, delete the cache:")
        print("     ~/Library/Application Support/de.holtwick.mac.homebrew.Receipts2/data/<workspaceId>*")
        print("     Then reopen Receipts Space — it will rebuild from the transaction files.")
        print("  3. Verify amounts, dates, contacts and categories on a few entries.")


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Migrate Paperless (ReceiptWallet) receipts into Receipts Space",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--csv", required=True, help="Path to Paperless CSV export")
    parser.add_argument("--library", required=True, help="Path to Receipts Space library")
    parser.add_argument("--client-id", required=True, help="Client ID (largest folder in transactions/)")
    parser.add_argument("--currency", default="EUR", help="Currency code: EUR, USD, AUD, etc. (default: EUR)")
    parser.add_argument("--db-tag", default="", help="Tag added to every entry for filtering (e.g. 'MyLibrary')")
    parser.add_argument("--dry-run", action="store_true", help="Preview without writing files")
    args = parser.parse_args()

    # Step 1: Build lookup from CSV
    lookup, _ = build_lookup(args.csv)
    with open("/tmp/lookup.json", "w", encoding="utf-8") as f:
        json.dump(lookup, f, ensure_ascii=False, indent=2)
    print("💾 Lookup saved to /tmp/lookup.json")

    library_path = args.library
    if not (Path(library_path) / "transactions").exists():
        print(f"\n❌ Cannot find 'transactions' inside: {library_path}")
        sys.exit(1)

    # Read device ID from library info.json
    info_path = Path(library_path) / "info.json"
    device_id = args.client_id
    if info_path.exists():
        try:
            info = json.loads(info_path.read_text(encoding="utf-8"))
            device_id = info.get("createDeviceId", args.client_id)
            print(f"   Device ID: {device_id}")
        except Exception:
            pass

    # Step 2: Collect entities
    contacts_needed, categories_needed, tags_needed = collect_entities(lookup, args.db_tag)
    print(f"\n   Entities needed: {len(contacts_needed)} contacts, "
          f"{len(categories_needed)} categories, {len(tags_needed)} tags")

    # Step 3: Read library
    doc_entries, existing_contacts, existing_categories, existing_tags = read_library(library_path)

    # Step 4: Match
    matches = match_entries(lookup, doc_entries)
    with open("/tmp/matches.json", "w", encoding="utf-8") as f:
        json.dump(matches, f, ensure_ascii=False, indent=2)
    print("💾 Matches saved to /tmp/matches.json")

    if not matches:
        print("\n❌ No matches found.")
        sys.exit(1)

    # Step 5: Write
    write_all(
        matches, library_path, args.client_id, device_id,
        contacts_needed, categories_needed, tags_needed,
        existing_contacts, existing_categories, existing_tags,
        dry_run=args.dry_run,
        db_tag=args.db_tag,
        currency=args.currency,
    )


if __name__ == "__main__":
    main()
