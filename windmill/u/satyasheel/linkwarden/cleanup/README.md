# Linkwarden Cleanup Tool

Automated cleanup for your Linkwarden bookmarks.

## Features

### 1. Dead Link Checker ☠️
- Scans all links and checks HTTP status codes
- Identifies 404s, 403s, 410s, server errors
- Detects SSL certificate errors, timeouts, connection failures
- Auto-deletes high-confidence dead links (404, 410) when `dry_run=false`

### 2. Stale Link Detection 📅
- Finds old links (default: >365 days) with no description or tags
- Helps identify bookmarks that were never properly organized
- Reports only (doesn't auto-delete) - manual review recommended

### 3. URL Normalization Audit 🔗
- Finds duplicate URLs that differ only by:
  - Tracking parameters (utm_*, fbclid, gclid, etc.)
  - www prefix
  - http vs https
  - Trailing slashes
- Keeps oldest bookmark, removes duplicates when `dry_run=false`

### 4. Content Freshness Check 🔄
- Checks if linked content has changed since saving
- GitHub-specific checks:
  - Archived repositories
  - Deleted repositories
  - Private repositories (now inaccessible)
- Reports issues for manual review

## Configuration

### Schedule
- **Default**: `0 0 * * 0` (Weekly on Sunday at midnight)
- **Status**: `enabled: false` (disabled by default)
- **Important**: Set `dry_run: true` for initial runs!

### Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `dry_run` | `true` | If true, only reports issues without making changes |
| `check_dead_links_enabled` | `true` | Enable dead link checking |
| `check_stale_links_enabled` | `true` | Enable stale link detection |
| `check_url_duplicates_enabled` | `true` | Enable URL duplicate detection |
| `check_freshness_enabled` | `true` | Enable content freshness checks |
| `stale_days` | `365` | Links older than this are considered stale |
| `dead_link_timeout` | `10` | Timeout (seconds) for checking links |

## Usage

### First Run (Dry Run Mode)
1. Keep `dry_run: true` in schedule.yaml
2. Set `enabled: true` to activate
3. Review the output to see what would be cleaned
4. Check the logs to ensure it's working correctly

### Enable Live Mode
1. Once you're confident, set `dry_run: false`
2. The tool will:
   - Auto-delete 404/410 dead links
   - Auto-delete duplicate URLs
   - Report (but not delete) stale links and freshness issues

### Manual Run
Run on-demand via Windmill UI to test or do immediate cleanup.

## Safety Features
- Dry run mode by default
- Only auto-deletes high-confidence cases (404, 410, exact duplicates)
- Stale links and freshness issues are reported only
- Retry logic with backoff for API calls
- Configurable timeout to avoid hanging

## Scheduled Timing Recommendation
- Run cleanup **after** the organize script (which runs every 2 hours)
- Weekly cleanup (Sunday midnight) is usually sufficient
- For large libraries, consider running every 2 weeks

## What Gets Deleted Automatically?
When `dry_run: false`:
- ✅ Dead links with 404 or 410 status
- ✅ Duplicate URLs (keeps oldest)
- ❌ NOT deleted: Stale links (manual review needed)
- ❌ NOT deleted: Freshness issues (manual review needed)
- ❌ NOT deleted: SSL errors or timeouts (may be temporary)

## Output
Returns a summary JSON:
```json
{
  "dead_links": 5,
  "stale_links": 12,
  "duplicate_groups": 3,
  "freshness_issues": 2,
  "actions": {
    "deleted_dead": 5,
    "deleted_stale": 0,
    "deleted_duplicates": 3,
    "tagged_issues": 0
  },
  "dry_run": false
}
```
