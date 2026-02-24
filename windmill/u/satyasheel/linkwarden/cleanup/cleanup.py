#!/usr/bin/env python3
"""Linkwarden Cleanup - Check dead links, stale content, URL duplicates, and freshness"""

import requests
import json
import time
import os
from datetime import datetime, timedelta
from urllib.parse import urlparse, urlunparse, parse_qs, urlencode
from collections import defaultdict
from functools import wraps

# Global configuration
LINKWARDEN_URL = ""
LINKWARDEN_API_KEY = ""
INBOX_COLLECTION_ID = None  # Will be auto-detected by finding "Unorganized" collection

# Cleanup thresholds (configurable via env vars)
STALE_DAYS = 365  # Links older than this with no description/tags are considered stale
DEAD_LINK_TIMEOUT = 10  # Seconds to wait for link response
CHECK_FRESHNESS_FOR_DOMAINS = ["github.com", "medium.com", "dev.to", "stackoverflow.com"]

# --- Retry decorator ---

def retry_with_backoff(max_retries=2, base_delay=1):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except requests.exceptions.RequestException as e:
                    if attempt == max_retries - 1:
                        raise
                    delay = base_delay * (2 ** attempt)
                    time.sleep(delay)
            return None
        return wrapper
    return decorator

# --- Linkwarden API helpers ---

def _lw_headers(with_json=False):
    h = {"Authorization": f"Bearer {LINKWARDEN_API_KEY}"}
    if with_json: h["Content-Type"] = "application/json"
    return h

@retry_with_backoff(max_retries=2)
def _lw_get(path, params=None, timeout=10):
    r = requests.get(f"{LINKWARDEN_URL}{path}", headers=_lw_headers(), params=params, timeout=timeout)
    r.raise_for_status()
    return r.json()

@retry_with_backoff(max_retries=2)
def _lw_put(path, body, timeout=30):
    r = requests.put(f"{LINKWARDEN_URL}{path}", headers=_lw_headers(True), json=body, timeout=timeout)
    r.raise_for_status()
    return r.json()

@retry_with_backoff(max_retries=2)
def _lw_delete(path, timeout=10):
    r = requests.delete(f"{LINKWARDEN_URL}{path}", headers=_lw_headers(), timeout=timeout)
    r.raise_for_status()

def get_collections():
    return _lw_get("/api/v1/collections")['response']

def delete_link(link_id):
    _lw_delete(f"/api/v1/links/{link_id}")

# --- URL normalization ---

_TRACKING_PARAMS = {
    'utm_source', 'utm_medium', 'utm_campaign', 'utm_term', 'utm_content',
    'ref', 'fbclid', 'gclid', 'mc_cid', 'mc_eid', 'source',
}

def normalize_url(url):
    if not url: return url
    parsed = urlparse(url.strip())
    scheme = 'https'
    host = (parsed.hostname or '').lower()
    if host.startswith('www.'): host = host[4:]
    port = parsed.port
    netloc = f"{host}:{port}" if port and port != 443 else host
    query = parse_qs(parsed.query, keep_blank_values=True)
    clean_query = urlencode({k: v for k, v in query.items() if k.lower() not in _TRACKING_PARAMS}, doseq=True)
    path = parsed.path.rstrip('/') or '/'
    return urlunparse((scheme, netloc, path, parsed.params, clean_query, ''))

# --- Paginated link fetching ---

def get_links_paginated(collection_id=None):
    all_links, cursor, page = [], None, 0
    while True:
        params = {}
        if collection_id is not None: params['collectionId'] = collection_id
        if cursor is not None: params['cursor'] = cursor

        data = _lw_get("/api/v1/search", params=params, timeout=15)
        search_data = data.get('data', {})
        links = search_data.get('links', [])
        all_links.extend(links)
        page += 1

        next_cursor = search_data.get('nextCursor')
        if not next_cursor or not links: break
        cursor = next_cursor
        time.sleep(0.3)

    if page > 1: print(f"   📄 Fetched {len(all_links)} links across {page} pages")
    return all_links

def get_all_links():
    return get_links_paginated()

def find_collection(colls, **kw):
    key, val = next(iter(kw.items()))
    for c in colls:
        if key == 'name' and c.get('name', '').lower() == val.lower(): return c
        elif c.get(key) == val: return c
    return None

# --- Cleanup Feature 1: Dead Link Checker ---

def check_link_status(url):
    """Check if a link is accessible. Returns (status_code, is_dead, reason)"""
    try:
        # Use HEAD request first (faster)
        r = requests.head(
            url,
            timeout=DEAD_LINK_TIMEOUT,
            allow_redirects=True,
            headers={'User-Agent': 'Mozilla/5.0 (compatible; LinkwardenCleanup/1.0)'}
        )
        status = r.status_code

        # If HEAD not allowed, try GET
        if status == 405:
            r = requests.get(
                url,
                timeout=DEAD_LINK_TIMEOUT,
                allow_redirects=True,
                headers={'User-Agent': 'Mozilla/5.0 (compatible; LinkwardenCleanup/1.0)'},
                stream=True
            )
            status = r.status_code

        if status == 404:
            return status, True, "Not Found (404)"
        elif status == 403:
            return status, True, "Forbidden (403)"
        elif status == 410:
            return status, True, "Gone (410)"
        elif status >= 500:
            return status, True, f"Server Error ({status})"
        elif status >= 400:
            return status, True, f"Client Error ({status})"
        else:
            return status, False, "OK"

    except requests.exceptions.SSLError:
        return None, True, "SSL Certificate Error"
    except requests.exceptions.Timeout:
        return None, True, "Timeout"
    except requests.exceptions.ConnectionError:
        return None, True, "Connection Error"
    except requests.exceptions.TooManyRedirects:
        return None, True, "Too Many Redirects"
    except Exception as e:
        return None, False, f"Unknown Error: {str(e)[:50]}"

def check_dead_links(all_links, collections):
    """Scan all links and identify dead ones"""
    print("\n🔍 Feature 1: Dead Link Checker")
    print("=" * 60)

    dead_links = []
    checked = 0

    for link in all_links:
        url = link.get('url', '')
        if not url:
            continue

        checked += 1
        print(f"[{checked}/{len(all_links)}] Checking: {link.get('name', 'Untitled')[:50]}")

        status, is_dead, reason = check_link_status(url)

        if is_dead:
            col = find_collection(collections, id=link.get('collectionId'))
            col_name = col['name'] if col else 'Unknown'

            dead_links.append({
                'id': link['id'],
                'name': link.get('name', 'Untitled'),
                'url': url,
                'collection': col_name,
                'status': status,
                'reason': reason,
                'created': link.get('createdAt', '')
            })

            print(f"   ☠️  DEAD: {reason}")
        else:
            print(f"   ✅ OK ({status or 'N/A'})")

        time.sleep(0.5)  # Be nice to servers

    return dead_links

# --- Cleanup Feature 2: Stale Link Cleanup ---

def check_stale_links(all_links, collections):
    """Identify old links that haven't been enriched"""
    print("\n📅 Feature 2: Stale Link Checker")
    print("=" * 60)

    stale_links = []
    cutoff_date = datetime.now() - timedelta(days=STALE_DAYS)

    for link in all_links:
        created_at = link.get('createdAt', '')
        if not created_at:
            continue

        try:
            created_date = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
        except:
            continue

        if created_date > cutoff_date:
            continue

        # Check if link is "underdeveloped"
        has_description = bool(link.get('description', '').strip())
        has_tags = len(link.get('tags', [])) > 0
        in_inbox = link.get('collectionId') == INBOX_COLLECTION_ID

        if not has_description and not has_tags and not in_inbox:
            col = find_collection(collections, id=link.get('collectionId'))
            col_name = col['name'] if col else 'Unknown'

            age_days = (datetime.now() - created_date).days

            stale_links.append({
                'id': link['id'],
                'name': link.get('name', 'Untitled'),
                'url': link.get('url', ''),
                'collection': col_name,
                'created': created_at,
                'age_days': age_days
            })

    print(f"   Found {len(stale_links)} stale links (>{STALE_DAYS} days old, no description/tags)")

    return stale_links

# --- Cleanup Feature 5: URL Normalization Audit ---

def check_url_duplicates(all_links, collections):
    """Find links that point to same content with different URLs"""
    print("\n🔗 Feature 5: URL Normalization Audit")
    print("=" * 60)

    url_groups = defaultdict(list)

    # Group by normalized URL
    for link in all_links:
        url = link.get('url', '')
        if url:
            norm_url = normalize_url(url)
            url_groups[norm_url].append(link)

    # Find duplicates
    duplicates = []
    for norm_url, links in url_groups.items():
        if len(links) > 1:
            # Sort by creation date, keep oldest
            links.sort(key=lambda l: l.get('createdAt', ''))
            keeper = links[0]

            keeper_col = find_collection(collections, id=keeper.get('collectionId'))

            dup_info = {
                'normalized_url': norm_url,
                'keeper': {
                    'id': keeper['id'],
                    'name': keeper.get('name', 'Untitled'),
                    'url': keeper.get('url', ''),
                    'collection': keeper_col['name'] if keeper_col else 'Unknown',
                    'created': keeper.get('createdAt', '')
                },
                'duplicates': []
            }

            for dup in links[1:]:
                dup_col = find_collection(collections, id=dup.get('collectionId'))
                dup_info['duplicates'].append({
                    'id': dup['id'],
                    'name': dup.get('name', 'Untitled'),
                    'url': dup.get('url', ''),
                    'collection': dup_col['name'] if dup_col else 'Unknown',
                    'created': dup.get('createdAt', '')
                })

            duplicates.append(dup_info)

    print(f"   Found {len(duplicates)} URL groups with duplicates")

    return duplicates

# --- Cleanup Feature 6: Content Freshness Check ---

def check_content_freshness(all_links, collections):
    """Check if content has been updated/changed since saving"""
    print("\n🔄 Feature 6: Content Freshness Checker")
    print("=" * 60)

    freshness_issues = []
    checked = 0

    for link in all_links:
        url = link.get('url', '')
        if not url:
            continue

        # Only check specific domains to save time
        parsed = urlparse(url)
        domain = (parsed.hostname or '').lower()
        if domain.startswith('www.'):
            domain = domain[4:]

        if domain not in CHECK_FRESHNESS_FOR_DOMAINS:
            continue

        checked += 1
        print(f"[{checked}] Checking freshness: {link.get('name', 'Untitled')[:50]}")

        # GitHub-specific checks
        if domain == "github.com":
            # Check if repo is archived or deleted
            try:
                r = requests.get(
                    url,
                    timeout=5,
                    headers={'User-Agent': 'Mozilla/5.0'},
                    allow_redirects=False
                )

                if r.status_code == 404:
                    col = find_collection(collections, id=link.get('collectionId'))
                    freshness_issues.append({
                        'id': link['id'],
                        'name': link.get('name', 'Untitled'),
                        'url': url,
                        'collection': col['name'] if col else 'Unknown',
                        'issue': 'GitHub repo not found (deleted or private)',
                        'severity': 'high'
                    })
                    print(f"   ⚠️  Repo not found")
                elif 'archived' in r.text.lower() or 'this repository has been archived' in r.text.lower():
                    col = find_collection(collections, id=link.get('collectionId'))
                    freshness_issues.append({
                        'id': link['id'],
                        'name': link.get('name', 'Untitled'),
                        'url': url,
                        'collection': col['name'] if col else 'Unknown',
                        'issue': 'GitHub repo is archived',
                        'severity': 'medium'
                    })
                    print(f"   📦 Repo is archived")
                else:
                    print(f"   ✅ OK")

            except Exception as e:
                print(f"   ⚠️  Could not check: {e}")

        time.sleep(0.5)

    print(f"   Checked {checked} links, found {len(freshness_issues)} issues")

    return freshness_issues

# --- Action executor ---

def execute_cleanup_actions(dead_links, stale_links, duplicates, freshness_issues, dry_run=True):
    """Execute cleanup actions based on findings"""
    print("\n🔧 Cleanup Actions")
    print("=" * 60)

    if dry_run:
        print("   🔍 DRY RUN MODE - No actual changes will be made")

    actions = {
        'deleted_dead': 0,
        'deleted_stale': 0,
        'deleted_duplicates': 0,
        'tagged_issues': 0
    }

    # Action 1: Delete dead links (high confidence)
    if dead_links:
        print(f"\n   Dead links to delete: {len(dead_links)}")
        for link in dead_links:
            if link['reason'] in ['Not Found (404)', 'Gone (410)']:  # High confidence
                print(f"      🗑️  Deleting: {link['name'][:50]} ({link['reason']})")
                if not dry_run:
                    try:
                        delete_link(link['id'])
                        actions['deleted_dead'] += 1
                        time.sleep(0.5)
                    except Exception as e:
                        print(f"         ❌ Failed: {e}")

    # Action 2: Tag stale links (don't auto-delete)
    if stale_links and len(stale_links) > 0:
        print(f"\n   Stale links found: {len(stale_links)}")
        print("   💡 Suggestion: Review these manually or re-organize them")
        for link in stale_links[:5]:  # Show first 5
            print(f"      📅 {link['name'][:50]} ({link['age_days']} days old)")

    # Action 3: Delete duplicates
    if duplicates:
        print(f"\n   Duplicate URL groups: {len(duplicates)}")
        for dup_group in duplicates:
            print(f"      🔗 {dup_group['normalized_url'][:60]}")
            print(f"         Keeping: {dup_group['keeper']['name'][:40]} in {dup_group['keeper']['collection']}")
            for dup in dup_group['duplicates']:
                print(f"         🗑️  Deleting: {dup['name'][:40]} in {dup['collection']}")
                if not dry_run:
                    try:
                        delete_link(dup['id'])
                        actions['deleted_duplicates'] += 1
                        time.sleep(0.5)
                    except Exception as e:
                        print(f"            ❌ Failed: {e}")

    # Action 4: Report freshness issues
    if freshness_issues:
        print(f"\n   Freshness issues found: {len(freshness_issues)}")
        for issue in freshness_issues:
            severity_icon = "🔴" if issue['severity'] == 'high' else "🟡"
            print(f"      {severity_icon} {issue['name'][:50]}")
            print(f"         Issue: {issue['issue']}")

    return actions

# --- Main ---

def main(
    linkwarden_url: str = None,
    linkwarden_api_key: str = None,
    dry_run: bool = True,
    check_dead_links_enabled: bool = True,
    check_stale_links_enabled: bool = True,
    check_url_duplicates_enabled: bool = True,
    check_freshness_enabled: bool = True,
    stale_days: int = 365,
    dead_link_timeout: int = 10
):
    """Windmill-compatible entry point for Linkwarden Cleanup"""
    global LINKWARDEN_URL, LINKWARDEN_API_KEY, STALE_DAYS, DEAD_LINK_TIMEOUT

    LINKWARDEN_URL = linkwarden_url or os.getenv("LINKWARDEN_URL", "http://192.168.10.29:3000")
    LINKWARDEN_API_KEY = linkwarden_api_key or os.getenv("LINKWARDEN_API_KEY", "")
    STALE_DAYS = stale_days
    DEAD_LINK_TIMEOUT = dead_link_timeout

    if not LINKWARDEN_API_KEY:
        raise ValueError("LINKWARDEN_API_KEY is required")

    print("🧹 Linkwarden Cleanup Tool")
    print(f"   Linkwarden: {LINKWARDEN_URL}")
    print(f"   Mode: {'DRY RUN' if dry_run else 'LIVE'}")
    print()

    # Fetch all data
    print("📚 Fetching collections and links...")
    collections = get_collections()
    all_links = get_all_links()
    print(f"   Found {len(all_links)} total links")

    # Run cleanup checks
    dead_links = []
    stale_links = []
    duplicates = []
    freshness_issues = []

    if check_dead_links_enabled:
        dead_links = check_dead_links(all_links, collections)

    if check_stale_links_enabled:
        stale_links = check_stale_links(all_links, collections)

    if check_url_duplicates_enabled:
        duplicates = check_url_duplicates(all_links, collections)

    if check_freshness_enabled:
        freshness_issues = check_content_freshness(all_links, collections)

    # Execute cleanup actions
    actions = execute_cleanup_actions(dead_links, stale_links, duplicates, freshness_issues, dry_run)

    # Summary
    print("\n📊 Cleanup Summary")
    print("=" * 60)
    print(f"   Dead links found: {len(dead_links)}")
    print(f"   Stale links found: {len(stale_links)}")
    print(f"   Duplicate groups: {len(duplicates)}")
    print(f"   Freshness issues: {len(freshness_issues)}")
    print(f"\n   Actions taken:")
    print(f"   - Deleted dead links: {actions['deleted_dead']}")
    print(f"   - Deleted duplicates: {actions['deleted_duplicates']}")

    return {
        'dead_links': len(dead_links),
        'stale_links': len(stale_links),
        'duplicate_groups': len(duplicates),
        'freshness_issues': len(freshness_issues),
        'actions': actions,
        'dry_run': dry_run
    }

if __name__ == "__main__":
    main(
        linkwarden_url=os.getenv("LINKWARDEN_URL"),
        linkwarden_api_key=os.getenv("LINKWARDEN_API_KEY")
    )
