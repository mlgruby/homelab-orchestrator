#!/usr/bin/env python3
"""AI Librarian - Categorize Linkwarden bookmarks using Gemini AI"""

import requests, json, re, time, os
from functools import wraps
from html.parser import HTMLParser
from urllib.parse import urlparse, urlunparse, parse_qs, urlencode
from collections import defaultdict

# Global configuration - set by main()
LINKWARDEN_URL = ""
LINKWARDEN_API_KEY = ""
GEMINI_API_KEY = ""
GEMINI_MODEL = "gemini-2.5-flash"
GEMINI_BATCH_MAX_LINKS = 25
GEMINI_BATCH_MAX_PROMPT_CHARS = 45000
GEMINI_USAGE = {'prompt': 0, 'output': 0, 'total': 0, 'calls': 0}
GEMINI_LOG_RAW_JSON = True
INBOX_COLLECTION_ID = 31

# --- Retry decorator ---

def retry_with_backoff(max_retries=3, base_delay=2):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except requests.exceptions.RequestException as e:
                    status = getattr(getattr(e, 'response', None), 'status_code', None)
                    body = ""
                    try: body = e.response.text[:500] if e.response else ""
                    except: pass

                    print(f"   ❗ [{func.__name__}] Failed (status: {status or 'N/A'})")
                    if body: print(f"   ❗ Response: {body}")

                    if attempt == max_retries - 1:
                        print(f"   ❌ [{func.__name__}] All {max_retries} attempts exhausted")
                        raise

                    delay = base_delay * (2 ** attempt) * (2 if status == 429 else 1)
                    label = "Rate limited" if status == 429 else "Retrying"
                    print(f"   🔄 {label}. Waiting {delay}s (attempt {attempt + 1}/{max_retries})...")
                    time.sleep(delay)
            return None
        return wrapper
    return decorator

# --- Linkwarden API helpers ---

def _lw_headers(with_json=False):
    h = {"Authorization": f"Bearer {LINKWARDEN_API_KEY}"}
    if with_json: h["Content-Type"] = "application/json"
    return h

@retry_with_backoff(max_retries=3)
def _lw_get(path, params=None, timeout=10):
    r = requests.get(f"{LINKWARDEN_URL}{path}", headers=_lw_headers(), params=params, timeout=timeout)
    r.raise_for_status()
    return r.json()

@retry_with_backoff(max_retries=3)
def _lw_post(path, body, timeout=10):
    r = requests.post(f"{LINKWARDEN_URL}{path}", headers=_lw_headers(True), json=body, timeout=timeout)
    r.raise_for_status()
    return r.json()

@retry_with_backoff(max_retries=3)
def _lw_put(path, body, timeout=30):
    r = requests.put(f"{LINKWARDEN_URL}{path}", headers=_lw_headers(True), json=body, timeout=timeout)
    r.raise_for_status()
    return r.json()

@retry_with_backoff(max_retries=3)
def _lw_delete(path, timeout=10):
    r = requests.delete(f"{LINKWARDEN_URL}{path}", headers=_lw_headers(), timeout=timeout)
    r.raise_for_status()

def get_collections(): return _lw_get("/api/v1/collections")['response']
def get_tags(): return _lw_get("/api/v1/tags")['response']
def delete_link(link_id): _lw_delete(f"/api/v1/links/{link_id}")

# --- URL normalization ---

_TRACKING_PARAMS = {
    'utm_source', 'utm_medium', 'utm_campaign', 'utm_term', 'utm_content',
    'ref', 'fbclid', 'gclid', 'mc_cid', 'mc_eid',
}

def normalize_url(url):
    if not url: return url
    parsed = urlparse(url.strip())
    scheme = 'https'  # always normalize to https
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

def get_unorganized_links(): return get_links_paginated(collection_id=INBOX_COLLECTION_ID)
def get_all_links(): return get_links_paginated()

def build_url_lookup(all_links):
    return {normalize_url(l.get('url')): l for l in all_links if l.get('collectionId') != INBOX_COLLECTION_ID}

# --- Collection helpers ---

def find_collection(colls, **kw):
    key, val = next(iter(kw.items()))
    for c in colls:
        if key == 'name' and c.get('name', '').lower() == val.lower(): return c
        elif c.get(key) == val: return c
    return None

def create_collection(name, description=""):
    print(f"   🆕 Creating new collection '{name}'...")
    result = _lw_post("/api/v1/collections", {"name": name, "description": description, "color": "#7dd3fc"})
    print(f"   ✅ Collection created!")
    return result['response']

# --- Deduplication ---

def deduplicate_across_collections(all_links, colls):
    url_groups = defaultdict(list)
    for link in all_links:
        if link.get('collectionId') != INBOX_COLLECTION_ID:
            url_groups[normalize_url(link.get('url'))].append(link)

    dup_count = 0
    for url, copies in url_groups.items():
        if len(copies) <= 1: continue
        copies.sort(key=lambda l: l.get('createdAt', ''), reverse=True)
        keeper = copies[0]
        keeper_col = find_collection(colls, id=keeper.get('collectionId'))
        print(f"   🔗 Duplicate URL: {url[:80]}")
        print(f"      Keeping in: '{keeper_col['name'] if keeper_col else 'Unknown'}' (newest)")

        for dup in copies[1:]:
            dup_col = find_collection(colls, id=dup.get('collectionId'))
            print(f"      🗑️  Removing from: '{dup_col['name'] if dup_col else 'Unknown'}'")
            try:
                delete_link(dup['id'])
                dup_count += 1
            except requests.exceptions.RequestException as e:
                print(f"      ❌ Failed to delete: {e}")
            time.sleep(0.5)
    return dup_count

# --- Meta tag fetching ---

class MetaTagParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.meta = {}
    def handle_starttag(self, tag, attrs):
        if tag != 'meta': return
        a = dict(attrs)
        prop = a.get('property', '')
        if prop in ('og:description', 'og:title', 'og:site_name') and a.get('content'):
            self.meta[prop] = a['content']
        if a.get('name', '').lower() == 'description' and a.get('content'):
            self.meta['description'] = a['content']

def fetch_meta_description(url):
    try:
        r = requests.get(url, timeout=5, headers={'User-Agent': 'Mozilla/5.0 (compatible; AI-Librarian/1.0)'}, stream=True)
        chunk = next(r.iter_content(chunk_size=50000), b"")
        parser = MetaTagParser()
        parser.feed(chunk.decode('utf-8', errors='ignore'))

        desc = parser.meta.get('og:description', parser.meta.get('description', ''))
        og_title = parser.meta.get('og:title', '')
        parts = []
        if og_title: parts.append(f"Page Title: {og_title}")
        if desc: parts.append(f"Page Summary: {desc}")
        result = "; ".join(parts)
        print(f"   📄 {'Fetched meta: ' + result[:120] + '...' if result else 'No meta description found'}")
        return result
    except Exception as e:
        print(f"   📄 Could not fetch meta (skipping): {e}")
        return ""

def _is_x_url(url):
    try:
        host = (urlparse(url).hostname or "").lower()
        if host.startswith("www."):
            host = host[4:]
        return host in ("x.com", "twitter.com", "mobile.twitter.com")
    except Exception:
        return False

def _extract_x_status_info(url):
    try:
        parsed = urlparse(url)
        parts = [p for p in (parsed.path or "").split("/") if p]
        if len(parts) >= 3 and parts[1].lower() == "status":
            handle = parts[0].lstrip("@")
            status_id = parts[2]
            return handle, status_id
    except Exception:
        pass
    return "", ""

def _is_generic_x_description(text):
    t = (text or "").strip().lower()
    if not t:
        return True
    generic_markers = [
        "a social media post from x",
        "formerly twitter",
        "pending a detailed content review",
        "pending review"
    ]
    return any(marker in t for marker in generic_markers)

def _shorten(text, max_chars=500):
    if not text:
        return ""
    text = re.sub(r"\s+", " ", str(text)).strip()
    if len(text) <= max_chars:
        return text
    return text[:max_chars - 1] + "…"

def fetch_x_post_context(url):
    """Best-effort fetch for X/Twitter status metadata and text."""
    if not _is_x_url(url):
        return {}

    handle, status_id = _extract_x_status_info(url)
    result = {'handle': handle, 'statusId': status_id}

    if not status_id:
        return result

    try:
        api_url = f"https://cdn.syndication.twimg.com/tweet-result?id={status_id}&lang=en"
        r = requests.get(api_url, timeout=6, headers={'User-Agent': 'Mozilla/5.0 (compatible; AI-Librarian/1.0)'})
        r.raise_for_status()
        data = r.json()

        text = _shorten(data.get('text', ''), 700)
        user = data.get('user', {}) if isinstance(data.get('user', {}), dict) else {}
        screen_name = user.get('screen_name') or handle
        author_name = user.get('name', '')

        if screen_name:
            result['handle'] = screen_name
            result['title'] = f"X post by @{screen_name}"
        elif handle:
            result['title'] = f"X post by @{handle}"
        else:
            result['title'] = f"X post {status_id}"

        if text:
            result['description'] = text
            context_parts = [f"Post Text: {text}"]
            if author_name or screen_name:
                who = f"{author_name} (@{screen_name})" if author_name and screen_name else f"@{screen_name or handle}"
                context_parts.append(f"Author: {who}")
            result['context'] = "; ".join(context_parts)
        else:
            result['context'] = f"X status id: {status_id}"

        print(f"   🐦 Fetched X post context for status {status_id}")
        return result
    except Exception as e:
        print(f"   🐦 Could not fetch X post context (using URL fallback): {e}")
        fallback = [f"X status id: {status_id}"] if status_id else []
        if handle:
            fallback.append(f"Author handle: @{handle}")
            result['title'] = f"X post by @{handle}"
        if fallback:
            result['context'] = "; ".join(fallback)
        return result

# --- Gemini AI categorization ---

def _get_int_env(name, default, min_value=1):
    raw = os.getenv(name, str(default))
    try:
        value = int(raw)
    except ValueError:
        print(f"   ⚠️  Invalid {name}='{raw}', using default {default}")
        return default
    if value < min_value:
        print(f"   ⚠️  {name}={value} is too low, using minimum {min_value}")
        return min_value
    return value

def _get_bool_env(name, default=True):
    raw = os.getenv(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in ("1", "true", "yes", "y", "on")

def _safe_int(value):
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0

def _record_gemini_usage(data, label="call"):
    usage = data.get('usageMetadata') or {}
    prompt_tokens = _safe_int(usage.get('promptTokenCount'))
    output_tokens = _safe_int(usage.get('candidatesTokenCount') or usage.get('outputTokenCount'))
    total_tokens = _safe_int(usage.get('totalTokenCount'))
    if total_tokens == 0 and (prompt_tokens or output_tokens):
        total_tokens = prompt_tokens + output_tokens

    if usage:
        GEMINI_USAGE['prompt'] += prompt_tokens
        GEMINI_USAGE['output'] += output_tokens
        GEMINI_USAGE['total'] += total_tokens
        GEMINI_USAGE['calls'] += 1
        print(f"   📊 Gemini tokens ({label}): prompt={prompt_tokens}, output={output_tokens}, total={total_tokens}")
    else:
        print(f"   📊 Gemini tokens ({label}): usage metadata unavailable")

def _log_api_json(label, data):
    if not GEMINI_LOG_RAW_JSON:
        return
    try:
        print(f"   🧾 Gemini raw JSON ({label}):")
        print(json.dumps(data, indent=2, ensure_ascii=False))
    except Exception as e:
        print(f"   ⚠️  Could not pretty-print Gemini JSON ({label}): {e}")

PROMPT_TEMPLATE = """You are an AI Librarian organizing bookmarks.

Here are the links to categorize:
{links_json}

Available Collections (STRONGLY prefer using one of these):
{collections_json}

Available Tags (use these when relevant):
{tags_json}

Analyze every link and respond with ONLY a valid JSON object (no markdown, no explanation):
{{
  "results": [
    {{
      "linkId": 123,
      "collectionName": "collection name",
      "tags": ["tag1", "tag2", "tag3"],
      "description": "brief description",
      "isNewCollection": false
    }}
  ]
}}

CRITICAL Rules for Collections:
- NEVER suggest "Unorganized" as a collection - that is the inbox, not a real category
- You MUST always assign a meaningful collection name
- FIRST, check if each link GENUINELY fits an existing collection's theme
- DO NOT force-fit links into unrelated collections - accuracy matters more than reusing collections
- If a link's topic is clearly different from ALL existing collections (90%+ confidence), create a new one
- Only interpret collections broadly if the content is actually related (e.g., "Python tutorial" fits in "Pyhton" collection)
- DO NOT put unrelated topics together (e.g., cooking ≠ hiking ≠ finance)
- If suggesting a new collection, set "isNewCollection": true
- If using an existing collection, use the EXACT name from the list above and set "isNewCollection": false

Rules for Tags:
- Use existing tags when relevant, create new ones if needed
- Keep 2-4 tags maximum

Rules for Description:
- Keep description concise (1-2 sentences)
- For X/Twitter links, avoid generic placeholders; use specific post details when available

Output constraints:
- Return exactly one result entry for every input linkId
- Do not omit any input linkId
- Keep linkId values unchanged"""

SINGLE_PROMPT_TEMPLATE = """You are an AI Librarian organizing bookmarks.

Here is the link to categorize:
{link_context}

Available Collections (STRONGLY prefer using one of these):
{collections_json}

Available Tags (use these when relevant):
{tags_json}

Analyze this link and respond with ONLY a valid JSON object (no markdown, no explanation):
{{
  "collectionName": "collection name",
  "tags": ["tag1", "tag2", "tag3"],
  "description": "brief description",
  "isNewCollection": false
}}

CRITICAL Rules for Collections:
- NEVER suggest "Unorganized" as a collection - that is the inbox, not a real category
- You MUST always assign a meaningful collection name
- FIRST, check if this link GENUINELY fits an existing collection's theme
- DO NOT force-fit links into unrelated collections - accuracy matters more than reusing collections
- If the link's topic is clearly different from ALL existing collections (90%+ confidence), create a new one
- Only interpret collections broadly if the content is actually related (e.g., "Python tutorial" fits in "Pyhton" collection)
- DO NOT put unrelated topics together (e.g., cooking ≠ hiking ≠ finance)
- If suggesting a new collection, set "isNewCollection": true
- If using an existing collection, use the EXACT name from the list above and set "isNewCollection": false

Rules for Tags:
- Use existing tags when relevant, create new ones if needed
- Keep 2-4 tags maximum

Rules for Description:
- Keep description concise (1-2 sentences)
- For X/Twitter links, avoid generic placeholders; use specific post details when available"""

def _estimate_prompt_chars(link_contexts, collection_names, tag_names):
    return len(PROMPT_TEMPLATE.format(
        links_json=json.dumps(link_contexts, indent=2),
        collections_json=json.dumps(collection_names, indent=2),
        tags_json=json.dumps(tag_names, indent=2)
    ))

def split_link_contexts_into_batches(link_contexts, collection_names, tag_names):
    batches, current = [], []
    for ctx in link_contexts:
        tentative = current + [ctx]
        too_many_links = len(tentative) > GEMINI_BATCH_MAX_LINKS
        too_large_prompt = _estimate_prompt_chars(tentative, collection_names, tag_names) > GEMINI_BATCH_MAX_PROMPT_CHARS

        if current and (too_many_links or too_large_prompt):
            batches.append(current)
            current = [ctx]
        else:
            current = tentative

    if current:
        batches.append(current)
    return batches

@retry_with_backoff(max_retries=3, base_delay=3)
def categorize_batch_with_gemini(link_contexts, collection_names, tag_names):
    prompt = PROMPT_TEMPLATE.format(
        links_json=json.dumps(link_contexts, indent=2),
        collections_json=json.dumps(collection_names, indent=2),
        tags_json=json.dumps(tag_names, indent=2)
    )

    print(f"   🤖 Calling Gemini ({GEMINI_MODEL})...")
    api_url = f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent"
    r = requests.post(api_url, headers={"Content-Type": "application/json", "x-goog-api-key": GEMINI_API_KEY},
                      json={"contents": [{"parts": [{"text": prompt}]}]}, timeout=30)
    r.raise_for_status()
    data = r.json()
    _log_api_json(f"batch:{len(link_contexts)}", data)
    _record_gemini_usage(data, label=f"batch:{len(link_contexts)}")

    if not data.get('candidates'):
        raise ValueError(f"Gemini returned no candidates: {json.dumps(data)[:300]}")

    text = data['candidates'][0]['content']['parts'][0]['text']
    match = re.search(r'\{[\s\S]*\}', text)
    if match:
        parsed = json.loads(match.group(0))
        if isinstance(parsed, dict) and isinstance(parsed.get('results'), list):
            return parsed
        if isinstance(parsed, list):
            return {'results': parsed}
        raise ValueError(f"Unexpected Gemini JSON shape: {type(parsed).__name__}")
    raise ValueError(f"Could not extract JSON from Gemini response: {text[:300]}")

@retry_with_backoff(max_retries=3, base_delay=3)
def categorize_single_with_gemini(link_context, collection_names, tag_names):
    context_lines = [
        f"- URL: {link_context['url']}",
        f"- Title: {link_context['title']}",
        f"- Description: {link_context.get('description', 'No description')}"
    ]
    if link_context.get('pageContent'):
        context_lines.append(f"- Page Content: {link_context['pageContent']}")

    prompt = SINGLE_PROMPT_TEMPLATE.format(
        link_context="\n".join(context_lines),
        collections_json=json.dumps(collection_names, indent=2),
        tags_json=json.dumps(tag_names)
    )

    print(f"   🤖 Calling Gemini ({GEMINI_MODEL})...")
    api_url = f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent"
    r = requests.post(api_url, headers={"Content-Type": "application/json", "x-goog-api-key": GEMINI_API_KEY},
                      json={"contents": [{"parts": [{"text": prompt}]}]}, timeout=30)
    r.raise_for_status()
    data = r.json()
    _log_api_json("single", data)
    _record_gemini_usage(data, label="single")

    if not data.get('candidates'):
        raise ValueError(f"Gemini returned no candidates: {json.dumps(data)[:300]}")

    text = data['candidates'][0]['content']['parts'][0]['text']
    match = re.search(r'\{[\s\S]*\}', text)
    if match:
        parsed = json.loads(match.group(0))
        if isinstance(parsed, dict):
            return parsed
        raise ValueError(f"Unexpected Gemini JSON shape: {type(parsed).__name__}")
    raise ValueError(f"Could not extract JSON from Gemini response: {text[:300]}")

def categorize_with_auto_batching(link_contexts, collection_names, tag_names):
    """Run categorization in one batch when possible, auto-splitting on size/failure."""
    if not link_contexts:
        return {}, {}

    initial_batches = split_link_contexts_into_batches(link_contexts, collection_names, tag_names)
    print(f"   🧠 Gemini batching: {len(link_contexts)} links -> {len(initial_batches)} batch(es)")

    pending = list(initial_batches)
    results_by_id, failed_by_id = {}, {}

    while pending:
        batch = pending.pop(0)
        requested_ids = {str(item.get('linkId')) for item in batch if item.get('linkId') is not None}

        if len(batch) == 1:
            single_ctx = batch[0]
            single_id = str(single_ctx.get('linkId'))
            try:
                print(f"   🤖 Processing AI single-link prompt for link {single_id}...")
                single_result = categorize_single_with_gemini(single_ctx, collection_names, tag_names)
                single_result['linkId'] = single_ctx.get('linkId')
                results_by_id[single_id] = single_result
            except Exception as e:
                failed_by_id[single_id] = f"Gemini single-link call failed: {e}"
                print(f"   ❌ Single-link AI call failed for link {single_id}: {e}")
            continue

        try:
            print(f"   🤖 Processing AI batch with {len(batch)} link(s)...")
            batch_result = categorize_batch_with_gemini(batch, collection_names, tag_names)

            returned_ids = set()
            for item in batch_result.get('results', []):
                link_id = item.get('linkId')
                if link_id is None:
                    continue
                key = str(link_id)
                results_by_id[key] = item
                returned_ids.add(key)

            missing_ids = requested_ids - returned_ids
            for missing_id in missing_ids:
                failed_by_id[missing_id] = "Gemini omitted this linkId in batch response"

        except Exception as e:
            status = getattr(getattr(e, 'response', None), 'status_code', None)
            can_split = isinstance(e, ValueError) or (status in (400, 413))

            if len(batch) <= 1 or not can_split:
                reason = f"Gemini batch failed: {e}"
                for failed_id in requested_ids:
                    failed_by_id[failed_id] = reason
                print(f"   ❌ Batch failed for {len(requested_ids)} link(s): {e}")
                continue

            mid = len(batch) // 2
            left, right = batch[:mid], batch[mid:]
            print(f"   ⚠️  Batch failed ({len(batch)} links). Splitting into {len(left)} + {len(right)} and retrying...")
            pending.insert(0, right)
            pending.insert(0, left)

    return results_by_id, failed_by_id

# --- Link update ---

@retry_with_backoff(max_retries=3)
def update_link(link_id, collection, tags, description):
    print(f"   🔄 Fetching current link data...")
    link_data = _lw_get(f"/api/v1/links/{link_id}")['response']
    link_data.update({
        'collectionId': collection['id'],
        'collection': collection,
        'tags': [{"name": tag} for tag in tags],
        'description': description
    })
    print(f"   🔄 Updating link {link_id} to collection '{collection['name']}'...")
    result = _lw_put(f"/api/v1/links/{link_id}", link_data)
    print(f"   ✅ Response: 200")
    return result

# --- Main ---

def main(
    linkwarden_url: str = "",
    linkwarden_api_key: str = "",
    gemini_api_key: str = "",
    gemini_model: str = "gemini-2.5-flash"
):
    """Windmill-compatible entry point for AI Librarian."""
    global LINKWARDEN_URL, LINKWARDEN_API_KEY, GEMINI_API_KEY, GEMINI_MODEL, GEMINI_BATCH_MAX_LINKS, GEMINI_BATCH_MAX_PROMPT_CHARS, GEMINI_USAGE, GEMINI_LOG_RAW_JSON

    LINKWARDEN_URL = linkwarden_url or os.getenv("LINKWARDEN_URL", "http://192.168.10.29:3000")
    LINKWARDEN_API_KEY = linkwarden_api_key or os.getenv("LINKWARDEN_API_KEY", "")
    GEMINI_API_KEY = gemini_api_key or os.getenv("GEMINI_API_KEY", "")
    GEMINI_MODEL = gemini_model or os.getenv("GEMINI_MODEL", "gemini-2.5-flash")
    GEMINI_BATCH_MAX_LINKS = _get_int_env("GEMINI_BATCH_MAX_LINKS", 25, min_value=1)
    GEMINI_BATCH_MAX_PROMPT_CHARS = _get_int_env("GEMINI_BATCH_MAX_PROMPT_CHARS", 45000, min_value=2000)
    GEMINI_LOG_RAW_JSON = _get_bool_env("GEMINI_LOG_RAW_JSON", True)
    GEMINI_USAGE = {'prompt': 0, 'output': 0, 'total': 0, 'calls': 0}

    if not LINKWARDEN_API_KEY: raise ValueError("LINKWARDEN_API_KEY is required")
    if not GEMINI_API_KEY: raise ValueError("GEMINI_API_KEY is required")

    print("🤖 AI Librarian starting...")
    print(f"   Linkwarden: {LINKWARDEN_URL} | Model: {GEMINI_MODEL}")

    # Check inbox first - skip everything if empty
    print(f"📥 Checking Unorganized collection...")
    links = get_unorganized_links()
    print(f"   Found {len(links)} links to organize")

    if not links:
        print("✅ Inbox is empty! Skipping.")
        return {'total': 0, 'organized': 0, 'duplicates': 0, 'failed': 0, 'new_collections': 0, 'message': 'Inbox is empty'}

    # Fetch context
    print("📚 Fetching collections and tags...")
    collections = get_collections()
    tags = get_tags()
    collection_names = [c['name'] for c in collections if c['id'] != INBOX_COLLECTION_ID]
    tag_names = [t['name'] for t in tags]
    print(f"   Found {len(collections)} collections and {len(tags)} tags")

    # Build duplicate index
    print(f"🔍 Building duplicate detection index...")
    all_links = get_all_links()
    url_lookup = build_url_lookup(all_links)
    print(f"   Indexed {len(url_lookup)} existing links")

    # Cross-collection dedup (piggybacks on data already fetched)
    print(f"🔍 Checking for cross-collection duplicates...")
    cross_dups = deduplicate_across_collections(all_links, collections)
    if cross_dups > 0:
        print(f"   🧹 Removed {cross_dups} cross-collection duplicates")
        all_links = get_all_links()
        url_lookup = build_url_lookup(all_links)
    else:
        print(f"   ✅ No cross-collection duplicates found")

    # Inbox self-dedup
    seen_urls, deduped, inbox_dups = {}, [], 0
    for link in links:
        norm = normalize_url(link.get('url'))
        if norm in seen_urls:
            print(f"   🗑️  Inbox duplicate: {link['name']} (keeping first copy)")
            try: delete_link(link['id']); inbox_dups += 1
            except: pass
            time.sleep(0.5)
        else:
            seen_urls[norm] = True
            deduped.append(link)
    if inbox_dups > 0: print(f"   🧹 Removed {inbox_dups} inbox duplicates")
    links = deduped

    stats = {'total': len(links), 'organized': 0, 'duplicates': inbox_dups + cross_dups, 'failed': 0, 'new_collections': 0}

    # Pre-filter duplicates and build AI batch inputs
    ai_candidates, link_contexts = [], []
    for i, link in enumerate(links, 1):
        print(f"\n[{i}/{len(links)}] Preparing: {link['name']}")
        print(f"   URL: {link['url']}")

        existing = url_lookup.get(normalize_url(link['url']))
        if existing:
            col = find_collection(collections, id=existing.get('collectionId'))
            print(f"   ⚠️  Duplicate! Already in '{col['name'] if col else 'Unknown'}'")
            try: delete_link(link['id']); stats['duplicates'] += 1
            except: stats['failed'] += 1
            time.sleep(1)
            continue

        meta_desc = fetch_meta_description(link['url'])
        x_ctx = fetch_x_post_context(link['url'])
        title = (link.get('name') or "").strip()
        description = (link.get('description') or "").strip()

        if x_ctx:
            if not title and x_ctx.get('title'):
                title = x_ctx['title']
            if _is_generic_x_description(description) and x_ctx.get('description'):
                description = x_ctx['description']
            if x_ctx.get('context'):
                meta_desc = f"{meta_desc}; {x_ctx['context']}" if meta_desc else x_ctx['context']

        if not title:
            parsed = urlparse(link.get('url', ''))
            fallback_host = (parsed.hostname or "unknown-site").replace("www.", "")
            fallback_path = parsed.path.rstrip("/").split("/")[-1] if parsed.path else ""
            title = f"{fallback_host} {fallback_path}".strip()
        if not description:
            description = "No description"

        ai_candidates.append(link)
        link_contexts.append({
            'linkId': link['id'],
            'url': link['url'],
            'title': title,
            'description': description,
            'pageContent': meta_desc
        })

    ai_results_by_id, ai_failed_by_id = {}, {}
    if link_contexts:
        try:
            ai_results_by_id, ai_failed_by_id = categorize_with_auto_batching(link_contexts, collection_names, tag_names)
        except Exception as e:
            print(f"   ❌ Batch Gemini call failed: {e}")
            stats['failed'] += len(ai_candidates)
            ai_candidates = []

    # Process each link using batch AI results
    for i, link in enumerate(ai_candidates, 1):
        print(f"\n[{i}/{len(ai_candidates)}] Processing: {link['name']}")
        try:
            result = ai_results_by_id.get(str(link['id']))
            if not result:
                fail_reason = ai_failed_by_id.get(str(link['id']), "Gemini returned no result for this link")
                print(f"   ❌ {fail_reason} (link id {link['id']})")
                stats['failed'] += 1
                continue

            if not result.get('collectionName') or not result.get('tags') or not result.get('description'):
                print(f"   ❌ Gemini returned incomplete JSON: {json.dumps(result)[:200]}")
                stats['failed'] += 1
                continue

            print(f"   ✨ AI suggests:")
            print(f"      Collection: {result['collectionName']}{' (NEW!)' if result.get('isNewCollection') else ''}")
            print(f"      Tags: {', '.join(result['tags'])}")
            print(f"      Description: {result['description']}")

            if result['collectionName'].lower() in ['unorganized', 'inbox', 'uncategorized', 'unsorted']:
                print(f"   ⚠️  LLM suggested '{result['collectionName']}' - skipping to avoid loop")
                stats['failed'] += 1
                continue

            target = find_collection(collections, name=result['collectionName'])
            if not target:
                if result.get('isNewCollection', False):
                    print(f"   💡 No existing collection fits (90%+ confidence)")
                    try:
                        target = create_collection(result['collectionName'], f"Auto-created for: {link['name']}")
                        collections.append(target)
                        collection_names.append(target['name'])
                        stats['new_collections'] += 1
                    except requests.exceptions.RequestException as e:
                        print(f"   ❌ Failed to create collection: {e}")
                        stats['failed'] += 1
                        continue
                else:
                    print(f"   ⚠️  Collection '{result['collectionName']}' not found (AI typo?). Skipping.")
                    stats['failed'] += 1
                    continue

            try:
                update_link(link['id'], target, result['tags'], result['description'])
                print(f"   ✅ Updated successfully!")
                stats['organized'] += 1
                url_lookup[normalize_url(link['url'])] = link
            except requests.exceptions.RequestException as e:
                print(f"   ❌ Failed to update link: {e}")
                stats['failed'] += 1
                continue

            time.sleep(1)

        except requests.exceptions.RequestException as e:
            print(f"   ❌ API Error: {e}")
            stats['failed'] += 1
            continue
        except Exception as e:
            print(f"   ❌ Unexpected error: {e}")
            import traceback; traceback.print_exc()
            stats['failed'] += 1
            continue

    stats['gemini_tokens'] = dict(GEMINI_USAGE)
    print(f"\n🎉 Done! Summary:")
    print(f"   📊 Total: {stats['total']} | ✅ Organized: {stats['organized']} | 🔄 Duplicates: {stats['duplicates']} | 🆕 New collections: {stats['new_collections']} | ❌ Failed: {stats['failed']}")
    print(f"   🤖 Gemini usage: calls={GEMINI_USAGE['calls']}, prompt={GEMINI_USAGE['prompt']}, output={GEMINI_USAGE['output']}, total={GEMINI_USAGE['total']}")
    return stats

if __name__ == "__main__":
    main(linkwarden_url=os.getenv("LINKWARDEN_URL"), linkwarden_api_key=os.getenv("LINKWARDEN_API_KEY"), gemini_api_key=os.getenv("GEMINI_API_KEY"))
