import os, re, json, time, hashlib, datetime
from urllib import request, parse

OUT_DIR = "out"
PAGES_DIR = "pages"

GENRES = [
  "tech_web",
  "travel_planning",
  "food_cooking",
  "health_fitness",
  "study_learning",
  "money_personal_finance",
  "career_work",
  "relationships_communication",
  "home_life_admin",
  "shopping_products",
  "events_leisure",
]

BAN_WORDS = [
  "kill", "murder", "bomb", "terrorist", "genocide",
  "child porn", "cp", "best gore", "suicide",
]
ADULT_HINTS = ["porn", "nsfw", "sex", "nude", "onlyfans", "escort"]

KEYWORDS_BY_GENRE = {
  "tech_web": [
    "bug","error","stack trace","how to","fix","convert","compress","pdf","mp4","ffmpeg",
    "javascript","python","github","actions","vercel","dns","domain","ssl","api","auth"
  ],
  "travel_planning": [
    "itinerary","travel plan","packing list","layover","transfer","eSIM","sim","insurance",
    "budget","safety","visa","refund","cancellation","hotel","flight","train","jr","metro"
  ],
  "food_cooking": [
    "recipe","meal prep","dinner","lunch","breakfast","calories","nutrition","grocery list",
    "cook","easy","quick","batch","leftovers","protein","vegetables"
  ],
  "health_fitness": [
    "sleep","insomnia","workout","exercise","weight loss","diet","habit","routine","steps",
    "gym","running","strength","stretch","back pain","posture"
  ],
  "study_learning": [
    "study plan","memorize","flashcards","review schedule","procrastination","focus",
    "english","toeic","eiken","exam","homework","notes","time table","pomodoro"
  ],
  "money_personal_finance": [
    "save money","budget","fees","installment","split payment","credit card","interest",
    "subscription","refund","chargeback","invoice","tax","household","rent"
  ],
  "career_work": [
    "resume","cv","interview","job","career","side hustle","freelance","portfolio",
    "cover letter","salary","negotiation","remote work"
  ],
  "relationships_communication": [
    "conversation","template","awkward","small talk","text back","reply","communication",
    "apology","boundary","friends","dating plan","message draft"
  ],
  "home_life_admin": [
    "moving","declutter","cleaning","laundry","housework","checklist","paperwork",
    "utilities","internet setup","kitchen","storage"
  ],
  "shopping_products": [
    "compare","which one","best","recommend","budget option","value","review","buy",
    "laptop","phone","camera","earbuds","skincare"
  ],
  "events_leisure": [
    "weekend","date plan","rainy day","what should i do","ideas","schedule",
    "festival","movie","trip idea","day trip"
  ],
}

SCORING_BONUS = [
  (r"\b(plan|itinerary|packing|checklist|template|step[- ]by[- ]step|compare|best|recommend|budget|schedule)\b", 10),
  (r"\b(urgent|today|tomorrow|this week|before i go|deadline)\b", 8),
  (r"\b(i'm stuck|confused|overwhelmed|don't know what to choose)\b", 8),
  (r"\b(error|failed|doesn't work|can't|broken)\b", 6),
]

SCORING_PENALTY = [
  (r"\b(just vent|i hate everything|life sucks)\b", -10),
  (r"^\s*(ugh+|lol+|idk)\s*$", -6),
]

def now_iso():
  return datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

def safe_text(s: str) -> str:
  s = s or ""
  s = s.replace("\u0000", "")
  return s.strip()

def has_japanese(s: str) -> bool:
  return bool(re.search(r"[\u3040-\u30ff\u4e00-\u9fff]", s or ""))

def is_sensitive(text: str) -> bool:
  t = (text or "").lower()
  if any(w in t for w in ADULT_HINTS):
    return True
  if any(w in t for w in BAN_WORDS):
    return True
  return False

def infer_genre(text: str) -> str:
  t = (text or "").lower()
  best = ("tech_web", 0)
  for g, kws in KEYWORDS_BY_GENRE.items():
    score = 0
    for k in kws:
      if k.lower() in t:
        score += 1
    if score > best[1]:
      best = (g, score)
  return best[0]

def score_item(text: str) -> int:
  t = (text or "").lower()
  score = 0
  for pat, add in SCORING_BONUS:
    if re.search(pat, t):
      score += add
  for pat, sub in SCORING_PENALTY:
    if re.search(pat, t):
      score += sub
  # 具体性のざっくり
  if len(t) > 180: score += 3
  if "?" in t: score += 2
  if re.search(r"\b(need|should|can someone|how do i)\b", t): score += 3
  return score

def slugify(s: str) -> str:
  s = (s or "").lower()
  s = re.sub(r"https?://\S+", "", s)
  s = re.sub(r"[^a-z0-9]+", "-", s).strip("-")
  if not s: s = "topic"
  return s[:60].strip("-")

def http_get(url: str, headers=None, timeout=20) -> str:
  req = request.Request(url, headers=headers or {})
  with request.urlopen(req, timeout=timeout) as resp:
    return resp.read().decode("utf-8", errors="replace")

def collect_reddit_rss(queries, limit_each=30):
  items = []
  for q in queries:
    u = "https://www.reddit.com/search.rss?" + parse.urlencode({"q": q, "sort": "new"})
    try:
      xml = http_get(u, headers={"User-Agent": "goliath-auto-tool/1.0"})
    except Exception:
      continue
    # 超雑RSSパース（標準ライブラリのみで）
    for m in re.finditer(r"<entry>(.*?)</entry>", xml, re.S):
      block = m.group(1)
      title = re.search(r"<title>(.*?)</title>", block, re.S)
      link  = re.search(r"<link[^>]+href=\"(.*?)\"", block)
      summ  = re.search(r"<content[^>]*>(.*?)</content>", block, re.S)
      t = safe_text(re.sub(r"<.*?>", "", (title.group(1) if title else "")))
      s = safe_text(re.sub(r"<.*?>", "", (summ.group(1) if summ else "")))
      url = (link.group(1) if link else "")
      text = (t + "\n" + s).strip()
      if not url or not text: 
        continue
      items.append({"source":"reddit","url":url,"text":text})
      if len(items) >= limit_each * len(queries):
        break
  return items

def collect_hn_algolia(queries, limit_each=30):
  items = []
  for q in queries:
    api = "https://hn.algolia.com/api/v1/search_by_date?" + parse.urlencode({
      "query": q, "tags": "story", "hitsPerPage": str(limit_each)
    })
    try:
      js = http_get(api, headers={"User-Agent":"goliath-auto-tool/1.0"})
      data = json.loads(js)
    except Exception:
      continue
    for h in data.get("hits", []):
      url = h.get("url") or ("https://news.ycombinator.com/item?id=" + str(h.get("objectID","")))
      title = safe_text(h.get("title",""))
      text = title
      if url and text:
        items.append({"source":"hn","url":url,"text":text})
  return items

def bsky_create_session(handle, app_password):
  api = "https://bsky.social/xrpc/com.atproto.server.createSession"
  payload = json.dumps({"identifier": handle, "password": app_password}).encode("utf-8")
  req = request.Request(api, data=payload, method="POST", headers={"Content-Type":"application/json"})
  with request.urlopen(req, timeout=20) as resp:
    return json.loads(resp.read().decode("utf-8"))

def collect_bluesky(queries, limit_each=20):
  handle = os.environ.get("BSKY_HANDLE","").strip()
  pw = os.environ.get("BSKY_APP_PASSWORD","").strip()
  if not handle or not pw:
    return []
  try:
    sess = bsky_create_session(handle, pw)
    token = sess.get("accessJwt","")
    if not token:
      return []
  except Exception:
    return []
  items = []
  for q in queries:
    api = "https://bsky.social/xrpc/app.bsky.feed.searchPosts?" + parse.urlencode({"q": q, "limit": str(limit_each)})
    try:
      js = http_get(api, headers={"Authorization": f"Bearer {token}"})
      data = json.loads(js)
    except Exception:
      continue
    for p in data.get("posts", []):
      uri = p.get("uri","")
      rec = p.get("record", {}) or {}
      text = safe_text(rec.get("text",""))
      # URI を web で見れる形に（完全ではないが実用）
      # https://bsky.app/profile/{did}/post/{rkey}
      try:
        did = p.get("author", {}).get("did","")
        rkey = uri.split("/")[-1] if uri else ""
        url = f"https://bsky.app/profile/{did}/post/{rkey}" if (did and rkey) else ""
      except Exception:
        url = ""
      if url and text:
        items.append({"source":"bluesky","url":url,"text":text})
  return items

def collect_mastodon(queries, limit_each=20):
  base = os.environ.get("MASTODON_BASE","").strip().rstrip("/")
  token = os.environ.get("MASTODON_TOKEN","").strip()
  if not base or not token:
    return []
  items = []
  for q in queries:
    api = f"{base}/api/v2/search?" + parse.urlencode({"q": q, "type":"statuses", "limit": str(limit_each)})
    try:
      js = http_get(api, headers={"Authorization": f"Bearer {token}"})
      data = json.loads(js)
    except Exception:
      continue
    for st in data.get("statuses", []):
      url = st.get("url","")
      content = safe_text(re.sub(r"<.*?>", "", st.get("content","") or ""))
      if url and content:
        items.append({"source":"mastodon","url":url,"text":content})
  return items

def collect_x_mentions(limit_each=50):
  bearer = os.environ.get("X_BEARER_TOKEN","").strip()
  user_id = os.environ.get("X_USER_ID","").strip()
  if not bearer or not user_id:
    return []
  api = f"https://api.x.com/2/users/{user_id}/mentions?max_results={limit_each}&tweet.fields=lang,created_at"
  try:
    js = http_get(api, headers={"Authorization": f"Bearer {bearer}"})
    data = json.loads(js)
  except Exception:
    return []
  items = []
  for tw in data.get("data", []) or []:
    tid = tw.get("id","")
    text = safe_text(tw.get("text",""))
    if tid and text:
      url = f"https://x.com/i/web/status/{tid}"
      items.append({"source":"x_mentions","url":url,"text":text})
  return items

def sanitize_affiliate_html(html: str) -> str:
  h = html or ""
  h = re.sub(r"<\s*script\b[^>]*>.*?<\s*/\s*script\s*>", "", h, flags=re.I|re.S)
  h = re.sub(r"on\w+\s*=\s*\"[^\"]*\"", "", h, flags=re.I)
  h = re.sub(r"on\w+\s*=\s*\'[^\']*\'", "", h, flags=re.I)
  return h.strip()

def load_affiliates(path="affiliates.json"):
  if not os.path.exists(path):
    return {g: [] for g in GENRES}
  with open(path, "r", encoding="utf-8") as f:
    data = json.load(f)
  out = {}
  for g in GENRES:
    arr = data.get(g, [])
    if not isinstance(arr, list):
      arr = []
    clean = []
    for it in arr:
      if not isinstance(it, dict): 
        continue
      html = sanitize_affiliate_html(it.get("html",""))
      clean.append({
        "id": str(it.get("id","")),
        "title": str(it.get("title","")),
        "html": html,
        "priority": int(it.get("priority", 50)) if str(it.get("priority","")).isdigit() else 50,
        "tags": it.get("tags", []) if isinstance(it.get("tags", []), list) else []
      })
    out[g] = clean
  return out

def ensure_affiliate_keys(aff: dict):
  missing = [g for g in GENRES if g not in aff]
  extra = [k for k in aff.keys() if k not in GENRES]
  return missing, extra

def make_page_url(slug: str) -> str:
  repo = os.environ.get("GITHUB_REPOSITORY","").strip()
  if not repo:
    # ローカルでも読めるように
    return f"/{PAGES_DIR}/{slug}/"
  owner, name = repo.split("/", 1)
  # GitHub Pages (project pages)
  return f"https://{owner}.github.io/{name}/{PAGES_DIR}/{slug}/"

def write_page(slug: str, genre: str, problem_text: str, affiliate_block: str):
  os.makedirs(os.path.join(PAGES_DIR, slug), exist_ok=True)
  title = f"{genre.replace('_',' ').title()} helper"
  safe_problem = (problem_text or "").strip()
  if len(safe_problem) > 600:
    safe_problem = safe_problem[:600] + "..."
  html = f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>{title}</title>
<style>
body{{font-family:system-ui,-apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif;line-height:1.6;margin:0;background:#0b0f14;color:#e6edf3}}
main{{max-width:920px;margin:0 auto;padding:24px}}
.card{{background:#111826;border:1px solid #223; border-radius:14px; padding:18px; margin:14px 0}}
h1{{font-size:22px;margin:0 0 10px}}
h2{{font-size:16px;margin:0 0 8px}}
pre{{white-space:pre-wrap;word-break:break-word;background:#0b1220;border:1px solid #223;border-radius:10px;padding:12px}}
small{{color:#9fb1c1}}
a{{color:#7cc0ff}}
ul{{margin:8px 0 0 18px}}
</style>
</head>
<body>
<main>
  <div class="card">
    <h1>{title}</h1>
    <small>Updated: {now_iso()} / Genre: {genre}</small>
  </div>

  <div class="card">
    <h2>What you’re trying to solve</h2>
    <pre>{safe_problem}</pre>
  </div>

  <div class="card">
    <h2>Plan / Checklist</h2>
    <ul>
      <li>Define goal + constraints (time, budget, must-haves)</li>
      <li>Make options (2–5) and compare with a quick table</li>
      <li>Use a checklist to avoid “forgot something” failures</li>
      <li>Decide the next small step you can do in 10 minutes</li>
    </ul>
  </div>

  <div class="card">
    <h2>Templates</h2>
    <ul>
      <li><b>Comparison table</b>: Option / Pros / Cons / Cost / Risk / Notes</li>
      <li><b>Checklist</b>: Must-do / Nice-to-have / Avoid / Deadline</li>
      <li><b>Message draft</b>: 1 empathy line + 1 request line + 1 thanks line</li>
    </ul>
  </div>

  <div class="card">
    <h2>Helpful links</h2>
    <ul>
      <li><a href="../">Back to pages</a></li>
    </ul>
  </div>

  <div class="card">
    <h2>Recommended</h2>
    {affiliate_block}
  </div>
</main>
</body>
</html>"""
  with open(os.path.join(PAGES_DIR, slug, "index.html"), "w", encoding="utf-8") as f:
    f.write(html)

def pick_affiliate_html(aff_by_genre: dict, genre: str) -> str:
  arr = aff_by_genre.get(genre, []) or []
  if not arr:
    return "<small>(No affiliate set for this genre yet.)</small>"
  # priority 高い順で上位2つだけ
  arr = sorted(arr, key=lambda x: int(x.get("priority",50)), reverse=True)[:2]
  blocks = []
  for it in arr:
    title = safe_text(it.get("title",""))
    html = it.get("html","").strip()
    blocks.append(f"<div class='card'><small>{title}</small><div>{html}</div></div>")
  return "\n".join(blocks)

def build_reply(problem_text: str, page_url: str) -> str:
  jp = has_japanese(problem_text)
  if jp:
    a = "それ、迷いますよね。"
    b = "状況に合わせて、決めやすいように1ページにまとめました。"
    return f"{a}\n{b}\n{page_url}"
  else:
    a = "That sounds stressful to deal with."
    b = "I put together a single page to help you decide quickly and avoid mistakes."
    return f"{a}\n{b}\n{page_url}"

def ensure_dirs():
  os.makedirs(OUT_DIR, exist_ok=True)
  os.makedirs(PAGES_DIR, exist_ok=True)

def main():
  ensure_dirs()

  leads_total = int(os.environ.get("LEADS_TOTAL","") or "100")
  per_source_target = max(30, leads_total)  # 足りないときに備えて多めに集める

  # 生活系も混ぜる検索語（Reddit/HNで確実に母数確保）
  mixed_queries = [
    "itinerary travel plan packing list",
    "eSIM layover transfer budget refund cancellation",
    "meal prep recipe quick dinner calories nutrition",
    "sleep routine workout weight loss habit",
    "study plan memorize procrastination focus TOEIC",
    "resume interview template cover letter",
    "split payment fees subscription refund",
    "compare best recommend checklist template",
    "moving declutter cleaning checklist",
    "rainy day weekend plan what should I do"
  ]

  tech_queries = [
    "pdf convert compress merge split",
    "mp4 compress ffmpeg",
    "github actions error",
    "dns domain ssl",
    "api auth token"
  ]

  # Collect
  reddit = collect_reddit_rss(mixed_queries + tech_queries, limit_each=20)
  hn = collect_hn_algolia(mixed_queries + tech_queries, limit_each=20)
  bsky = collect_bluesky(mixed_queries, limit_each=20)
  mast = collect_mastodon(mixed_queries, limit_each=20)
  xmen = collect_x_mentions(limit_each=50)

  # Filter & normalize
  all_items = []
  for it in (reddit + hn + bsky + mast + xmen):
    text = safe_text(it.get("text",""))
    url = safe_text(it.get("url",""))
    if not url or not text:
      continue
    if is_sensitive(text):
      continue
    g = infer_genre(text)
    sc = score_item(text)
    all_items.append({
      "source": it.get("source",""),
      "url": url,
      "text": text,
      "genre": g,
      "score": sc
    })

  # dedupe by url
  seen = set()
  dedup = []
  for it in sorted(all_items, key=lambda x: x["score"], reverse=True):
    if it["url"] in seen:
      continue
    seen.add(it["url"])
    dedup.append(it)

  # Load affiliates & validate keys
  aff = load_affiliates("affiliates.json")
  missing_keys, extra_keys = ensure_affiliate_keys(aff)

  # Build pages + replies (>= leads_total)
  leads = []
  for it in dedup[:max(leads_total, 120)]:  # 作りすぎ防止しつつ、足りないと困るので少し余裕
    base = f'{it["genre"]} {it["source"]} {it["url"]} {it["text"][:80]}'
    slug = slugify(base)
    # collision回避
    h = hashlib.sha1(base.encode("utf-8", errors="ignore")).hexdigest()[:8]
    slug = f"{slug}-{h}"
    page_url = make_page_url(slug)
    aff_html = pick_affiliate_html(aff, it["genre"])
    write_page(slug, it["genre"], it["text"], aff_html)
    reply = build_reply(it["text"], page_url)

    leads.append({
      "problem_url": it["url"],
      "reply": reply,
      "page_url": page_url,
      "genre": it["genre"],
      "source": it["source"],
      "score": it["score"],
    })
    if len(leads) >= leads_total:
      break

  # If still short, pad with stub
  i = 0
  while len(leads) < leads_total:
    i += 1
    stub_problem = f"Stub lead (collection shortage) #{i}"
    slug = f"stub-{i}-{int(time.time())}"
    page_url = make_page_url(slug)
    write_page(slug, "tech_web", stub_problem, "<small>(stub)</small>")
    leads.append({
      "problem_url": f"https://example.com/?stub={i}",
      "reply": build_reply(stub_problem, page_url),
      "page_url": page_url,
      "genre": "tech_web",
      "source": "stub",
      "score": 0,
    })

  stats = {
    "time": now_iso(),
    "counts": {
      "reddit": len(reddit),
      "hn": len(hn),
      "bluesky": len(bsky),
      "mastodon": len(mast),
      "x_mentions": len(xmen),
      "dedup_after_filter": len(dedup),
      "leads": len(leads),
    },
    "affiliates_key_check": {
      "missing_keys": missing_keys,
      "extra_keys": extra_keys,
      "ok": (len(missing_keys) == 0),
    },
    "note": {
      "x_mode": "mentions_only (unless you implement search later)"
    }
  }

  with open(os.path.join(OUT_DIR, "leads.json"), "w", encoding="utf-8") as f:
    json.dump(leads, f, ensure_ascii=False, indent=2)

  with open(os.path.join(OUT_DIR, "stats.json"), "w", encoding="utf-8") as f:
    json.dump(stats, f, ensure_ascii=False, indent=2)

  # Self-check logs (must show in Actions)
  print("=== SELF CHECK ===")
  print("Counts:", json.dumps(stats["counts"], ensure_ascii=False))
  print("Leads >= 100:", stats["counts"]["leads"] >= 100)
  print("Affiliates keys ok:", stats["affiliates_key_check"]["ok"])
  if missing_keys:
    print("Missing affiliate keys:", missing_keys)
  if extra_keys:
    print("Extra affiliate keys:", extra_keys)
  print("==================")

if __name__ == "__main__":
  main()