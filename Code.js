/**
 * Telecom + IT Compliance Tracker
 * Fully updated Code.gs
 *
 * New in this version:
 * 1) Adds ImposedDate to Alerts and emails
 * 2) Adds Accounts sheet support for affected-company mapping
 * 3) Adds AffectedCompanies and AffectedCompanyCount to Alerts
 * 4) Adds setup + backfill helpers
 * 5) Keeps existing reliability, digest, campaign, and YTD logic
 */

const CONFIG = {
  // Sheets
  SOURCES_SHEET: "Sources",
  ALERTS_SHEET: "Alerts",
  CAMPAIGNS_SHEET: "Campaigns",
  ROUTING_SHEET: "Routing",
  WEEKLY_SHEET: "Weekly Summary",
  SOURCE_HEALTH_SHEET: "Source Health",
  ACCOUNTS_SHEET: "Accounts",

  // Email
  EMAIL_TO: "aalavi@sandlerpartners.com",
  EMAIL_FROM_NAME: "Adam A.",
  REPLY_TO: "aalavi@sandlerpartners.com",

  // Processing limits
  SOURCES_PER_RUN: 4,
  MAX_ITEMS_PER_FEED: 8,
  MAX_AFFECTED_COMPANIES_IN_CELL: 12,

  // Behavior
  SEND_EMAIL: true,
  SUBJECT_PREFIX: "[Compliance Alert]",
  DIGEST_SUBJECT_PREFIX: "[Compliance Digest]",
  WEEKLY_SUBJECT_PREFIX: "[Weekly Summary]",
  YTD_SUBJECT_PREFIX: "[YTD Compliance]",
  CAMPAIGN_SUBJECT_PREFIX: "[Campaign Drafts]",

  // Script properties keys
  DIGEST_QUEUE_KEY: "digest_queue_v1",
  CURSOR_KEY: "sources_cursor_v1",

  // Immediate alert severities
  IMMEDIATE_SEVERITIES: new Set(["MANDATE", "FINAL_RULE", "ENFORCEMENT"]),

  // Time limit (ms)
  SOFT_TIME_LIMIT_MS: 4.5 * 60 * 1000,

  // Campaigns
  CAMPAIGN_LOOKBACK_DAYS: 7,
  CAMPAIGN_TOP_ALERTS_PER_INDUSTRY: 3,

  // YTD labels
  YTD_SNAPSHOT_NAME: "[YTD Snapshot]",
  YTD_CAMPAIGN_NAME: "[YTD Campaigns]",

  // Fetch behavior
  FETCH_MAX_RETRIES: 3,
  FETCH_BACKOFF_MS: 1500,
  FETCH_TIMEOUT_MS: 25000,
  MAX_BODY_CHARS: 1500000,

  // JSON parsing
  JSON_MAX_ITEMS: 25
};

/* ======================= MAIN: RUN COMPLIANCE ======================= */

function runComplianceCheck() {
  const startRun = Date.now();
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const srcSheet = ss.getSheetByName(CONFIG.SOURCES_SHEET);
  if (!srcSheet) throw new Error("Missing Sources sheet");

  let alertsSheet = ss.getSheetByName(CONFIG.ALERTS_SHEET);
  if (!alertsSheet) alertsSheet = ss.insertSheet(CONFIG.ALERTS_SHEET);
  ensureAlertsHeaders_(alertsSheet);

  const sources = getSources_(srcSheet);
  if (!sources.length) {
    Logger.log("No enabled sources found.");
    return;
  }

  const props = PropertiesService.getScriptProperties();
  let cursor = parseInt(props.getProperty(CONFIG.CURSOR_KEY) || "0", 10);
  if (isNaN(cursor) || cursor < 0) cursor = 0;
  if (cursor >= sources.length) cursor = 0;

  const matches = [];
  let processed = 0;

  Logger.log("RUN START: sources=" + sources.length + ", cursor=" + cursor + ", batch=" + CONFIG.SOURCES_PER_RUN);

  while (
    processed < CONFIG.SOURCES_PER_RUN &&
    (Date.now() - startRun) < CONFIG.SOFT_TIME_LIMIT_MS
  ) {
    const src = sources[cursor];
    const t0 = Date.now();

    Logger.log("SOURCE START [" + (cursor + 1) + "/" + sources.length + "]: " + src.sourceName + " | URL=" + src.feedUrl);

    try {
      const items = fetchFeedItems_(src, CONFIG.MAX_ITEMS_PER_FEED);
      Logger.log("  " + src.sourceName + ": fetched " + items.length + " item(s).");

      for (const item of items) {
        const dkey = makeDedupeKey_(src.sourceName, item);
        if (isAlreadySeen_(dkey)) continue;

        const m = matchItem_(item, src.keywords, src.industries);
        if (!m.matched) continue;

        markSeen_(dkey);

        const severity = classifySeverity_(item);
        const category = classifyCategory_(item);
        const impact = computeImpactScore_(item, severity);
        const due = extractDueDate_(item);
        const imposedDate = extractImposedDate_(item);
        const sourceIndustriesStr = (src.industries || []).join(", ");
        const techCategories = classifyTechCategories_(item, sourceIndustriesStr, category);

        const affected = findAffectedCompanies_(src.industries || []);
        const affectedCompanies = affected.display;
        const affectedCompanyCount = affected.count;

        const row = [
          new Date(),                        // Timestamp
          severity,                          // Severity
          category,                          // Category
          impact,                            // ImpactScore
          (src.industries || []).length,     // IndustryCoverage
          sourceIndustriesStr,               // SourceIndustries
          imposedDate,                       // ImposedDate
          due,                               // DueDate
          affectedCompanies,                 // AffectedCompanies
          affectedCompanyCount,              // AffectedCompanyCount
          src.sourceName,                    // SourceName
          item.title || "",                  // Title
          item.published || "",              // Published
          item.link || "",                   // Link
          m.matchedKeywords.join(", "),      // MatchedKeywords
          m.matchedIndustries.join(", "),    // MatchedIndustries
          techCategories                     // TechCategories
        ];

        matches.push({
          row,
          item,
          src,
          severity,
          category,
          impact,
          due,
          imposedDate,
          techCategories,
          sourceIndustries: sourceIndustriesStr,
          affectedCompanies,
          affectedCompanyCount,
          matchedKeywords: m.matchedKeywords.join(", "),
          matchedIndustries: m.matchedIndustries.join(", ")
        });
      }
    } catch (err) {
      Logger.log("  ERROR in " + src.sourceName + ": " + err);
    }

    Logger.log("SOURCE END   [" + (cursor + 1) + "/" + sources.length + "]: " + src.sourceName + " (" + (Date.now() - t0) + " ms)");

    cursor++;
    if (cursor >= sources.length) cursor = 0;
    processed++;
  }

  props.setProperty(CONFIG.CURSOR_KEY, String(cursor));

  Logger.log("RUN END: processed=" + processed + ", nextCursor=" + cursor + ", matches=" + matches.length);

  if (!matches.length) return;

  appendAlerts_(alertsSheet, matches.map(m => m.row));

  if (!CONFIG.SEND_EMAIL) return;

  const immediate = matches.filter(m => CONFIG.IMMEDIATE_SEVERITIES.has(m.severity));
  const digestOnly = matches.filter(m => !CONFIG.IMMEDIATE_SEVERITIES.has(m.severity));

  if (immediate.length) {
    Logger.log("Sending immediate email for " + immediate.length + " item(s).");
    sendImmediateEmail_(immediate);
  }
  if (digestOnly.length) {
    Logger.log("Queueing " + digestOnly.length + " item(s) for daily digest.");
    queueDigest_(digestOnly);
  }
}

/* ======================= FEEDS: FETCH & PARSE ======================= */

function fetchFeedItems_(src, max) {
  const candidates = buildFetchCandidates_(src);
  if (!candidates.length) return [];

  Logger.log("  FETCH START: candidates=" + candidates.map(c => c.label).join(", "));

  const res = tryFetchCandidates_(candidates, src);
  if (!res || !res.ok) {
    const status = res ? res.statusCode : "";
    const err = res ? res.errorText : "Fetch failed";
    updateSourceHealth_(src, status, 0, err, false, {
      finalUrl: res ? res.finalUrl : (src.feedUrl || ""),
      snippet: res ? res.snippet : ""
    });
    return [];
  }

  if (res.statusCode === 304) {
    updateSourceHealth_(src, 304, 0, "", true, { finalUrl: res.finalUrl, snippet: "" });
    return [];
  }

  const body = String(res.body || "");
  const contentType = String(res.contentType || "").toLowerCase();

  const looksJson = contentType.indexOf("application/json") !== -1 || looksLikeJson_(body) || String(res.finalUrl || "").toLowerCase().endsWith(".json");
  if (looksJson) {
    try {
      const items = parseJsonFeed_(String(res.finalUrl || src.feedUrl || ""), body).slice(0, max);
      updateSourceHealth_(src, res.statusCode, items.length, "", true, { finalUrl: res.finalUrl, snippet: res.snippet });
      return items;
    } catch (e) {
      updateSourceHealth_(src, res.statusCode, 0, "JSON parse failed: " + String(e), false, { finalUrl: res.finalUrl, snippet: res.snippet });
      return [];
    }
  }

  const head = body.slice(0, 400).toLowerCase();
  const looksXml = head.indexOf("<rss") !== -1 || head.indexOf("<feed") !== -1 || head.indexOf("<?xml") !== -1;
  if (!looksXml) {
    updateSourceHealth_(src, res.statusCode, 0, "Non-XML content", false, { finalUrl: res.finalUrl, snippet: res.snippet });
    return [];
  }

  let xml;
  try {
    xml = XmlService.parse(body);
  } catch (e) {
    updateSourceHealth_(src, res.statusCode, 0, "XML parse failed: " + String(e), false, { finalUrl: res.finalUrl, snippet: res.snippet });
    return [];
  }

  const root = xml.getRootElement();
  const rn = String(root.getName() || "").toLowerCase();

  if (rn === "rss") {
    const ch = root.getChild("channel");
    const items = ch ? ch.getChildren("item") : [];
    const parsed = items.slice(0, max).map(parseRssItem_);
    updateSourceHealth_(src, res.statusCode, parsed.length, "", true, { finalUrl: res.finalUrl, snippet: res.snippet });
    return parsed;
  }

  if (rn === "feed") {
    const ns = root.getNamespace();
    const entries = root.getChildren("entry", ns);
    const parsed = entries.slice(0, max).map(e => parseAtomEntry_(e, ns));
    updateSourceHealth_(src, res.statusCode, parsed.length, "", true, { finalUrl: res.finalUrl, snippet: res.snippet });
    return parsed;
  }

  updateSourceHealth_(src, res.statusCode, 0, "Unknown XML root: <" + rn + ">", false, { finalUrl: res.finalUrl, snippet: res.snippet });
  return [];
}

function buildFetchCandidates_(src) {
  const out = [];

  if (src.feedUrl) out.push({ label: "Primary", url: String(src.feedUrl).trim() });
  if (src.fallbackUrl1) out.push({ label: "Fallback1", url: String(src.fallbackUrl1).trim() });
  if (src.fallbackUrl2) out.push({ label: "Fallback2", url: String(src.fallbackUrl2).trim() });

  const withVariants = [];
  out.forEach(c => {
    const u = String(c.url || "").trim();
    if (!u) return;

    withVariants.push({ label: c.label, url: u });

    if (u.indexOf("http://") === 0) {
      withVariants.push({ label: c.label + "_HTTPS", url: "https://" + u.slice(7) });
    } else if (u.indexOf("https://") === 0) {
      withVariants.push({ label: c.label + "_HTTP", url: "http://" + u.slice(8) });
    }
  });

  const seen = {};
  const deduped = [];
  withVariants.forEach(c => {
    const u = c.url;
    if (!u || seen[u]) return;
    seen[u] = true;
    deduped.push(c);
  });

  return deduped.filter(c => c.url);
}

function tryFetchCandidates_(candidates, src) {
  let last = null;

  for (let i = 0; i < candidates.length; i++) {
    const c = candidates[i];
    Logger.log("  FETCH TRY (" + c.label + "): " + c.url);

    const r = fetchWithRetries_(c.url);
    last = r;

    if (r && r.ok) return r;

    Logger.log("  FETCH FAIL (" + c.label + "): status=" + (r ? r.statusCode : "") + " err=" + (r ? r.errorText : ""));
  }

  return last;
}

function fetchWithRetries_(url) {
  const headers = buildDefaultHeaders_();

  const options = {
    method: "get",
    muteHttpExceptions: true,
    followRedirects: true,
    headers: headers,
    timeout: CONFIG.FETCH_TIMEOUT_MS
  };

  let last = null;

  for (let attempt = 0; attempt < CONFIG.FETCH_MAX_RETRIES; attempt++) {
    try {
      const resp = UrlFetchApp.fetch(url, options);
      const code = resp.getResponseCode();

      const h = resp.getHeaders ? resp.getHeaders() : {};
      const ct = String((h && (h["Content-Type"] || h["content-type"])) || "").toLowerCase();

      const finalUrl = safeGetFinalUrl_(resp, url);

      let body = "";
      try {
        body = resp.getContentText() || "";
      } catch (_) {
        body = "";
      }

      if (body && body.length > CONFIG.MAX_BODY_CHARS) {
        body = body.slice(0, CONFIG.MAX_BODY_CHARS);
      }

      if (code === 304) {
        return { ok: true, statusCode: 304, body: "", finalUrl: finalUrl, contentType: ct, errorText: "", snippet: "" };
      }

      if (code >= 200 && code < 300) {
        return {
          ok: true,
          statusCode: code,
          body: body,
          finalUrl: finalUrl,
          contentType: ct,
          errorText: "",
          snippet: body.slice(0, 350)
        };
      }

      const retryable = (code === 403 || code === 429 || code >= 500);
      const snippet = (body || "").slice(0, 350);

      last = {
        ok: false,
        statusCode: code,
        body: body,
        finalUrl: finalUrl,
        contentType: ct,
        errorText: "HTTP " + code,
        snippet: snippet
      };

      if (retryable && attempt < CONFIG.FETCH_MAX_RETRIES - 1) {
        Utilities.sleep((attempt + 1) * CONFIG.FETCH_BACKOFF_MS);
        continue;
      }

      return last;
    } catch (e) {
      last = { ok: false, statusCode: "", body: "", finalUrl: url, contentType: "", errorText: String(e), snippet: "" };
      if (attempt < CONFIG.FETCH_MAX_RETRIES - 1) {
        Utilities.sleep((attempt + 1) * CONFIG.FETCH_BACKOFF_MS);
        continue;
      }
      return last;
    }
  }

  return last || { ok: false, statusCode: "", body: "", finalUrl: url, contentType: "", errorText: "Fetch failed", snippet: "" };
}

function buildDefaultHeaders_() {
  return {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
    "Accept": "application/rss+xml, application/atom+xml, application/xml;q=0.9, application/json;q=0.9, text/xml;q=0.8, */*;q=0.7",
    "Accept-Language": "en-US,en;q=0.9",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "Upgrade-Insecure-Requests": "1"
  };
}

function safeGetFinalUrl_(resp, fallback) {
  try {
    const all = resp.getAllHeaders ? resp.getAllHeaders() : {};
    if (all && all.Location) return String(all.Location);
  } catch (_) {}
  return fallback;
}

function looksLikeJson_(txt) {
  if (!txt) return false;
  const s = String(txt).trim();
  if (!s) return false;
  return (s[0] === "{" && s.indexOf("}") !== -1) || (s[0] === "[" && s.indexOf("]") !== -1);
}

function parseJsonFeed_(url, body) {
  const data = JSON.parse(body);

  if (String(url || "").indexOf("known_exploited_vulnerabilities.json") !== -1) {
    const vulns = (data && data.vulnerabilities) ? data.vulnerabilities : [];
    return vulns.map(v => {
      const cve = v.cveID || v.cve || "";
      const vendor = v.vendorProject || "";
      const product = v.product || "";
      const nameBits = [cve, vendor, product].filter(Boolean).join(" - ");

      const due = v.dueDate ? "Due Date: " + v.dueDate : "";
      const action = v.requiredAction ? "Required Action: " + v.requiredAction : "";
      const notes = v.notes ? "Notes: " + v.notes : "";
      const known = v.knownRansomwareCampaignUse ? "Ransomware Use: " + v.knownRansomwareCampaignUse : "";

      const description = [
        v.vulnerabilityName ? "Name: " + v.vulnerabilityName : "",
        v.shortDescription ? "Summary: " + v.shortDescription : "",
        known,
        due,
        action,
        notes
      ].filter(Boolean).join("\n");

      const link = "https://www.cisa.gov/known-exploited-vulnerabilities-catalog";

      return {
        title: "CISA KEV: " + (nameBits || (v.vulnerabilityName || "Known Exploited Vulnerability")),
        link: link,
        published: v.dateAdded || "",
        description: description
      };
    });
  }

  if (Array.isArray(data)) {
    return data.slice(0, CONFIG.JSON_MAX_ITEMS).map((x, i) => ({
      title: String(x.title || x.name || ("Item " + (i + 1))),
      link: normalizeLink_(String(x.link || x.url || ""), x),
      published: String(x.published || x.updated || x.date || ""),
      description: String(x.description || x.summary || "")
    }));
  }

  if (data && Array.isArray(data.items)) {
    return data.items.slice(0, CONFIG.JSON_MAX_ITEMS).map((x, i) => ({
      title: String(x.title || x.name || ("Item " + (i + 1))),
      link: normalizeLink_(String(x.link || x.url || ""), x),
      published: String(x.published || x.updated || x.date || ""),
      description: String(x.description || x.summary || "")
    }));
  }

  if (data && Array.isArray(data.entries)) {
    return data.entries.slice(0, CONFIG.JSON_MAX_ITEMS).map((x, i) => ({
      title: String(x.title || x.name || ("Item " + (i + 1))),
      link: normalizeLink_(String(x.link || x.url || ""), x),
      published: String(x.published || x.updated || x.date || ""),
      description: String(x.description || x.summary || "")
    }));
  }

  throw new Error("Unsupported JSON feed structure");
}

/* ======================= XML PARSERS ======================= */

function parseRssItem_(el) {
  return {
    title: text_(el, "title"),
    link: text_(el, "link"),
    published: text_(el, "pubDate") || "",
    description: text_(el, "description") || ""
  };
}

function parseAtomEntry_(el, ns) {
  const title = childText_(el, "title", ns);
  const pub = childText_(el, "published", ns) || childText_(el, "updated", ns) || "";
  let link = "";
  const links = el.getChildren("link", ns) || [];
  if (links.length) {
    const alt = links.find(l => (l.getAttribute("rel") ? l.getAttribute("rel").getValue() : "") === "alternate");
    const chosen = alt || links[0];
    const href = chosen.getAttribute("href");
    link = href ? href.getValue() : "";
  }
  const desc = childText_(el, "summary", ns) || childText_(el, "content", ns) || "";
  return { title: title, link: link, published: pub, description: desc };
}

function text_(p, c) {
  const el = p.getChild(c);
  return el ? (el.getText() || "").trim() : "";
}

function childText_(p, c, ns) {
  const el = p.getChild(c, ns);
  return el ? (el.getText() || "").trim() : "";
}

/* ======================= MATCHING, SEVERITY, CATEGORY ======================= */

function matchItem_(item, keywords, industries) {
  const title = String(item.title || "");
  const desc  = String(item.description || "");
  const hay   = (title + "\n" + desc).toLowerCase();

  const kwList = (keywords || [])
    .map(k => String(k || "").trim())
    .filter(Boolean);

  const matchAll = kwList.some(k => {
    const t = k.toLowerCase();
    return t === "all" || t === "*";
  });

  const mk = matchAll
    ? []
    : kwList.filter(k => hay.includes(k.toLowerCase()));

  const mi = (industries || [])
    .map(i => String(i || "").trim())
    .filter(Boolean)
    .filter(i => hay.includes(i.toLowerCase()));

  const matched = matchAll ? true : (kwList.length ? mk.length > 0 : true);

  return {
    matched,
    matchedKeywords: mk,
    matchedIndustries: mi
  };
}

function classifySeverity_(item) {
  const t = (String(item.title || "") + "\n" + String(item.description || "")).toLowerCase();
  if (containsAny_(t, ["final rule", "finalized", "adopted rule"])) return "FINAL_RULE";
  if (containsAny_(t, ["enforcement", "settlement", "penalty", "fine", "forfeiture", "violation"])) return "ENFORCEMENT";
  if (containsAny_(t, ["mandate", "required", "shall", "deadline", "must", "effective date"])) return "MANDATE";
  if (containsAny_(t, ["proposed rule", "draft", "guidance", "advisory", "framework"])) return "GUIDANCE";
  return "ADVISORY";
}

function classifyCategory_(item) {
  const t = (String(item.title || "") + "\n" + String(item.description || "")).toLowerCase();

  if (containsAny_(t, ["federal communications commission", "fcc", "robocall", "e911", "cpni", "wireless emergency alerts", "eas", "spectrum", "telecommunications"])) {
    return "Telecom";
  }

  if (containsAny_(t, ["voip", "telecom", "e911", "robocall", "cpni", "order", "nal"])) return "Telecom";
  if (containsAny_(t, ["cloud", "saas", "iaas", "paas", "fedramp"])) return "Cloud";
  if (containsAny_(t, ["cyber", "vulnerability", "cve", "ransom", "zero trust", "mfa", "incident"])) return "Security";
  if (containsAny_(t, ["pci", "finra", "interchange", "payments"])) return "Payments";
  if (containsAny_(t, ["privacy", "hipaa", "phi", "glba", "data protection"])) return "Privacy";
  return "IT";
}

function computeImpactScore_(item, severity) {
  const base = ({
    "MANDATE": 5,
    "FINAL_RULE": 4,
    "ENFORCEMENT": 4,
    "GUIDANCE": 2,
    "ADVISORY": 1
  }[severity]) || 1;

  const t = (String(item.title || "") + "\n" + String(item.description || "")).toLowerCase();
  let bump = 0;
  if (containsAny_(t, ["deadline", "effective date", "no later than"])) bump++;
  if (containsAny_(t, ["audit", "certification", "attestation"])) bump++;
  if (containsAny_(t, ["penalty", "fine", "forfeiture"])) bump++;
  return Math.min(5, base + bump);
}

function containsAny_(h, ns) { return (ns || []).some(n => h.indexOf(n) !== -1); }

/* ======================= TECH CATEGORY MAP ======================= */

let TECH_MAP_CACHE = null;

function classifyTechCategories_(item, sourceIndustriesStr, category) {
  if (!TECH_MAP_CACHE) TECH_MAP_CACHE = loadTechMap_();

  const title = String(item.title || "");
  const desc = String(item.description || "");
  const text = (title + "\n" + desc).toLowerCase();

  const inds = String(sourceIndustriesStr || "").toLowerCase();
  const catRaw = String(category || "").toLowerCase().trim();

  const cat = ({
    "telecommunications": "telecom",
    "security": "security",
    "cybersecurity": "security",
    "privacy": "privacy",
    "payments": "payments",
    "cloud": "cloud",
    "it": "it"
  }[catRaw]) || catRaw;

  const tech = new Set();

  const proceduralSignals = [
    "sunshine act meeting",
    "open commission meeting",
    "public meeting",
    "notice of meeting",
    "advisory committee",
    "information collection",
    "paperwork reduction act",
    "request for comments",
    "comment request",
    "delegated authority",
    "notice of public meeting",
    "meeting agenda",
    "collection being reviewed"
  ];

  const hasStrongTechSignal = containsAny_(text, [
    "cve-", "vulnerability", "ransomware", "malware", "phishing", "breach", "incident",
    "zero trust", "mfa", "multi-factor", "iam", "sso", "edr", "xdr", "siem", "soc",
    "patch", "mitigation", "exploit", "known exploited",
    "e911", "wireless emergency alerts", "wea", "robocall", "cpni",
    "broadband", "dia", "mpls", "sd-wan", "sase", "vpn",
    "fedramp", "cspm", "cwpp", "encryption", "dlp", "data loss", "logging", "audit trail"
  ]);

  const isProceduralOnly = containsAny_(text, proceduralSignals) && !hasStrongTechSignal;
  if (isProceduralOnly) return "";

  const QUICK_MAP = [
    { keys: ["e911", "wireless emergency alerts", "wea", "emergency alert system", "eas"], tech: ["UCaaS/VoIP/E911"] },
    { keys: ["robocall", "call blocking", "stir/shaken", "do not originate"], tech: ["UCaaS/VoIP/E911"] },
    { keys: ["broadband", "dedicated internet access", "dia", "mpls", "private line", "ethernet"], tech: ["DIA/MPLS/Private Transport"] },
    { keys: ["sd-wan", "sase", "zero trust network access", "ztna", "vpn"], tech: ["SD-WAN/SASE"] },

    { keys: ["ransomware", "malware", "exploit", "breach", "incident"], tech: ["MDR/XDR/EDR", "SIEM/Logging"] },
    { keys: ["cve-", "vulnerability", "patch", "mitigation"], tech: ["MDR/XDR/EDR", "Vulnerability Management", "Patch Management"] },
    { keys: ["mfa", "multi-factor", "iam", "sso", "identity"], tech: ["IAM/SSO/MFA"] },
    { keys: ["zero trust", "ztna"], tech: ["Zero Trust Network Access"] },
    { keys: ["siem", "log management", "logging", "soc"], tech: ["SIEM/Logging"] },

    { keys: ["encryption", "data protection"], tech: ["Encryption/Data Protection"] },
    { keys: ["dlp", "data loss prevention"], tech: ["DLP"] },
    { keys: ["archiving", "retention", "e-discovery"], tech: ["Email Archiving/Compliance"] },

    { keys: ["fedramp", "authorization", "ato"], tech: ["Cloud Security (CSPM/CWPP)"] },
    { keys: ["backup", "disaster recovery", "dr", "rto", "rpo"], tech: ["Backup/DRaaS"] }
  ];

  QUICK_MAP.forEach(rule => {
    if (rule.keys.some(k => text.indexOf(k) !== -1)) {
      rule.tech.forEach(t => tech.add(t));
    }
  });

  const industryHas = (val) => inds.indexOf(val) !== -1;

  TECH_MAP_CACHE.forEach(rule => {
    if (rule.catFilter && rule.catFilter !== cat) return;

    if (rule.industries && rule.industries.length) {
      const match = rule.industries.some(ind => industryHas(ind));
      if (!match) return;
    }

    if (rule.type === "TEXT") {
      if (text.indexOf(rule.keyword) === -1) return;
    } else if (rule.type === "CATEGORY") {
      if (rule.keyword !== cat) return;
    }

    String(rule.techList || "").split(",")
      .map(s => s.trim())
      .filter(Boolean)
      .forEach(t => tech.add(t));
  });

  if (tech.size === 0) {
    if (!hasStrongTechSignal) return "";

    if (cat === "telecom") {
      ["UCaaS/VoIP/E911", "DIA/MPLS/Private Transport", "SD-WAN/SASE"].forEach(t => tech.add(t));
    } else if (cat === "cloud") {
      ["Cloud Security (CSPM/CWPP)", "Backup/DRaaS"].forEach(t => tech.add(t));
    } else if (cat === "security") {
      ["MDR/XDR/EDR", "Zero Trust Network Access", "SIEM/Logging", "IAM/SSO/MFA"].forEach(t => tech.add(t));
    } else if (cat === "payments") {
      ["SD-WAN/SASE", "Firewall/NGFW", "MDR/XDR/EDR"].forEach(t => tech.add(t));
    } else if (cat === "privacy") {
      ["DLP", "Encryption/Data Protection", "Email Archiving/Compliance"].forEach(t => tech.add(t));
    } else {
      ["Backup/DRaaS", "MDM/UEM"].forEach(t => tech.add(t));
    }
  }

  return Array.from(tech).join(", ");
}

/* ======================= DATE EXTRACTION ======================= */

function extractDueDate_(item) {
  const txt = (String(item.title || "") + "\n" + String(item.description || "")).replace(/\s+/g, " ").trim();
  if (!txt) return "";
  const patterns = [
    /\b(?:deadline|due(?:\s+date)?|effective|no later than)\s+([A-Za-z]+\s+\d{1,2},\s+\d{4})/i,
    /\b(?:deadline|due(?:\s+date)?|effective|no later than)\s+(\d{1,2}\/\d{1,2}\/\d{2,4})/i,
    /\b(\d{4}-\d{2}-\d{2})\b/
  ];
  for (const re of patterns) {
    const m = txt.match(re);
    if (!m) continue;
    const d = parseFlexibleDate_(m[1]);
    if (d) return Utilities.formatDate(d, Session.getScriptTimeZone(), "yyyy-MM-dd");
  }
  return "";
}

function extractImposedDate_(item) {
  const txt = (String(item.title || "") + "\n" + String(item.description || "")).replace(/\s+/g, " ").trim();
  if (txt) {
    const patterns = [
      /\b(?:issued|adopted|published|announced|released|imposed|finalized|effective as of)\s+on\s+([A-Za-z]+\s+\d{1,2},\s+\d{4})/i,
      /\b(?:issued|adopted|published|announced|released|imposed|finalized|effective as of)\s+([A-Za-z]+\s+\d{1,2},\s+\d{4})/i,
      /\b(?:issued|adopted|published|announced|released|imposed|finalized|effective as of)\s+on\s+(\d{1,2}\/\d{1,2}\/\d{2,4})/i,
      /\b(?:issued|adopted|published|announced|released|imposed|finalized|effective as of)\s+(\d{1,2}\/\d{1,2}\/\d{2,4})/i
    ];
    for (const re of patterns) {
      const m = txt.match(re);
      if (!m) continue;
      const d = parseFlexibleDate_(m[1]);
      if (d) return Utilities.formatDate(d, Session.getScriptTimeZone(), "yyyy-MM-dd");
    }
  }

  const pub = normalizeDate_(item.published || "");
  if (pub) return Utilities.formatDate(pub, Session.getScriptTimeZone(), "yyyy-MM-dd");

  return "";
}

function parseFlexibleDate_(raw) {
  if (/^\d{4}-\d{2}-\d{2}$/.test(raw)) {
    const parts = raw.split("-").map(Number);
    return new Date(parts[0], parts[1] - 1, parts[2]);
  }
  if (/^\d{1,2}\/\d{1,2}\/\d{2,4}$/.test(raw)) {
    const parts = raw.split("/").map(Number);
    const mm = parts[0], dd = parts[1], yy = parts[2];
    const y = yy < 100 ? (yy >= 70 ? 1900 + yy : 2000 + yy) : yy;
    return new Date(y, mm - 1, dd);
  }
  const d = new Date(raw);
  return isNaN(d.getTime()) ? null : d;
}

/* ======================= AFFECTED COMPANIES ======================= */

let ACCOUNTS_CACHE = null;

function loadAccounts_() {
  if (ACCOUNTS_CACHE) return ACCOUNTS_CACHE;

  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sh = ss.getSheetByName(CONFIG.ACCOUNTS_SHEET);
  if (!sh || sh.getLastRow() < 2) {
    ACCOUNTS_CACHE = [];
    return ACCOUNTS_CACHE;
  }

  const values = sh.getDataRange().getValues();
  const headers = values[0].map(h => String(h || "").trim());
  const idx = n => headers.indexOf(n);

  const iEnabled = idx("Enabled");
  const iCompany = idx("CompanyName");
  const iTags = idx("IndustryTags");
  const iOwner = idx("Owner");
  const iNotes = idx("Notes");

  if ([iEnabled, iCompany, iTags].some(i => i === -1)) {
    throw new Error("Accounts sheet must have Enabled, CompanyName, IndustryTags headers.");
  }

  const out = [];
  for (let r = 1; r < values.length; r++) {
    const row = values[r];
    const enabled = (row[iEnabled] === true || String(row[iEnabled]).toUpperCase() === "TRUE");
    if (!enabled) continue;

    const companyName = String(row[iCompany] || "").trim();
    if (!companyName) continue;

    const tags = String(row[iTags] || "")
      .split(",")
      .map(s => normalizeIndustry_(s))
      .filter(Boolean);

    if (!tags.length) continue;

    out.push({
      companyName,
      tags,
      owner: iOwner === -1 ? "" : String(row[iOwner] || "").trim(),
      notes: iNotes === -1 ? "" : String(row[iNotes] || "").trim()
    });
  }

  ACCOUNTS_CACHE = out;
  return ACCOUNTS_CACHE;
}

function resetAccountsCache_() {
  ACCOUNTS_CACHE = null;
}

function findAffectedCompanies_(sourceIndustries) {
  const accounts = loadAccounts_();
  if (!accounts.length) return { count: 0, companies: [], display: "" };

  const srcTags = (sourceIndustries || []).map(i => normalizeIndustry_(i)).filter(Boolean);
  if (!srcTags.length) return { count: 0, companies: [], display: "" };

  const matches = accounts.filter(acct => acct.tags.some(t => srcTags.indexOf(t) !== -1))
    .map(acct => acct.companyName);

  const unique = Array.from(new Set(matches)).sort();
  const count = unique.length;

  if (!count) return { count: 0, companies: [], display: "" };

  const maxNames = CONFIG.MAX_AFFECTED_COMPANIES_IN_CELL;
  const shown = unique.slice(0, maxNames);
  const remainder = count - shown.length;
  const display = remainder > 0
    ? shown.join(", ") + " (+" + remainder + " more)"
    : shown.join(", ");

  return {
    count,
    companies: unique,
    display
  };
}

/* ======================= DEDUPE KEYS ======================= */

function makeDedupeKey_(src, item) {
  const base = item.link || ((item.title || "") + "|" + (item.published || ""));
  return Utilities.base64EncodeWebSafe(String(src) + "::" + String(base)).slice(0, 200);
}
function isAlreadySeen_(k) { return PropertiesService.getScriptProperties().getProperty(k) !== null; }
function markSeen_(k) { PropertiesService.getScriptProperties().setProperty(k, String(Date.now())); }

/* ======================= ALERTS SHEET HELPERS ======================= */

function normalizeLink_(link, obj) {
  const raw = String(link || "").trim();

  if (raw.indexOf("http://") === 0 || raw.indexOf("https://") === 0) return raw;

  const cveMatch = raw.match(/CVE-\d{4}-\d+/i) || String(obj.cveID || obj.cve || "").match(/CVE-\d{4}-\d+/i);
  if (cveMatch) {
    const cve = cveMatch[0].toUpperCase();
    return "https://nvd.nist.gov/vuln/detail/" + cve;
  }

  return raw;
}

function getExpectedAlertsHeaders_() {
  return [
    "Timestamp",
    "Severity",
    "Category",
    "ImpactScore",
    "IndustryCoverage",
    "SourceIndustries",
    "ImposedDate",
    "DueDate",
    "AffectedCompanies",
    "AffectedCompanyCount",
    "SourceName",
    "Title",
    "Published",
    "Link",
    "MatchedKeywords",
    "MatchedIndustries",
    "TechCategories"
  ];
}

function ensureAlertsHeaders_(sheet) {
  const expected = getExpectedAlertsHeaders_();

  if (sheet.getLastRow() === 0) {
    sheet.getRange(1, 1, 1, expected.length).setValues([expected]);
    sheet.setFrozenRows(1);
    sheet.getRange(1, 1, 1, expected.length).setFontWeight("bold");
    return;
  }

  const lastCol = sheet.getLastColumn();
  const headers = sheet.getRange(1, 1, 1, lastCol).getValues()[0].map(h => String(h || "").trim());

  const missing = expected.filter(h => headers.indexOf(h) === -1);
  if (!missing.length) return;

  sheet.getRange(1, lastCol + 1, 1, missing.length).setValues([missing]);
  sheet.getRange(1, 1, 1, lastCol + missing.length).setFontWeight("bold");
}

function appendAlerts_(sheet, rows) {
  if (!rows.length) return;
  const sr = sheet.getLastRow() + 1;
  sheet.getRange(sr, 1, rows.length, rows[0].length).setValues(rows);
}

function loadTechMap_() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sh = ss.getSheetByName("Tech Map");
  if (!sh) return [];

  const values = sh.getDataRange().getValues();
  if (values.length < 2) return [];

  const headers = values[0].map(h => String(h || "").trim());
  const idx = name => headers.indexOf(name);

  const iType = idx("KeywordType");
  const iKeyword = idx("Keyword");
  const iInd = idx("IndustryFilter");
  const iCat = idx("CategoryFilter");
  const iTech = idx("TechList");

  if ([iType, iKeyword, iInd, iCat, iTech].some(i => i === -1)) return [];

  const rows = [];
  for (let r = 1; r < values.length; r++) {
    const row = values[r];
    const keyword = String(row[iKeyword] || "").trim();
    const techList = String(row[iTech] || "").trim();
    if (!keyword || !techList) continue;

    rows.push({
      type: String(row[iType] || "TEXT").trim().toUpperCase(),
      keyword: keyword.toLowerCase(),
      industries: String(row[iInd] || "").toLowerCase().split(";").map(s => s.trim()).filter(Boolean),
      catFilter: String(row[iCat] || "").toLowerCase(),
      techList: techList
    });
  }
  return rows;
}

/* ======================= ROUTING / NOISE CONTROLS ======================= */

function resolveRecipientsForAlert_(alert) {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sh = ss.getSheetByName(CONFIG.ROUTING_SHEET);
  if (!sh) return [CONFIG.EMAIL_TO];

  const data = sh.getDataRange().getValues();
  if (data.length < 2) return [CONFIG.EMAIL_TO];

  const headers = data[0].map(h => String(h || "").trim());
  const idx = (a, b) => {
    const ia = headers.indexOf(a);
    if (ia !== -1) return ia;
    return b ? headers.indexOf(b) : -1;
  };

  const iEnabled = idx("Enabled");
  const iIndKey = idx("IndustryKey", "Industry");
  const iCat = idx("Category");
  const iSev = idx("Severity", "Severities");
  const iMin = idx("ImpactMin", "MinImpact");
  const iMax = idx("ImpactMax");
  const iTo = idx("Email to", "EmailTo");

  if (iTo === -1) return [CONFIG.EMAIL_TO];

  const alertCat = String(alert.category || alert.Category || "").toLowerCase().trim();
  const alertImpact = Number(alert.impact || alert.ImpactScore || 0);
  const alertSev = String(alert.severity || alert.Severity || "").toUpperCase().trim();

  const alertIndKeys = String(alert.sourceIndustries || alert.SourceIndustries || "")
    .split(",")
    .map(s => normalizeIndustry_(s.trim()))
    .filter(Boolean);

  const recipients = new Set();

  for (let r = 1; r < data.length; r++) {
    const row = data[r];

    if (iEnabled !== -1) {
      const en = row[iEnabled];
      const enabled = (en === true || String(en).toUpperCase() === "TRUE");
      if (!enabled) continue;
    }

    const ruleTo = String(row[iTo] || "").trim();
    if (!ruleTo) continue;

    const ruleIndKey = iIndKey === -1 ? "" : String(row[iIndKey] || "").trim().toLowerCase();
    if (ruleIndKey && ruleIndKey !== "any") {
      if (alertIndKeys.indexOf(ruleIndKey) === -1) continue;
    }

    const ruleCat = iCat === -1 ? "" : String(row[iCat] || "").trim().toLowerCase();
    if (ruleCat && ruleCat !== "any" && ruleCat !== alertCat) continue;

    const ruleMin = iMin === -1 ? 0 : Number(row[iMin]) || 0;
    const ruleMax = iMax === -1 ? 999 : Number(row[iMax]) || 999;
    if (alertImpact < ruleMin || alertImpact > ruleMax) continue;

    const ruleSev = iSev === -1 ? "" : String(row[iSev] || "").trim();
    if (ruleSev && ruleSev.toUpperCase() !== "ANY") {
      const sevSet = ruleSev.split(",").map(s => s.trim().toUpperCase()).filter(Boolean);
      if (sevSet.length && sevSet.indexOf(alertSev) === -1) continue;
    }

    ruleTo.split(/[;,]/).map(s => s.trim()).filter(Boolean).forEach(e => recipients.add(e));
  }

  const cleaned = sanitizeRecipientList_(Array.from(recipients));
  if (!cleaned.length) return [CONFIG.EMAIL_TO];
  return cleaned;
}

function loadNoiseControls_() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sh = ss.getSheetByName("Noise Controls");
  if (!sh) return [];

  const values = sh.getDataRange().getValues();
  if (values.length < 2) return [];

  const headers = values[0].map(h => String(h || "").trim());
  const idx = n => headers.indexOf(n);

  const iSrc = idx("SourceName");
  const iInd = idx("SuppressIndustries");
  const iMin = idx("MinImpact");
  const iSev = idx("EmailSeverities");
  const iCamp = idx("ExcludeFromCampaigns");

  if (iSrc === -1) return [];

  const rules = [];
  for (let r = 1; r < values.length; r++) {
    const row = values[r];
    const srcName = String(row[iSrc] || "").trim();
    if (!srcName) continue;

    rules.push({
      sourceName: srcName,
      suppressIndustries: iInd === -1 ? [] : String(row[iInd] || "").toLowerCase().split(",").map(s => s.trim()).filter(Boolean),
      minImpact: iMin === -1 ? 0 : (Number(row[iMin]) || 0),
      emailSeverities: iSev === -1 ? [] : String(row[iSev] || "").toUpperCase().split(",").map(s => s.trim()).filter(Boolean),
      excludeFromCampaigns: iCamp !== -1 && String(row[iCamp] || "").toUpperCase() === "TRUE"
    });
  }
  return rules;
}

let NOISE_RULES_CACHE = null;

function evaluateNoiseForAlert_(alert) {
  if (!NOISE_RULES_CACHE) {
    NOISE_RULES_CACHE = loadNoiseControls_();
  }
  const srcName = String(alert.sourceName || alert.SourceName || "").trim();
  const inds = String(alert.sourceIndustries || alert.SourceIndustries || "").toLowerCase();
  const sev = String(alert.severity || alert.Severity || "").toUpperCase();
  const impact = Number(alert.impact || alert.ImpactScore || 0);

  const matches = NOISE_RULES_CACHE.filter(r => r.sourceName === srcName);
  if (!matches.length) return { suppressEmail: false, excludeFromCampaigns: false };

  let suppressEmail = false;
  let excludeFromCampaigns = false;

  matches.forEach(rule => {
    if (rule.suppressIndustries.length) {
      const hit = rule.suppressIndustries.some(ind => inds.indexOf(ind) !== -1);
      if (hit) suppressEmail = true;
    }
    if (impact < rule.minImpact) suppressEmail = true;
    if (rule.emailSeverities.length && rule.emailSeverities.indexOf(sev) === -1) {
      suppressEmail = true;
    }
    if (rule.excludeFromCampaigns) excludeFromCampaigns = true;
  });

  return { suppressEmail: suppressEmail, excludeFromCampaigns: excludeFromCampaigns };
}

/* ======================= SOURCES ======================= */

function getSources_(sheet) {
  const v = sheet.getDataRange().getValues();
  if (v.length < 2) return [];
  const hd = v[0].map(h => String(h || "").trim());
  const idx = n => hd.indexOf(n);

  const iE = idx("Enabled");
  const iS = idx("SourceName");
  const iU = idx("FeedURL");
  const iF1 = idx("FallbackURL1");
  const iF2 = idx("FallbackURL2");
  const iI = idx("Industries");
  const iK = idx("Keywords");

  if ([iE, iS, iU, iI, iK].some(i => i === -1)) {
    throw new Error("Sources sheet must have Enabled,SourceName,FeedURL,Industries,Keywords (and optionally FallbackURL1,FallbackURL2)");
  }

  const out = [];
  for (let r = 1; r < v.length; r++) {
    const enabled = (v[r][iE] === true || String(v[r][iE]).toUpperCase() === "TRUE");
    if (!enabled) continue;

    const sn = String(v[r][iS] || "").trim();
    const url = String(v[r][iU] || "").trim();
    if (!sn || !url) continue;

    out.push({
      sourceName: sn,
      feedUrl: url,
      fallbackUrl1: iF1 === -1 ? "" : String(v[r][iF1] || "").trim(),
      fallbackUrl2: iF2 === -1 ? "" : String(v[r][iF2] || "").trim(),
      industries: splitCsv_(v[r][iI]).map(x => String(x || "").trim()),
      keywords: splitCsv_(v[r][iK])
    });
  }
  return out;
}

function splitCsv_(c) { return String(c || "").split(",").map(s => s.trim()).filter(Boolean); }

/* ======================= DIGEST QUEUE ======================= */

function queueDigest_(items) {
  const existing = readDigestQueue_();
  const toAdd = items.map(m => ({
    severity: m.severity,
    category: m.category,
    impact: String(m.impact || ""),
    imposedDate: m.imposedDate || "",
    dueDate: m.due || "",
    sourceIndustries: m.sourceIndustries || "",
    affectedCompanies: m.affectedCompanies || "",
    affectedCompanyCount: String(m.affectedCompanyCount || ""),
    sourceName: m.src ? m.src.sourceName : (m.srcName || ""),
    title: m.item.title || "",
    link: m.item.link || "",
    matchedKeywords: m.matchedKeywords || "",
    matchedIndustries: m.matchedIndustries || "",
    techCategories: m.techCategories || ""
  }));
  PropertiesService.getScriptProperties().setProperty(CONFIG.DIGEST_QUEUE_KEY, JSON.stringify(existing.concat(toAdd)));
}

function readDigestQueue_() {
  const raw = PropertiesService.getScriptProperties().getProperty(CONFIG.DIGEST_QUEUE_KEY);
  if (!raw) return [];
  try { return JSON.parse(raw) || []; } catch (_) { return []; }
}

function clearDigestQueue_() {
  PropertiesService.getScriptProperties().deleteProperty(CONFIG.DIGEST_QUEUE_KEY);
}

/* ======================= DAILY DIGEST ======================= */

function sendDailyDigest() {
  if (!CONFIG.SEND_EMAIL) return;

  const queued = readDigestQueue_();
  if (!queued.length) {
    Logger.log("Digest: queue empty.");
    return;
  }

  queued.sort((a, b) => {
    const sa = severityRank_(a.severity);
    const sb = severityRank_(b.severity);
    if (sa !== sb) return sa - sb;
    return (Number(b.impact) || 0) - (Number(a.impact) || 0);
  });

  const table = buildHtmlAlertsTable_(queued, {
    includeIndustries: true,
    includeImposedDate: true,
    includeDueDate: true,
    includeAffected: true,
    includeTech: true
  });

  const htmlBody = buildHtmlEmailWrapper_({
    title: "Daily Compliance Digest",
    subtitle: "Non-immediate items queued since last digest: " + queued.length,
    blocks: [table],
    footerNote: "Includes advisory/guidance items only."
});

  const textBody = queued.map(a => [
    (a.severity + " | " + a.category + " | Impact " + a.impact + " | Imposed " + (a.imposedDate || "n/a") + " | Due " + (a.dueDate || "n/a")),
    ("Industries: " + (a.sourceIndustries || "n/a")),
    ("Affected Companies: " + (a.affectedCompanies || "n/a")),
    (a.sourceName + ": " + a.title),
    (a.link || ""),
    ""
  ].join("\n")).join("\n");

  sendStyledEmail_({
    to: CONFIG.EMAIL_TO,
    subject: CONFIG.DIGEST_SUBJECT_PREFIX + " " + queued.length + " item(s)",
    htmlBody: htmlBody,
    body: textBody
  });

  clearDigestQueue_();
}

/* ======================= IMMEDIATE EMAIL ======================= */

function sendImmediateEmail_(matches) {
  let payload = matches.map(m => ({
    severity: m.severity,
    category: m.category,
    impact: m.impact,
    imposedDate: m.imposedDate || "",
    dueDate: m.due || "",
    sourceIndustries: m.sourceIndustries || "",
    affectedCompanies: m.affectedCompanies || "",
    affectedCompanyCount: m.affectedCompanyCount || 0,
    sourceName: m.src.sourceName,
    title: m.item.title || "",
    link: m.item.link || "",
    techCategories: m.techCategories || ""
  }));

  payload = payload.filter(p => !evaluateNoiseForAlert_(p).suppressEmail);
  if (!payload.length) {
    Logger.log("sendImmediateEmail_: all alerts suppressed by noise controls.");
    return;
  }

  const allRecipients = new Set();
  payload.forEach(p => {
    resolveRecipientsForAlert_(p).forEach(e => allRecipients.add(e));
  });

  const toList = sanitizeRecipientList_(Array.from(allRecipients)).join(",");
  const finalTo = toList ? toList : CONFIG.EMAIL_TO;

  const table = buildHtmlAlertsTable_(payload, {
    includeIndustries: true,
    includeImposedDate: true,
    includeDueDate: true,
    includeAffected: true,
    includeTech: true
  });

  const htmlBody = buildHtmlEmailWrapper_({
    title: "High-Severity Compliance Alert",
    subtitle: "MANDATE / FINAL_RULE / ENFORCEMENT items that may require action.",
    blocks: [table],
    footerNote: "Auto-generated by Compliance Tracker."
  });

  const textBody = payload.map(a => [
    (a.severity + " | " + a.category + " | Impact " + a.impact + " | Imposed " + (a.imposedDate || "n/a") + " | Due " + (a.dueDate || "n/a")),
    ("Industries: " + (a.sourceIndustries || "n/a")),
    ("Affected Companies: " + (a.affectedCompanies || "n/a")),
    (a.sourceName + ": " + a.title),
    (a.link || ""),
    ""
  ].join("\n")).join("\n");

  sendStyledEmail_({
    to: finalTo,
    subject: CONFIG.SUBJECT_PREFIX + " " + payload.length + " High-Severity item(s)",
    htmlBody: htmlBody,
    body: textBody
  });
}

/* ======================= WEEKLY SUMMARY ======================= */

function buildWeeklySummary() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sh = ss.getSheetByName(CONFIG.ALERTS_SHEET);
  if (!sh) throw new Error("Missing Alerts sheet");

  let ws = ss.getSheetByName(CONFIG.WEEKLY_SHEET);
  if (!ws) ws = ss.insertSheet(CONFIG.WEEKLY_SHEET);
  ws.clearContents();

  const data = sh.getDataRange().getValues();
  if (data.length < 2) {
    ws.getRange(1, 1).setValue("No alerts yet.");
    return;
  }

  const headers = data[0].map(h => String(h || "").trim());
  const idx = n => headers.indexOf(n);
  const iTs = idx("Timestamp"), iSev = idx("Severity"), iCat = idx("Category"),
    iImpact = idx("ImpactScore"), iSrcName = idx("SourceName");

  if ([iTs, iSev, iCat, iImpact, iSrcName].some(i => i === -1)) {
    throw new Error("Alerts missing required columns for weekly summary.");
  }

  const rows = data.slice(1);
  const cutoff = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000);

  const lastWeek = rows.filter(r => {
    const ts = normalizeDate_(r[iTs]);
    return ts && ts >= cutoff;
  });
  const total = lastWeek.length;

  const countsBy = (colIdx) => {
    const m = {};
    lastWeek.forEach(r => {
      const k = String(r[colIdx] || "").trim() || "Unknown";
      m[k] = (m[k] || 0) + 1;
    });
    return m;
  };

  const sevCounts = countsBy(iSev);
  const catCounts = countsBy(iCat);
  const srcCounts = countsBy(iSrcName);

  const avgImpact = total
    ? (lastWeek.reduce((a, r) => a + (Number(r[iImpact]) || 0), 0) / total)
    : 0;

  ws.getRange(1, 1).setValue("Weekly Compliance Summary (Last 7 Days)");
  ws.getRange(2, 1).setValue("Generated");
  ws.getRange(2, 2).setValue(new Date());

  ws.getRange(4, 1).setValue("Total Alerts");
  ws.getRange(4, 2).setValue(total);

  ws.getRange(5, 1).setValue("Average Impact Score");
  ws.getRange(5, 2).setValue(Math.round(avgImpact * 10) / 10);

  writeTopTable_(ws, 7, 1, "By Severity", sevCounts);
  writeTopTable_(ws, 7, 4, "By Category", catCounts);
  writeTopTable_(ws, 7, 7, "Top Sources", srcCounts);

  ws.getRange(1, 1, ws.getLastRow(), ws.getLastColumn()).setFontFamily("Arial");
  ws.getRange(1, 1).setFontWeight("bold");
  ws.setFrozenRows(1);

  if (!CONFIG.SEND_EMAIL) return;

  const htmlBody = buildHtmlEmailWrapper_({
    title: "Weekly Compliance Summary",
    subtitle: "Last 7 days: " + total + " item(s) | Avg impact: " + (Math.round(avgImpact * 10) / 10),
    blocks: [
      buildHtmlMetricRow_([
        { label: "Total Alerts", value: String(total) },
        { label: "Average Impact", value: String(Math.round(avgImpact * 10) / 10) },
        { label: "Top Severity", value: topKey_(sevCounts) }
      ]),
      buildHtmlCountsTable_("Top Severities", sevCounts),
      buildHtmlCountsTable_("Top Categories", catCounts),
      buildHtmlCountsTable_("Top Sources", srcCounts)
    ],
    footerNote: "Use Campaigns tab to turn these into outreach."
  });

  const textBody = [
    "Weekly Compliance Summary (last 7 days)",
    "Total alerts: " + total,
    "Average impact: " + (Math.round(avgImpact * 10) / 10),
    "",
    "Top severities:",
    ...formatCounts_(sevCounts),
    "",
    "Top categories:",
    ...formatCounts_(catCounts),
    "",
    "Top sources:",
    ...formatCounts_(srcCounts)
  ].join("\n");

  sendStyledEmail_({
    to: CONFIG.EMAIL_TO,
    subject: CONFIG.WEEKLY_SUBJECT_PREFIX + " " + Utilities.formatDate(new Date(), Session.getScriptTimeZone(), "MMM d, yyyy"),
    htmlBody: htmlBody,
    body: textBody
  });
}

function writeTopTable_(sheet, row, col, title, mapObj) {
  sheet.getRange(row, col).setValue(title);
  const entries = Object.entries(mapObj || {}).sort((a, b) => b[1] - a[1]).slice(0, 10);
  sheet.getRange(row + 1, col, 1, 2).setValues([["Item", "Count"]]);
  if (entries.length) sheet.getRange(row + 2, col, entries.length, 2).setValues(entries);
  else sheet.getRange(row + 2, col).setValue("None");
}

function formatCounts_(mapObj) {
  return Object.entries(mapObj || {}).sort((a, b) => b[1] - a[1]).slice(0, 10).map(([k, v]) => "- " + k + ": " + v);
}

function topKey_(mapObj) {
  const entries = Object.entries(mapObj || {}).sort((a, b) => b[1] - a[1]);
  return entries.length ? entries[0][0] : "None";
}

/* ======================= HTML EMAIL HELPERS ======================= */

function buildHtmlEmailWrapper_({title,subtitle,blocks,footerNote}) {
  const inner = (blocks||[]).join("");
  return `
  <div style="font-family:Arial,Helvetica,sans-serif;background:#f3f4f6;padding:16px;">
    <div style="max-width:960px;margin:0 auto;background:#ffffff;border-radius:12px;border:1px solid #e5e7eb;overflow:hidden;">
      <div style="padding:12px 16px;background:linear-gradient(90deg,#0b5cff,#34c3ff);color:#ffffff;">
        <div style="font-size:18px;font-weight:800;">${sanitize_(title)}</div>
        <div style="font-size:12px;margin-top:4px;opacity:0.95;">${sanitize_(subtitle||"")}</div>
      </div>
      <div style="padding:16px 18px;color:#111827;">
        ${inner}
        ${footerNote?`<div style="margin-top:12px;font-size:11px;color:#6b7280;">${sanitize_(footerNote)}</div>`:""}
      </div>
    </div>
  </div>`;
}

function buildHtmlAlertsTable_(items,opts){
  const includeIndustries = !!(opts&&opts.includeIndustries);
  const includeImposedDate = !!(opts&&opts.includeImposedDate);
  const includeDueDate = !!(opts&&opts.includeDueDate);
  const includeAffected = !!(opts&&opts.includeAffected);
  const includeTech = !!(opts&&opts.includeTech);

  const cols = [
    {key:"severity",label:"Severity"},
    {key:"category",label:"Category"},
    {key:"impact",label:"Impact"}
  ];
  if (includeImposedDate) cols.push({key:"imposedDate",label:"Imposed Date"});
  if (includeDueDate) cols.push({key:"dueDate",label:"Due Date"});
  if (includeIndustries) cols.push({key:"sourceIndustries",label:"Industries"});
  if (includeAffected) cols.push({key:"affectedCompanies",label:"Affected Companies"});
  if (includeTech) cols.push({key:"techCategories",label:"Tech / Solutions"});
  cols.push(
    {key:"sourceName",label:"Source"},
    {key:"title",label:"Title"}
  );

  const header = cols.map(c=>`
    <th style="padding:8px;border:1px solid #e5e7eb;background:#f3f4ff;font-size:12px;text-align:left;">
      ${sanitize_(c.label)}
    </th>`).join("");

  const rows = (items||[]).map(a=>{
    const sev = String(a.severity||"").toUpperCase();
    const sevStyle =
      sev==="MANDATE" ? "background:#fee2e2;color:#b91c1c;" :
      sev==="FINAL_RULE" ? "background:#ffedd5;color:#c2410c;" :
      sev==="ENFORCEMENT" ? "background:#ede9fe;color:#5b21b6;" :
      sev==="GUIDANCE" ? "background:#dbeafe;color:#1d4ed8;" :
      "background:#e5e7eb;color:#374151;";

    const cells = cols.map(c=>{
      if (c.key==="severity"){
        return `<td style="padding:8px;border:1px solid #e5e7eb;">
          <span style="display:inline-block;padding:2px 8px;border-radius:999px;font-size:11px;font-weight:700;${sevStyle}">
            ${sanitize_(a.severity||"")}
          </span>
        </td>`;
      }
      if (c.key==="title"){
        const link = a.link||"";
        const label = a.title||"View item";
        return `<td style="padding:8px;border:1px solid #e5e7eb;font-size:13px;">
          ${link?`<a href="${sanitizeUrl_(link)}" target="_blank" style="color:#0b5cff;text-decoration:none;">${sanitize_(label)}</a>`:sanitize_(label)}
        </td>`;
      }
      const v = (a[c.key]||"");
      return `<td style="padding:8px;border:1px solid #e5e7eb;font-size:12px;">${sanitize_(String(v))}</td>`;
    }).join("");

    return `<tr>${cells}</tr>`;
  }).join("");

  return `
  <div style="overflow:auto;margin-top:8px;">
    <table style="border-collapse:collapse;width:100%;min-width:980px;">
      <thead><tr>${header}</tr></thead>
      <tbody>${rows}</tbody>
    </table>
  </div>`;
}

function buildHtmlCountsTable_(title,mapObj){
  const entries = Object.entries(mapObj||{}).sort((a,b)=>b[1]-a[1]).slice(0,10);
  const rows = entries.length?entries:[["None",0]];
  const body = rows.map(([k,v])=>`
    <tr>
      <td style="padding:8px;border:1px solid #e5e7eb;">${sanitize_(k)}</td>
      <td style="padding:8px;border:1px solid #e5e7eb;text-align:right;font-weight:700;">${sanitize_(String(v))}</td>
    </tr>`).join("");

  return `
    <div style="margin:10px 0;">
      <div style="font-weight:800;margin-bottom:4px;">${sanitize_(title)}</div>
      <table style="border-collapse:collapse;width:100%;">
        <thead>
          <tr>
            <th style="padding:8px;border:1px solid #e5e7eb;background:#f3f4ff;text-align:left;">Item</th>
            <th style="padding:8px;border:1px solid #e5e7eb;background:#f3f4ff;text-align:right;">Count</th>
          </tr>
        </thead>
        <tbody>${body}</tbody>
      </table>
    </div>`;
}

function buildHtmlMetricRow_(metrics){
  const cards = (metrics||[]).map(m=>`
    <div style="flex:1;border:1px solid #e5e7eb;border-radius:10px;padding:10px;background:#ffffff;min-width:150px;">
      <div style="font-size:11px;color:#6b7280;font-weight:700;">${sanitize_(m.label)}</div>
      <div style="font-size:18px;font-weight:900;margin-top:2px;color:#111827;">${sanitize_(m.value)}</div>
    </div>`).join("");

  return `<div style="display:flex;gap:8px;flex-wrap:wrap;margin:10px 0;">${cards}</div>`;
}

function sanitize_(s){
  return String(s||"")
    .replace(/&/g,"&amp;")
    .replace(/</g,"&lt;")
    .replace(/>/g,"&gt;");
}
function sanitizeUrl_(u){
  return String(u||"").replace(/"/g,"%22").trim();
}

function sanitizeRecipientList_(list) {
  const emailRe = /^[^\s@]+@[^\s@]+\.[^\s@]+$/i;
  const out = [];
  const seen = {};
  (list || []).forEach(v => {
    const e = String(v || "").trim().toLowerCase();
    if (!e || !emailRe.test(e) || seen[e]) return;
    seen[e] = true;
    out.push(e);
  });
  return out;
}

function sendStyledEmail_({to,subject,htmlBody,body}){
  const cleaned = sanitizeRecipientList_(String(to || "").split(/[;,]/));
  const finalTo = cleaned.length ? cleaned.join(",") : CONFIG.EMAIL_TO;

  try {
    MailApp.sendEmail({
      to: finalTo,
      subject: subject,
      htmlBody: htmlBody,
      body:(body||""),
      replyTo:CONFIG.REPLY_TO,
      name: CONFIG.EMAIL_FROM_NAME
    });
  } catch (e) {
    Logger.log("sendStyledEmail_ failed: " + String(e));
  }
}

function severityRank_(sev){
  const s = String(sev||"").toUpperCase();
  if (s==="MANDATE") return 1;
  if (s==="FINAL_RULE") return 2;
  if (s==="ENFORCEMENT") return 3;
  if (s==="GUIDANCE") return 4;
  return 5;
}

/* ======================= SOURCE HEALTH SHEET ======================= */

function ensureSourceHealthSheet_() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  let sh = ss.getSheetByName(CONFIG.SOURCE_HEALTH_SHEET);
  if (!sh) {
    sh = ss.insertSheet(CONFIG.SOURCE_HEALTH_SHEET);
  }

  if (sh.getLastRow() === 0) {
    const headers = [
      "SourceName",
      "FeedURL",
      "Enabled",
      "LastFetchTime",
      "LastHTTPStatus",
      "LastItemCount",
      "LastParseOK",
      "LastError",
      "FinalURL",
      "ResponseSnippet"
    ];
    sh.getRange(1, 1, 1, headers.length).setValues([headers]);
    sh.setFrozenRows(1);
    sh.getRange(1, 1, 1, headers.length).setFontWeight("bold");
  }
  return sh;
}

function buildSourceHealthSummary() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const outName = "Source Health Summary";
  let out = ss.getSheetByName(outName);
  if (!out) out = ss.insertSheet(outName);
  out.clearContents();

  const headers = [
    "SourceName","FeedURL","Enabled","LastFetchTime","LastHTTPStatus","LastItemCount","LastParseOK","LastError","AuditNote"
  ];
  out.getRange(1,1,1,headers.length).setValues([headers]);
  out.setFrozenRows(1);
  out.getRange(1,1,1,headers.length).setFontWeight("bold");

  const health = ss.getSheetByName(CONFIG.SOURCE_HEALTH_SHEET);
  if (!health || health.getLastRow() < 2) {
    out.getRange(2,1).setValue("No Source Health data yet.");
    return;
  }

  const data = health.getDataRange().getValues();
  const rows = [];

  for (let r = 1; r < data.length; r++) {
    const sourceName = String(data[r][0] || "").trim();
    const feedUrl = String(data[r][1] || "").trim();
    const enabled = String(data[r][2] || "").trim();
    const lastFetch = data[r][3] || "";
    const http = String(data[r][4] || "").trim();
    const itemCount = data[r][5] || 0;
    const parseOk = String(data[r][6] || "").trim();
    const err = String(data[r][7] || "").trim();

    let note = "";
    if (feedUrl.toLowerCase().includes("fcc.gov")) {
      note = "FCC feeds disabled: fcc.gov is unreachable from Google Cloud (HTTP/2 internal errors + HTTP/1.1 timeouts). Replaced with Federal Register FCC RSS.";
    }

    rows.push([sourceName, feedUrl, enabled, lastFetch, http, itemCount, parseOk, err, note]);
  }

  out.getRange(2,1,rows.length,headers.length).setValues(rows);
  out.autoResizeColumns(1, headers.length);
  out.getRange(2,1,rows.length,headers.length).setVerticalAlignment("top");
}

function getSourceEnabledStateByName_(sourceName) {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sh = ss.getSheetByName(CONFIG.SOURCES_SHEET);
  if (!sh) return "";

  const values = sh.getDataRange().getValues();
  if (values.length < 2) return "";

  const headers = values[0].map(h => String(h || "").trim());
  const iEnabled = headers.indexOf("Enabled");
  const iSource = headers.indexOf("SourceName");
  if (iEnabled === -1 || iSource === -1) return "";

  const target = String(sourceName || "").trim().toLowerCase();

  for (let r = 1; r < values.length; r++) {
    const sn = String(values[r][iSource] || "").trim().toLowerCase();
    if (sn === target) {
      const val = values[r][iEnabled];
      return (val === true || String(val).toUpperCase() === "TRUE") ? "TRUE" : "FALSE";
    }
  }
  return "";
}

function updateSourceHealth_(src, statusCode, itemCount, errorText, parseOk, fetchMeta) {
  const sh = ensureSourceHealthSheet_();
  const values = sh.getDataRange().getValues();

  let rowIndex = -1;
  for (let r = 1; r < values.length; r++) {
    const name = String(values[r][0] || "").trim();
    if (name === src.sourceName) {
      rowIndex = r + 1;
      break;
    }
  }

  const finalUrl = fetchMeta && fetchMeta.finalUrl ? String(fetchMeta.finalUrl) : "";
  const snippet = fetchMeta && fetchMeta.snippet ? String(fetchMeta.snippet) : "";

  const record = [
    src.sourceName,
    src.feedUrl,
    getSourceEnabledStateByName_(src.sourceName),
    new Date(),
    statusCode != null ? String(statusCode) : "",
    itemCount || 0,
    parseOk ? "TRUE" : "FALSE",
    errorText ? String(errorText).slice(0, 500) : "",
    finalUrl.slice(0, 500),
    snippet.slice(0, 500)
  ];

  if (rowIndex === -1) {
    const lr = sh.getLastRow();
    sh.getRange(lr + 1, 1, 1, record.length).setValues([record]);
  } else {
    sh.getRange(rowIndex, 1, 1, record.length).setValues([record]);
  }
}

function openSourceHealthSheet() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sh = ensureSourceHealthSheet_();
  ss.setActiveSheet(sh);
}

/* ======================= CAMPAIGN DRAFTS (WEEKLY) ======================= */

function generateCampaignDrafts() {
  const days = CONFIG.CAMPAIGN_LOOKBACK_DAYS;
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const alertsSheet = ss.getSheetByName(CONFIG.ALERTS_SHEET);
  if (!alertsSheet) throw new Error("Missing Alerts sheet");

  const data = alertsSheet.getDataRange().getValues();
  if (data.length<2) return;

  const headers = data[0].map(h=>String(h||"").trim());
  const idx = n=>headers.indexOf(n);

  const iTimestamp = idx("Timestamp");
  const iSeverity = idx("Severity");
  const iCategory = idx("Category");
  const iImpact = idx("ImpactScore");
  const iSourceIndustries = idx("SourceIndustries");
  const iImposedDate = idx("ImposedDate");
  const iDueDate = idx("DueDate");
  const iAffectedCompanies = idx("AffectedCompanies");
  const iSourceName = idx("SourceName");
  const iTitle = idx("Title");
  const iLink = idx("Link");
  const iTech = idx("TechCategories");

  if ([iTimestamp,iSeverity,iCategory,iImpact,iSourceIndustries,iDueDate,iSourceName,iTitle,iLink].some(i=>i===-1)) {
    throw new Error("Alerts missing required columns for campaigns.");
  }

  const cutoff = new Date(Date.now()-days*24*60*60*1000);
  const alerts = [];

  for (let r = 1; r < data.length; r++) {
    const row = data[r];
    const ts = normalizeDate_(row[iTimestamp]);
    if (!ts || ts < cutoff) continue;

    const alert = {
      Timestamp: ts,
      Severity: String(row[iSeverity] || "").trim(),
      Category: String(row[iCategory] || "").trim(),
      ImpactScore: Number(row[iImpact]) || 0,
      SourceIndustries: String(row[iSourceIndustries] || "").trim(),
      ImposedDate: iImposedDate === -1 ? "" : normalizeDate_(row[iImposedDate]),
      DueDate: normalizeDate_(row[iDueDate]),
      AffectedCompanies: iAffectedCompanies === -1 ? "" : String(row[iAffectedCompanies] || "").trim(),
      SourceName: String(row[iSourceName] || "").trim(),
      Title: String(row[iTitle] || "").trim(),
      Link: String(row[iLink] || "").trim(),
      TechCategories: (iTech === -1) ? "" : String(row[iTech] || "").trim()
    };

    const noise = evaluateNoiseForAlert_(alert);
    if (noise.excludeFromCampaigns) continue;

    alerts.push(alert);
  }

  if (!alerts.length) {
    Logger.log("generateCampaignDrafts: no alerts after noise filtering.");
    return;
  }

  const grouped = groupAlertsByIndustry_(alerts);
  const rows = [];
  const genTs = new Date();

  Object.keys(grouped).sort().forEach(indKey => {
    const list = grouped[indKey].sort((a, b) => {
      const sr = severityRank_(a.Severity) - severityRank_(b.Severity);
      if (sr !== 0) return sr;
      return (b.ImpactScore || 0) - (a.ImpactScore || 0);
    });
    const top = list.slice(0, CONFIG.CAMPAIGN_TOP_ALERTS_PER_INDUSTRY);
    const summary = formatAlertBullets_(top);

    ["CISO","CIO","Compliance"].forEach(persona=>{
      const draft = buildPersonaDraft_(persona,indKey,top,summary);
      rows.push([
        genTs,
        industryToDisplay_(indKey),
        persona,
        draft.campaignType,
        draft.subject1,
        draft.subject2,
        draft.subject3,
        draft.emailCopyPaste,
        draft.linkedinDmCopyPaste,
        draft.callScriptCopyPaste,
        draft.primaryCTA,
        draft.alertTitles,
        draft.alertLinks
      ]);
    });
  });

  let campSheet = ss.getSheetByName(CONFIG.CAMPAIGNS_SHEET);
  if (!campSheet) campSheet = ss.insertSheet(CONFIG.CAMPAIGNS_SHEET);
  writeCampaigns_(campSheet,rows);

  if (CONFIG.SEND_EMAIL) {
    emailCampaignPack_(rows,"Weekly");
  }
}

function groupAlertsByIndustry_(alerts){
  const out={};
  alerts.forEach(a=>{
    const inds = a.SourceIndustries
      ? a.SourceIndustries.split(",").map(s=>s.trim()).filter(Boolean)
      : ["general-commercial"];
    inds.forEach(i=>{
      const key = normalizeIndustry_(i);
      if (!out[key]) out[key]=[];
      out[key].push(a);
    });
  });
  return out;
}

function normalizeIndustry_(s){
  return String(s||"")
    .toLowerCase()
    .replace(/\s+/g,"-")
    .replace(/[^a-z0-9\-]/g,"")
    || "general-commercial";
}

function industryToDisplay_(key){
  const map={
    "financial-services":"Financial Services",
    "banking":"Banking",
    "healthcare":"Healthcare",
    "k12":"K-12",
    "education":"Education",
    "manufacturing":"Manufacturing",
    "energy":"Energy",
    "aerospace":"Aerospace",
    "retail":"Retail",
    "hospitality":"Hospitality",
    "real-estate":"Real Estate",
    "government":"Government",
    "auto-dealers":"Auto Dealers",
    "telecom":"Telecom",
    "general-commercial":"Commercial",
    "nonprofit-ngo":"Nonprofit",
    "transportation-logistics":"Logistics",
    "professional-services":"Professional Services",
    "entertainment-media":"Entertainment",
    "legal":"Legal & Compliance",
    "construction":"Construction",
    "oil-and-gas":"Oil & Gas",
    "utilities":"Utilities",
    "food-services":"Food Services",
    "agriculture":"Agriculture"
  };
  if (map[key]) return map[key];
  return key.split("-").map(w=>w? w[0].toUpperCase()+w.slice(1):"").join(" ");
}

function formatAlertBullets_(alerts){
  return alerts.map(a=>{
    const imposed = a.ImposedDate ? ` | Imposed: ${a.ImposedDate}` : "";
    const due = a.DueDate ? ` | Due: ${a.DueDate}` : "";
    const src = a.SourceName ? ` | Source: ${a.SourceName}` : "";
    const affected = a.AffectedCompanies ? ` | Affected: ${a.AffectedCompanies}` : "";
    return `- ${a.Severity} | ${a.Category} | Impact ${a.ImpactScore}${imposed}${due}${src}${affected}\n  ${a.Title}\n  ${a.Link}`;
  }).join("\n");
}

function buildPersonaDraft_(persona,industryKey,alerts,summaryBlock){
  const top = alerts[0]||{};
  const topSev = String(top.Severity||"ADVISORY").toUpperCase();
  const campaignType =
    (topSev==="MANDATE"||topSev==="FINAL_RULE"||topSev==="ENFORCEMENT")
      ? "Action Required"
      : "Education & Prep";

  const industryLabel = industryToDisplay_(industryKey);

  const subjects = buildSubjectLines_(persona,industryLabel,campaignType,alerts);
  const bodies = buildMessageBodies_(persona,industryLabel,campaignType,alerts,summaryBlock);

  const emailCopyPaste = `Subject: ${subjects[0]}\n\n${bodies.emailBody}`;
  const alertTitles = alerts.map(a=>a.Title).join(" | ");
  const alertLinks = alerts.map(a=>a.Link).join(" | ");

  return {
    campaignType,
    subject1:subjects[0],
    subject2:subjects[1],
    subject3:subjects[2],
    emailCopyPaste,
    linkedinDmCopyPaste:bodies.linkedinDm,
    callScriptCopyPaste:bodies.callScript,
    primaryCTA:bodies.primaryCTA,
    alertTitles,
    alertLinks
  };
}

function buildSubjectLines_(persona,industryLabel,campaignType,alerts){
  const a = alerts[0] || {};
  const sev = a.Severity || "Update";
  const cat = a.Category || "IT";
  const due = a.DueDate ? ` (Due ${a.DueDate})` : "";
  const tech = a.TechCategories ? String(a.TechCategories).split(",")[0].trim() : "";
  const techBit = tech ? ` - focus on ${tech}` : "";

  if (persona === "CISO") {
    return [
      `${campaignType}: ${sev} ${cat} signal for ${industryLabel}${due}${techBit}`,
      `${industryLabel} security heads-up${techBit}: what changed and what to do next`,
      `New ${cat} compliance signal for ${industryLabel} security leadership${techBit}`
    ];
  }
  if (persona==="CIO"){
    return [
      `${industryLabel}: ${sev} ${cat} driver, planning and timeline${due}`,
      `Avoid reactive spend: new ${cat} requirements for ${industryLabel}`,
      `${campaignType} update: implications for IT leadership in ${industryLabel}`
    ];
  }
  return [
    `${industryLabel} compliance update: ${sev} ${cat} item to review${due}`,
    `Evidence-ready next steps for ${industryLabel} after this ${sev} update`,
    `${campaignType}: translate this ${cat} signal into an action plan`
  ];
}

function buildMessageBodies_(persona,industryLabel,campaignType,alerts,summaryBlock){
  const top = alerts[0]||{};
  const imposedLine = top.ImposedDate ? `Imposed date: ${top.ImposedDate}.` : "";
  const dueLine = top.DueDate ? `Timing note: one item includes a timeline due by ${top.DueDate}.` : "";
  const tech = top.TechCategories ? String(top.TechCategories).split(",").slice(0,3).join(", ") : "";
  const techLine = tech ? `Recommended focus: ${tech}.` : "";
  const affectedLine = top.AffectedCompanies ? `Potentially affected accounts already mapped in tracker: ${top.AffectedCompanies}.` : "";

  let angle="";
  if (persona==="CISO") angle="reduce security risk and avoid audit surprises";
  else if (persona==="CIO") angle="turn requirements into a scoped plan, timeline, and budget";
  else angle="map requirements to evidence, controls, and documentation";

  const opener =
    `Hi {FirstName},\n\n`+
    `We track telecom and IT compliance signals that can impact ${industryLabel} teams.\n\n`+
    `Based on recent updates, this looks like a ${campaignType.toLowerCase()} moment to ${angle}.`;

  const pkg =
    `\n\nIf helpful, we can deliver a short, practical package:\n`+
    `1) What is impacted (systems, vendors, policies)\n`+
    `2) Gap check (controls, processes, documentation)\n`+
    `3) Action plan (owners, milestones, timeline)\n`+
    `4) Vendor execution path (what usually gets outsourced vs handled in-house)\n`;

  const signals =
    `\n\nRecent signals we flagged:\n`+
    `${summaryBlock}\n`;

  const cta =
    `\n\nIf helpful, reply with “plan” and I will send a one-page roadmap tailored to ${industryLabel}.`;

  const infoLines = [imposedLine, dueLine, techLine, affectedLine].filter(Boolean).map(x => `\n${x}\n`).join("");

  const emailBody =
    `${opener}\n`+
    (infoLines || "\n")+
    pkg+
    signals+
    cta+
    `\n\nBest,\n${CONFIG.EMAIL_FROM_NAME}\n`;

  const linkedinDm =
    `Hi {FirstName} - quick heads-up. We flagged new telecom and IT compliance signals impacting ${industryLabel}. `+
    `If helpful, I can share a one-page roadmap and checklist. Want me to send it?`;

  const callScript =
    `Call opener:\n`+
    `Hi {FirstName}, this is Adam. I will be brief.\n\n`+
    `We track telecom and IT compliance signals for ${industryLabel} organizations and flagged a recent ${top.Category||"IT"} update that could create unplanned work if it is not scoped early.\n\n`+
    `Two quick questions:\n`+
    `1) Who owns compliance mapping and evidence readiness on your side?\n`+
    `2) Do you prefer a quick readiness check or a one-page plan first?\n\n`+
    `If you say “plan,” I will send a one-pager with scope, timeline, and typical workstreams.`;

  return {
    emailBody,
    linkedinDm,
    callScript,
    primaryCTA:`Reply “plan” for a one-page roadmap.`
  };
}

function writeCampaigns_(sheet,rows){
  sheet.clearContents();
  const headers=[
    "GeneratedOn","Industry","Persona","CampaignType",
    "Subject1","Subject2","Subject3",
    "Email_CopyPaste","LinkedInDM_CopyPaste","CallScript_CopyPaste",
    "PrimaryCTA","AlertTitles","AlertLinks"
  ];
  sheet.getRange(1,1,1,headers.length).setValues([headers]);
  sheet.setFrozenRows(1);
  if (!rows.length){
    sheet.getRange(2,1).setValue("No campaign drafts generated.");
    return;
  }
  sheet.getRange(2,1,rows.length,headers.length).setValues(rows);
  sheet.getRange(2,1,rows.length,headers.length).setWrap(true).setVerticalAlignment("top");
}

function emailCampaignPack_(rows, modeLabel) {
  if (!rows.length) return;

  const label = modeLabel ? ` ${modeLabel}` : "";
  const subject = `${CONFIG.CAMPAIGN_SUBJECT_PREFIX}${label} (${Utilities.formatDate(new Date(), Session.getScriptTimeZone(), "MMM d, yyyy")})`;

  const counts = {};
  rows.forEach(r => {
    const ind = r[1];
    counts[ind] = (counts[ind] || 0) + 1;
  });

  const lines = Object.keys(counts).sort().map(ind => `- ${ind}: ${counts[ind]} draft(s)`);

  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const url = ss.getUrl();
  const sheet = ss.getSheetByName(CONFIG.CAMPAIGNS_SHEET);
  const sheetUrl = sheet ? `${url}#gid=${sheet.getSheetId()}` : url;

  const htmlBody = buildHtmlEmailWrapper_({
    title: `Campaign Drafts${label}`,
    subtitle: `Generated ${rows.length} draft(s). Full copy is in the Campaigns tab.`,
    blocks: [
      `<div style="font-size:13px;margin:8px 0;"><b>Summary</b><br>${sanitize_(lines.join("\n")).replace(/\n/g,"<br>")}</div>`,
      `<div style="margin-top:10px;font-size:13px;">Open Campaigns sheet: <a href="${sanitizeUrl_(sheetUrl)}" target="_blank" style="color:#0b5cff;text-decoration:none;">View drafts</a></div>`
    ],
    footerNote: "Tip: Filter by Industry and Persona in the Campaigns tab."
  });

  const textBody =
    `Campaign Drafts${label}\n` +
    `Generated ${rows.length} draft(s).\n\n` +
    `Summary:\n${lines.join("\n")}\n\n` +
    `Open Campaigns sheet: ${sheetUrl}\n`;

  sendStyledEmail_({
    to: CONFIG.EMAIL_TO,
    subject,
    htmlBody,
    body: textBody
  });
}

function generateOnePagerDraftsFromAlerts() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sh = ss.getSheetByName(CONFIG.ALERTS_SHEET);
  if (!sh) throw new Error("Missing Alerts sheet");

  const data = sh.getDataRange().getValues();
  if (data.length < 2) return;

  const headers = data[0].map(h => String(h || "").trim());
  const idx = n => headers.indexOf(n);

  const iSev = idx("Severity");
  const iCat = idx("Category");
  const iImpact = idx("ImpactScore");
  const iInd = idx("SourceIndustries");
  const iSrc = idx("SourceName");
  const iTitle = idx("Title");
  const iImposed = idx("ImposedDate");
  const iDue = idx("DueDate");
  const iTech = idx("TechCategories");

  const cutoff = new Date(Date.now() - 14*24*60*60*1000);
  const iTs = idx("Timestamp");

  const drafts = [];

  for (let r=1; r<data.length; r++) {
    const row = data[r];
    const ts = normalizeDate_(row[iTs]);
    if (!ts || ts < cutoff) continue;

    const alert = {
      Severity: String(row[iSev]||"").trim(),
      Category: String(row[iCat]||"").trim(),
      ImpactScore: Number(row[iImpact])||0,
      SourceIndustries: String(row[iInd]||"").trim(),
      SourceName: String(row[iSrc]||"").trim(),
      Title: String(row[iTitle]||"").trim(),
      ImposedDate: iImposed === -1 ? "" : String(row[iImposed]||"").trim(),
      DueDate: String(row[iDue]||"").trim(),
      TechCategories: String(row[iTech]||"").trim()
    };

    const noise = evaluateNoiseForAlert_(alert);
    if (noise.excludeFromCampaigns) continue;

    const inds = alert.SourceIndustries
      ? alert.SourceIndustries.split(",").map(s=>s.trim()).filter(Boolean)
      : ["general-commercial"];

    const techFirst = alert.TechCategories
      ? alert.TechCategories.split(",")[0].trim()
      : "";

    inds.forEach(ind => {
      drafts.push([
        industryToDisplay_(normalizeIndustry_(ind)),
        techFirst,
        `${alert.SourceName}: ${alert.Title}`,
        `Severity: ${alert.Severity}, Impact: ${alert.ImpactScore}, Imposed: ${alert.ImposedDate || "n/a"}`,
        alert.TechCategories,
        alert.DueDate ? `Align controls before ${alert.DueDate}.` : ""
      ]);
    });
  }

  let out = ss.getSheetByName("One-Pager Drafts");
  if (!out) out = ss.insertSheet("One-Pager Drafts");
  out.clearContents();

  const headersOut = [
    "Industry","PrimaryTech","FrameworkSummary",
    "RiskSummary","RecommendedStack","CallToAction"
  ];
  out.getRange(1,1,1,headersOut.length).setValues([headersOut]);
  if (drafts.length) {
    out.getRange(2,1,drafts.length,headersOut.length).setValues(drafts);
    out.getRange(2,1,drafts.length,headersOut.length).setWrap(true).setVerticalAlignment("top");
  }
}

/* ======================= YTD COMMANDS (MANUAL) ======================= */

function YTD_scanAlerts() {
  try {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const alertsSheet = ss.getSheetByName(CONFIG.ALERTS_SHEET);
    if (!alertsSheet) throw new Error("Missing sheet: " + CONFIG.ALERTS_SHEET);

    const data = alertsSheet.getDataRange().getValues();
    if (data.length < 2) {
      Logger.log("No alerts found in Alerts sheet.");
      return;
    }

    const headers = data[0].map(h => String(h || "").trim());
    const idx = name => headers.indexOf(name);

    const iTimestamp = idx("Timestamp");
    const iSeverity = idx("Severity");
    const iCategory = idx("Category");
    const iImpact = idx("ImpactScore");
    const iSourceIndustries = idx("SourceIndustries");
    const iImposedDate = idx("ImposedDate");
    const iDueDate = idx("DueDate");
    const iAffectedCompanies = idx("AffectedCompanies");
    const iSourceName = idx("SourceName");
    const iTitle = idx("Title");
    const iLink = idx("Link");
    const iTech = idx("TechCategories");

    if ([iTimestamp, iSeverity, iCategory, iImpact, iSourceIndustries, iDueDate, iSourceName, iTitle, iLink].some(i => i === -1)) {
      throw new Error("Alerts sheet missing required columns for YTD scan.");
    }

    const yearStart = new Date(new Date().getFullYear(), 0, 1);
    const list = [];
    const seen = {};
    const YTD_ALERT_SEVERITIES = new Set(["MANDATE", "FINAL_RULE", "ENFORCEMENT"]);

    for (let r = 1; r < data.length; r++) {
      const row = data[r];
      const ts = normalizeDate_(row[iTimestamp]);
      if (!ts || ts < yearStart) continue;

      const sev = String(row[iSeverity] || "").trim().toUpperCase();
      if (!YTD_ALERT_SEVERITIES.has(sev)) continue;

      let techCategories = (iTech === -1) ? "" : String(row[iTech] || "").trim();

      if (!techCategories) {
        try {
          const itemStub = {
            title: String(row[iTitle] || ""),
            description: ""
          };
          const cat = String(row[iCategory] || "");
          const inds = String(row[iSourceIndustries] || "");
          techCategories = classifyTechCategories_(itemStub, inds, cat);
        } catch (e) {
          Logger.log("classifyTechCategories_ failed on row " + (r + 1) + ": " + String(e));
          techCategories = "";
        }
      }

      const fp = buildYtdFingerprint_(
        row[iSourceName],
        row[iTitle],
        row[iLink]
      );
      if (seen[fp]) continue;
      seen[fp] = true;

      list.push([
        sev,
        String(row[iCategory] || "").trim(),
        String(row[iImpact] || "").trim(),
        iImposedDate === -1 ? "" : normalizeDate_(row[iImposedDate]),
        normalizeDate_(row[iDueDate]),
        String(row[iSourceIndustries] || "").trim(),
        iAffectedCompanies === -1 ? "" : String(row[iAffectedCompanies] || "").trim(),
        String(row[iSourceName] || "").trim(),
        String(row[iTitle] || "").trim(),
        String(row[iLink] || "").trim(),
        String(techCategories || "").trim()
      ]);
    }

    Logger.log("YTD_scanAlerts: qualifying items = " + list.length);

    let out = ss.getSheetByName("YTD Alert Snapshot");
    if (!out) out = ss.insertSheet("YTD Alert Snapshot");
    out.clearContents();

    const outHeaders = [
      "Severity",
      "Category",
      "Impact",
      "ImposedDate",
      "DueDate",
      "SourceIndustries",
      "AffectedCompanies",
      "SourceName",
      "Title",
      "Link",
      "TechCategories"
    ];

    out.getRange(1, 1, 1, outHeaders.length).setValues([outHeaders]);
    out.setFrozenRows(1);

    if (list.length) {
      const range = out.getRange(2, 1, list.length, outHeaders.length);
      range.setValues(list);
      range.setWrap(true).setVerticalAlignment("top");

      // Format ImposedDate (column 4) and DueDate (column 5)
      out.getRange(2, 4, list.length, 1).setNumberFormat("MM/dd/yyyy");
      out.getRange(2, 5, list.length, 1).setNumberFormat("MM/dd/yyyy");
    }

    Logger.log("YTD_scanAlerts completed successfully. Wrote " + list.length + " row(s) to YTD Alert Snapshot.");

  } catch (e) {
    Logger.log("YTD_scanAlerts ERROR: " + String(e));
    throw e;
  }
}

function YTD_emailSnapshotSummary() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sh = ss.getSheetByName("YTD Alert Snapshot");
  if (!sh) throw new Error("Missing YTD Alert Snapshot sheet. Run YTD_scanAlerts first.");

  const data = sh.getDataRange().getValues();
  if (data.length < 2) {
    Logger.log("YTD_emailSnapshotSummary: no snapshot rows found.");
    return;
  }

  const headers = data[0].map(h => String(h || "").trim());
  const idx = name => headers.indexOf(name);

  const iSeverity = idx("Severity");
  const iCategory = idx("Category");
  const iSourceIndustries = idx("SourceIndustries");

  const countsBySeverity = {};
  const countsByCategory = {};
  const countsByIndustry = {};

  for (let r = 1; r < data.length; r++) {
    const row = data[r];

    const sev = String(row[iSeverity] || "Unknown").trim() || "Unknown";
    const cat = String(row[iCategory] || "Unknown").trim() || "Unknown";
    const inds = String(row[iSourceIndustries] || "").split(",").map(s => s.trim()).filter(Boolean);

    countsBySeverity[sev] = (countsBySeverity[sev] || 0) + 1;
    countsByCategory[cat] = (countsByCategory[cat] || 0) + 1;

    if (!inds.length) {
      countsByIndustry["Unspecified"] = (countsByIndustry["Unspecified"] || 0) + 1;
    } else {
      inds.forEach(ind => {
        countsByIndustry[ind] = (countsByIndustry[ind] || 0) + 1;
      });
    }
  }

  function topLines_(obj, limit) {
    return Object.entries(obj)
      .sort((a, b) => b[1] - a[1])
      .slice(0, limit)
      .map(([k, v]) => `- ${k}: ${v}`);
  }

  const total = data.length - 1;
  const sevLines = topLines_(countsBySeverity, 10);
  const catLines = topLines_(countsByCategory, 10);
  const indLines = topLines_(countsByIndustry, 10);

  const url = ss.getUrl();
  const sheetUrl = `${url}#gid=${sh.getSheetId()}`;

  const htmlBody = buildHtmlEmailWrapper_({
    title: "YTD Compliance Snapshot Summary",
    subtitle: `${total} qualifying item(s) written to the YTD Alert Snapshot sheet`,
    blocks: [
      `<div style="margin-bottom:14px;">
         <div style="font-weight:800;margin-bottom:6px;">Top Severities</div>
         <div style="font-size:13px;line-height:1.5;">${sanitize_(sevLines.join("\n")).replace(/\n/g, "<br>")}</div>
       </div>`,
      `<div style="margin-bottom:14px;">
         <div style="font-weight:800;margin-bottom:6px;">Top Categories</div>
         <div style="font-size:13px;line-height:1.5;">${sanitize_(catLines.join("\n")).replace(/\n/g, "<br>")}</div>
       </div>`,
      `<div style="margin-bottom:14px;">
         <div style="font-weight:800;margin-bottom:6px;">Top Industries</div>
         <div style="font-size:13px;line-height:1.5;">${sanitize_(indLines.join("\n")).replace(/\n/g, "<br>")}</div>
       </div>`,
      `<div style="margin-top:10px;font-size:13px;">
         Open full snapshot: <a href="${sanitizeUrl_(sheetUrl)}" target="_blank" style="color:#0b5cff;text-decoration:none;">View YTD Alert Snapshot</a>
       </div>`
    ],
    footerNote: "This summary avoids large-email size issues by linking to the full sheet."
  });

  const textBody = [
    "YTD Compliance Snapshot Summary",
    `${total} qualifying item(s) written to the YTD Alert Snapshot sheet.`,
    "",
    "Top Severities:",
    ...sevLines,
    "",
    "Top Categories:",
    ...catLines,
    "",
    "Top Industries:",
    ...indLines,
    "",
    `Open full snapshot: ${sheetUrl}`
  ].join("\n");

  sendStyledEmail_({
    to: CONFIG.EMAIL_TO,
    subject: `${CONFIG.YTD_SUBJECT_PREFIX} ${new Date().getFullYear()} Summary (${total} items)`,
    htmlBody,
    body: textBody
  });

  Logger.log("YTD_emailSnapshotSummary: summary email sent.");
}

function buildYtdFingerprint_(sourceName, title, link) {
  const raw = [
    String(sourceName || "").trim().toLowerCase(),
    String(link || "").trim().toLowerCase(),
    String(title || "").trim().toLowerCase()
  ].join("|");

  const bytes = Utilities.computeDigest(Utilities.DigestAlgorithm.MD5, raw);
  const out = [];

  for (let i = 0; i < bytes.length; i++) {
    let v = bytes[i];
    if (v < 0) v += 256;
    let hex = v.toString(16);
    if (hex.length === 1) hex = "0" + hex;
    out.push(hex);
  }

  return out.join("");
}

function YTD_generateCampaignDrafts() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const alertsSheet = ss.getSheetByName(CONFIG.ALERTS_SHEET);
  if (!alertsSheet) throw new Error(`Missing sheet: ${CONFIG.ALERTS_SHEET}`);

  const data = alertsSheet.getDataRange().getValues();
  if (data.length < 2) return;

  const headers = data[0].map(h => String(h || "").trim());
  const idx = name => headers.indexOf(name);

  const iTimestamp = idx("Timestamp");
  const iSeverity = idx("Severity");
  const iCategory = idx("Category");
  const iImpact = idx("ImpactScore");
  const iSourceIndustries = idx("SourceIndustries");
  const iImposedDate = idx("ImposedDate");
  const iDueDate = idx("DueDate");
  const iAffectedCompanies = idx("AffectedCompanies");
  const iSourceName = idx("SourceName");
  const iTitle = idx("Title");
  const iLink = idx("Link");
  const iTech = idx("TechCategories");

  const yearStart = new Date(new Date().getFullYear(), 0, 1);
  const alerts = [];

  for (let r = 1; r < data.length; r++) {
    const row = data[r];
    const ts = normalizeDate_(row[iTimestamp]);
    if (!ts || ts < yearStart) continue;

    const alert = {
      Timestamp: ts,
      Severity: String(row[iSeverity] || "").trim(),
      Category: String(row[iCategory] || "").trim(),
      ImpactScore: Number(row[iImpact]) || 0,
      SourceIndustries: String(row[iSourceIndustries] || "").trim(),
      ImposedDate: iImposedDate === -1 ? "" : String(row[iImposedDate] || "").trim(),
      DueDate: String(row[iDueDate] || "").trim(),
      AffectedCompanies: iAffectedCompanies === -1 ? "" : String(row[iAffectedCompanies] || "").trim(),
      SourceName: String(row[iSourceName] || "").trim(),
      Title: String(row[iTitle] || "").trim(),
      Link: String(row[iLink] || "").trim(),
      TechCategories: iTech === -1 ? "" : String(row[iTech] || "").trim()
    };

    const noise = evaluateNoiseForAlert_(alert);
    if (noise.excludeFromCampaigns) continue;

    alerts.push(alert);
  }

  if (!alerts.length) {
    Logger.log("No YTD alerts for campaigns after noise filtering.");
    return;
  }

  const grouped = groupAlertsByIndustry_(alerts);
  const rows = [];
  const genTs = new Date();

  Object.keys(grouped).sort().forEach(indKey => {
    const list = grouped[indKey].sort((a,b)=>{
      const sr = severityRank_(a.Severity)-severityRank_(b.Severity);
      if (sr!==0) return sr;
      return (b.ImpactScore||0)-(a.ImpactScore||0);
    });
    const top = list.slice(0,CONFIG.CAMPAIGN_TOP_ALERTS_PER_INDUSTRY);
    const summary = formatAlertBullets_(top);

    ["CISO","CIO","Compliance"].forEach(persona=>{
      const draft = buildPersonaDraft_(persona,indKey,top,summary);
      rows.push([
        genTs,
        industryToDisplay_(indKey),
        persona,
        draft.campaignType,
        draft.subject1,
        draft.subject2,
        draft.subject3,
        draft.emailCopyPaste,
        draft.linkedinDmCopyPaste,
        draft.callScriptCopyPaste,
        draft.primaryCTA,
        draft.alertTitles,
        draft.alertLinks
      ]);
    });
  });

  if (!rows.length) {
    Logger.log("No YTD campaign drafts generated.");
    return;
  }

  let ytdSheet = ss.getSheetByName("YTD Campaigns");
  if (!ytdSheet) ytdSheet = ss.insertSheet("YTD Campaigns");
  writeCampaigns_(ytdSheet, rows);

  const maxRowsPerEmail = 25;
  let part = 1;
  for (let i = 0; i < rows.length; i += maxRowsPerEmail) {
    const chunk = rows.slice(i, i + maxRowsPerEmail);
    emailYTDCampaignPack_(chunk, part);
    part++;
  }
}

/* ======================= YTD EMAIL HELPERS ======================= */

function DEBUG_clearAllDedupeKeys() {
  const props = PropertiesService.getScriptProperties();
  const all = props.getProperties();
  const keep = new Set([
    CONFIG.DIGEST_QUEUE_KEY,
    CONFIG.CURSOR_KEY
  ]);

  let removed = 0;
  Object.keys(all).forEach(key => {
    if (!keep.has(key)) {
      props.deleteProperty(key);
      removed++;
    }
  });

  Logger.log(`Cleared ${removed} dedupe keys (kept cursor + digest).`);
}

function emailYTDCampaignPack_(rows, partNumber) {
  if (!rows || !rows.length) return;

  var subject = "[Campaign YTD] Persona + Industry Drafts";
  if (partNumber && partNumber > 1) {
    subject += " (Part " + partNumber + ")";
  }

  var grouped = {};
  rows.forEach(function(r) {
    var ind = r[1];
    var persona = r[2];
    if (!grouped[ind]) grouped[ind] = {};
    if (!grouped[ind][persona]) grouped[ind][persona] = [];
    grouped[ind][persona].push(r);
  });

  var blocks = [];

  Object.keys(grouped).sort().forEach(function(ind) {
    blocks.push(
      '<div style="margin:18px 0 8px;font-size:16px;font-weight:900;color:#111827;">' +
      sanitize_(industryToDisplay_(ind)) +
      "</div>"
    );

    ["CISO", "CIO", "Compliance"].forEach(function(p) {
      var arr = grouped[ind][p] || [];
      if (!arr.length) return;

      var r = arr[0];

      blocks.push(
        '<div style="border:1px solid #e6e6e6;border-radius:14px;padding:14px;margin:12px 0;background:#ffffff;">' +
          '<div style="display:flex;gap:10px;align-items:center;margin-bottom:8px;">' +
            '<div style="font-weight:900;font-size:14px;">' + sanitize_(p) + "</div>" +
            '<div style="padding:3px 10px;border-radius:999px;background:#eef6ff;color:#134a9a;font-size:12px;font-weight:800;">' +
              sanitize_(r[3]) +
            "</div>" +
          "</div>" +

          '<div style="margin:8px 0;">' +
            '<div style="font-weight:800;margin-bottom:6px;">Subject options</div>' +
            '<ul style="margin:0;padding-left:18px;color:#111827;">' +
              "<li>" + sanitize_(r[4]) + "</li>" +
              "<li>" + sanitize_(r[5]) + "</li>" +
              "<li>" + sanitize_(r[6]) + "</li>" +
            "</ul>" +
          "</div>" +

          '<div style="margin:10px 0;">' +
            '<div style="font-weight:800;margin-bottom:6px;">Email (copy/paste)</div>' +
            '<div style="white-space:pre-wrap;background:#f8fafc;border:1px solid #e5e7eb;border-radius:12px;padding:12px;font-family:Consolas,Menlo,monospace;font-size:12px;line-height:1.45;">' +
              sanitize_(r[7]) +
            "</div>" +
          "</div>" +

          '<div style="margin:10px 0;">' +
            '<div style="font-weight:800;margin-bottom:6px;">LinkedIn DM (copy/paste)</div>' +
            '<div style="white-space:pre-wrap;background:#f8fafc;border:1px solid #e5e7eb;border-radius:12px;padding:12px;font-family:Consolas,Menlo,monospace;font-size:12px;line-height:1.45;">' +
              sanitize_(r[8]) +
            "</div>" +
          "</div>" +

          '<div style="margin:10px 0;">' +
            '<div style="font-weight:800;margin-bottom:6px;">Call script (copy/paste)</div>' +
            '<div style="white-space:pre-wrap;background:#f8fafc;border:1px solid #e5e7eb;border-radius:12px;padding:12px;font-family:Consolas,Menlo,monospace;font-size:12px;line-height:1.45;">' +
              sanitize_(r[9]) +
            "</div>" +
          "</div>" +

          '<div style="margin-top:10px;font-size:13px;">' +
            "<b>CTA:</b> " + sanitize_(r[10]) +
          "</div>" +
        "</div>"
      );
    });
  });

  var htmlBody = buildHtmlEmailWrapper_({
    title: "YTD Campaign Drafts" + (partNumber && partNumber > 1 ? " (Part " + partNumber + ")" : ""),
    subtitle: "Industry + persona variants from YTD alerts",
    blocks: blocks,
    footerNote: "Manual YTD campaign pack"
  });

  var textBody = rows.map(function(r) {
    return [
      "Industry: " + r[1] + " | Persona: " + r[2] + " | Type: " + r[3],
      "Subjects: " + r[4] + " | " + r[5] + " | " + r[6],
      "",
      "Email:",
      r[7],
      "",
      "LinkedIn DM:",
      r[8],
      "",
      "Call script:",
      r[9],
      "CTA: " + r[10],
      ""
    ].join("\n");
  }).join("\n");

  sendStyledEmail_({
    to: CONFIG.EMAIL_TO,
    subject: subject,
    htmlBody: htmlBody,
    body: textBody
  });
}

/* ======================= SETUP / BACKFILL HELPERS ======================= */

function ensureAccountsSheet_() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  let sh = ss.getSheetByName(CONFIG.ACCOUNTS_SHEET);
  if (!sh) sh = ss.insertSheet(CONFIG.ACCOUNTS_SHEET);

  const expected = ["Enabled", "CompanyName", "IndustryTags", "Owner", "Notes"];

  if (sh.getLastRow() === 0) {
    sh.getRange(1, 1, 1, expected.length).setValues([expected]);
    sh.setFrozenRows(1);
    sh.getRange(1, 1, 1, expected.length).setFontWeight("bold");
    return sh;
  }

  const lastCol = sh.getLastColumn();
  const headers = sh.getRange(1, 1, 1, lastCol).getValues()[0].map(h => String(h || "").trim());
  const missing = expected.filter(h => headers.indexOf(h) === -1);

  if (missing.length) {
    sh.getRange(1, lastCol + 1, 1, missing.length).setValues([missing]);
    sh.getRange(1, 1, 1, lastCol + missing.length).setFontWeight("bold");
  }

  return sh;
}

function SETUP_upgradeComplianceTracker() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();

  let alerts = ss.getSheetByName(CONFIG.ALERTS_SHEET);
  if (!alerts) alerts = ss.insertSheet(CONFIG.ALERTS_SHEET);
  ensureAlertsHeaders_(alerts);

  ensureAccountsSheet_();
  ensureSourceHealthSheet_();
  resetAccountsCache_();

  Logger.log("Compliance Tracker upgraded successfully.");
}

function BACKFILL_ImposedDateAndAffectedCompanies_Run() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sh = ss.getSheetByName(CONFIG.ALERTS_SHEET);
  if (!sh) throw new Error("Missing Alerts sheet");

  ensureAlertsHeaders_(sh);
  resetAccountsCache_();

  const data = sh.getDataRange().getValues();
  if (data.length < 2) {
    Logger.log("No data to backfill.");
    return;
  }

  const headers = data[0].map(h => String(h || "").trim());
  const idx = name => headers.indexOf(name);

  const iTitle = idx("Title");
  const iPublished = idx("Published");
  const iCategory = idx("Category");
  const iSourceIndustries = idx("SourceIndustries");
  const iImposed = idx("ImposedDate");
  const iAffected = idx("AffectedCompanies");
  const iAffectedCount = idx("AffectedCompanyCount");

  if ([iTitle, iPublished, iCategory, iSourceIndustries, iImposed, iAffected, iAffectedCount].some(i => i === -1)) {
    throw new Error("Alerts sheet missing one or more required columns for backfill.");
  }

  const imposedOut = [];
  const affectedOut = [];
  const affectedCountOut = [];

  for (let r = 1; r < data.length; r++) {
    const row = data[r];

    const currentImposed = String(row[iImposed] || "").trim();
    const currentAffected = String(row[iAffected] || "").trim();
    const currentAffectedCount = String(row[iAffectedCount] || "").trim();

    let imposed = currentImposed;
    if (!imposed) {
      const itemStub = {
        title: String(row[iTitle] || ""),
        description: "",
        published: String(row[iPublished] || "")
      };
      imposed = extractImposedDate_(itemStub);
    }

    let affectedDisplay = currentAffected;
    let affectedCount = currentAffectedCount;
    if (!affectedDisplay || !affectedCount) {
      const industries = String(row[iSourceIndustries] || "").split(",").map(s => s.trim()).filter(Boolean);
      const affected = findAffectedCompanies_(industries);
      affectedDisplay = affected.display;
      affectedCount = affected.count;
    }

    imposedOut.push([imposed]);
    affectedOut.push([affectedDisplay]);
    affectedCountOut.push([affectedCount]);
  }

  sh.getRange(2, iImposed + 1, imposedOut.length, 1).setValues(imposedOut);
  sh.getRange(2, iAffected + 1, affectedOut.length, 1).setValues(affectedOut);
  sh.getRange(2, iAffectedCount + 1, affectedCountOut.length, 1).setValues(affectedCountOut);

  Logger.log(`Backfilled ${imposedOut.length} alert row(s) for ImposedDate and affected-company fields.`);
}

/* ======================= MENU & UTIL ======================= */

function onOpen() {
  const ui = SpreadsheetApp.getUi();
  ui.createMenu("Compliance YTD")
    .addItem("Run YTD Alert Snapshot", "YTD_scanAlerts")
    .addItem("Run YTD Campaign Drafts", "YTD_generateCampaignDrafts")
    .addSeparator()
    .addItem("Open Source Health", "openSourceHealthSheet")
    .addItem("Build Source Health Summary", "buildSourceHealthSummary")
    .addSeparator()
    .addItem("Run Setup Upgrade", "SETUP_upgradeComplianceTracker")
    .addItem("Backfill Imposed + Affected", "BACKFILL_ImposedDateAndAffectedCompanies_Run")
    .addSeparator()
    .addItem("Run System Check", "RUN_systemCheck")
    .addItem("Open System Check", "OPEN_systemCheckSheet")
    .addToUi();
}

/* ======================= SYSTEM CHECK ======================= */

function RUN_systemCheck() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const results = [];

  function addCheck_(area, checkName, status, details) {
    results.push([
      new Date(),
      area,
      checkName,
      status,
      details || ""
    ]);
  }

  function getHeaders_(sheet) {
    if (!sheet || sheet.getLastRow() < 1 || sheet.getLastColumn() < 1) return [];
    return sheet.getRange(1, 1, 1, sheet.getLastColumn())
      .getValues()[0]
      .map(h => String(h || "").trim());
  }

  function hasHeaders_(headers, required) {
    const missing = required.filter(h => headers.indexOf(h) === -1);
    return {
      ok: missing.length === 0,
      missing: missing
    };
  }

  try {
    addCheck_("Workbook", "Spreadsheet opened", "PASS", ss.getName());

    // Sheets
    const alerts = ss.getSheetByName(CONFIG.ALERTS_SHEET);
    const sources = ss.getSheetByName(CONFIG.SOURCES_SHEET);
    const accounts = ss.getSheetByName(CONFIG.ACCOUNTS_SHEET);
    const sourceHealth = ss.getSheetByName(CONFIG.SOURCE_HEALTH_SHEET);
    const routing = ss.getSheetByName(CONFIG.ROUTING_SHEET);
    const techMap = ss.getSheetByName("Tech Map");
    const noiseControls = ss.getSheetByName("Noise Controls");

    addCheck_("Sheets", "Alerts sheet exists", alerts ? "PASS" : "FAIL", CONFIG.ALERTS_SHEET);
    addCheck_("Sheets", "Sources sheet exists", sources ? "PASS" : "FAIL", CONFIG.SOURCES_SHEET);
    addCheck_("Sheets", "Accounts sheet exists", accounts ? "PASS" : "FAIL", CONFIG.ACCOUNTS_SHEET);
    addCheck_("Sheets", "Source Health sheet exists", sourceHealth ? "PASS" : "WARN", CONFIG.SOURCE_HEALTH_SHEET);
    addCheck_("Sheets", "Routing sheet exists", routing ? "PASS" : "WARN", CONFIG.ROUTING_SHEET);
    addCheck_("Sheets", "Tech Map sheet exists", techMap ? "PASS" : "WARN", "Tech Map");
    addCheck_("Sheets", "Noise Controls sheet exists", noiseControls ? "PASS" : "WARN", "Noise Controls");

    // Alerts headers
    if (alerts) {
      const alertHeaders = getHeaders_(alerts);
      const expectedAlertHeaders = getExpectedAlertsHeaders_();
      const chk = hasHeaders_(alertHeaders, expectedAlertHeaders);
      addCheck_(
        "Alerts",
        "Required headers present",
        chk.ok ? "PASS" : "FAIL",
        chk.ok ? "All required headers found" : ("Missing: " + chk.missing.join(", "))
      );
    }

    // Accounts headers
    if (accounts) {
      const accountHeaders = getHeaders_(accounts);
      const requiredAccountHeaders = ["Enabled", "CompanyName", "IndustryTags", "Owner", "Notes"];
      const chk = hasHeaders_(accountHeaders, requiredAccountHeaders);
      addCheck_(
        "Accounts",
        "Required headers present",
        chk.ok ? "PASS" : "FAIL",
        chk.ok ? "All required headers found" : ("Missing: " + chk.missing.join(", "))
      );

      if (accounts.getLastRow() > 1) {
        addCheck_("Accounts", "Account rows available", "PASS", (accounts.getLastRow() - 1) + " row(s)");
      } else {
        addCheck_("Accounts", "Account rows available", "WARN", "No account rows found");
      }
    }

    // Sources headers and enabled sources
    if (sources) {
      const sourceHeaders = getHeaders_(sources);
      const requiredSourceHeaders = ["Enabled", "SourceName", "FeedURL", "Industries", "Keywords"];
      const chk = hasHeaders_(sourceHeaders, requiredSourceHeaders);
      addCheck_(
        "Sources",
        "Required headers present",
        chk.ok ? "PASS" : "FAIL",
        chk.ok ? "All required headers found" : ("Missing: " + chk.missing.join(", "))
      );

      if (chk.ok) {
        const sourceRows = sources.getDataRange().getValues();
        let enabledCount = 0;
        const iEnabled = sourceHeaders.indexOf("Enabled");
        const iSourceName = sourceHeaders.indexOf("SourceName");

        for (let r = 1; r < sourceRows.length; r++) {
          const enabled = (sourceRows[r][iEnabled] === true || String(sourceRows[r][iEnabled]).toUpperCase() === "TRUE");
          if (enabled) enabledCount++;
        }

        addCheck_(
          "Sources",
          "Enabled sources found",
          enabledCount > 0 ? "PASS" : "FAIL",
          enabledCount + " enabled source(s)"
        );

        if (enabledCount > 0) {
          const loadedSources = getSources_(sources);
          addCheck_(
            "Sources",
            "getSources_() loads successfully",
            loadedSources.length > 0 ? "PASS" : "FAIL",
            loadedSources.length + " source object(s) loaded"
          );
        }
      }
    }

    // Email config
    addCheck_(
      "Email",
      "EMAIL_TO configured",
      CONFIG.EMAIL_TO ? "PASS" : "FAIL",
      CONFIG.EMAIL_TO || "Blank"
    );
    addCheck_(
      "Email",
      "REPLY_TO configured",
      CONFIG.REPLY_TO ? "PASS" : "WARN",
      CONFIG.REPLY_TO || "Blank"
    );
    addCheck_(
      "Email",
      "SEND_EMAIL flag",
      CONFIG.SEND_EMAIL ? "PASS" : "WARN",
      String(CONFIG.SEND_EMAIL)
    );

    // Script properties
    const props = PropertiesService.getScriptProperties();
    const cursor = props.getProperty(CONFIG.CURSOR_KEY);
    const digest = props.getProperty(CONFIG.DIGEST_QUEUE_KEY);

    addCheck_(
      "Properties",
      "Cursor property present",
      cursor !== null ? "PASS" : "WARN",
      cursor !== null ? String(cursor) : "Not set yet"
    );
    addCheck_(
      "Properties",
      "Digest queue property present",
      digest !== null ? "PASS" : "WARN",
      digest !== null ? "Digest queue exists" : "No digest queue yet"
    );

    // Affected company mapping test
    try {
      resetAccountsCache_();
      const sample = findAffectedCompanies_(["healthcare", "banking", "retail"]);
      addCheck_(
        "Accounts Mapping",
        "findAffectedCompanies_() executes",
        "PASS",
        "Count=" + sample.count + (sample.display ? " | " + sample.display : " | No matches yet")
      );
    } catch (e) {
      addCheck_(
        "Accounts Mapping",
        "findAffectedCompanies_() executes",
        "FAIL",
        String(e)
      );
    }

    // Ensure helper setup functions execute
    try {
      ensureSourceHealthSheet_();
      addCheck_("Setup", "ensureSourceHealthSheet_()", "PASS", "Executed successfully");
    } catch (e) {
      addCheck_("Setup", "ensureSourceHealthSheet_()", "FAIL", String(e));
    }

    try {
      ensureAccountsSheet_();
      addCheck_("Setup", "ensureAccountsSheet_()", "PASS", "Executed successfully");
    } catch (e) {
      addCheck_("Setup", "ensureAccountsSheet_()", "FAIL", String(e));
    }

    try {
      if (alerts) {
        ensureAlertsHeaders_(alerts);
        addCheck_("Setup", "ensureAlertsHeaders_()", "PASS", "Executed successfully");
      } else {
        addCheck_("Setup", "ensureAlertsHeaders_()", "WARN", "Alerts sheet missing");
      }
    } catch (e) {
      addCheck_("Setup", "ensureAlertsHeaders_()", "FAIL", String(e));
    }

  } catch (e) {
    results.push([new Date(), "System", "RUN_systemCheck", "FAIL", String(e)]);
  }

  writeSystemCheckResults_(results);
}

function writeSystemCheckResults_(rows) {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheetName = "System Check";
  let sh = ss.getSheetByName(sheetName);
  if (!sh) sh = ss.insertSheet(sheetName);

  sh.clearContents();

  const headers = ["RunTime", "Area", "Check", "Status", "Details"];
  sh.getRange(1, 1, 1, headers.length).setValues([headers]);
  sh.getRange(1, 1, 1, headers.length).setFontWeight("bold");
  sh.setFrozenRows(1);

  if (rows.length) {
    sh.getRange(2, 1, rows.length, headers.length).setValues(rows);
  }

  sh.autoResizeColumns(1, headers.length);

  // Simple coloring
  const lastRow = sh.getLastRow();
  if (lastRow > 1) {
    const statusRange = sh.getRange(2, 4, lastRow - 1, 1);
    const values = statusRange.getValues();
    const bgs = values.map(row => {
      const v = String(row[0] || "").toUpperCase();
      if (v === "PASS") return ["#d9ead3"];
      if (v === "WARN") return ["#fff2cc"];
      return ["#f4cccc"];
    });
    statusRange.setBackgrounds(bgs);
  }
}

function OPEN_systemCheckSheet() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sh = ss.getSheetByName("System Check");
  if (!sh) throw new Error("System Check sheet does not exist yet. Run RUN_systemCheck first.");
  ss.setActiveSheet(sh);
}

/* ======================= Normalize & Backfill ======================= */

function normalizeDate_(val){
  if (val instanceof Date) return val;
  const s=String(val||"").trim();
  if (!s) return null;
  const d=new Date(s);
  if (!isNaN(d.getTime())) return d;
  const m=s.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})/);
  if (m) return new Date(Number(m[3]),Number(m[1])-1,Number(m[2]));
  return null;
}

function BACKFILL_TechCategories() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sh = ss.getSheetByName(CONFIG.ALERTS_SHEET);
  if (!sh) throw new Error("Missing Alerts sheet");

  const data = sh.getDataRange().getValues();
  if (data.length < 2) {
    Logger.log("No data to backfill.");
    return;
  }

  const headers = data[0].map(h => String(h || "").trim());
  const idx = name => headers.indexOf(name);

  const iTitle = idx("Title");
  const iCategory = idx("Category");
  const iSourceIndustries = idx("SourceIndustries");
  const iTech = idx("TechCategories");

  if ([iTitle, iCategory, iSourceIndustries, iTech].some(i => i === -1)) {
    throw new Error("Alerts sheet missing Title/Category/SourceIndustries/TechCategories columns.");
  }

  const out = [];
  for (let r = 1; r < data.length; r++) {
    const row = data[r];
    const current = String(row[iTech] || "").trim();
    if (current) {
      out.push(current);
      continue;
    }

    const title = String(row[iTitle] || "");
    const category = String(row[iCategory] || "");
    const sourceIndustriesStr = String(row[iSourceIndustries] || "");

    const item = { title, description: "" };

    const tech = classifyTechCategories_(item, sourceIndustriesStr, category);
    out.push(tech);
  }

  sh.getRange(2, iTech + 1, out.length, 1).setValues(out.map(v => [v]));
  Logger.log(`Backfilled TechCategories for ${out.length} rows.`);
}

function FIX_addTechHeaderIfMissing() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sh = ss.getSheetByName(CONFIG.ALERTS_SHEET);
  if (!sh) throw new Error("Missing Alerts sheet");

  const lastCol = sh.getLastColumn();
  const headers = sh.getRange(1, 1, 1, lastCol).getValues()[0].map(h => String(h || "").trim());

  if (headers.includes("TechCategories")) {
    Logger.log("TechCategories header already present.");
    return;
  }

  sh.getRange(1, lastCol + 1).setValue("TechCategories");
  Logger.log(`Added header 'TechCategories' in column ${lastCol + 1}.`);
}

function YTD_RESET_STATE(){
  Logger.log("YTD state reset (no side effects).");
}