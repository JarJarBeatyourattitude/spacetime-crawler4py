import atexit
import glob
import json
import os
import shelve
from collections import Counter
from urllib.parse import urlparse


ANALYTICS_DB_BASE = "crawl_analytics_store"
REPORT_JSON_PATH = "crawl_report.json"
REPORT_TEXT_PATH = "crawl_report.txt"


STOP_WORDS = {
    "a", "about", "above", "after", "again", "against", "all", "am", "an",
    "and", "any", "are", "as", "at", "be", "because", "been", "before",
    "being", "below", "between", "both", "but", "by", "can", "could", "did",
    "do", "does", "doing", "down", "during", "each", "few", "for", "from",
    "further", "had", "has", "have", "having", "he", "her", "here", "hers",
    "herself", "him", "himself", "his", "how", "i", "if", "in", "into", "is",
    "it", "its", "itself", "just", "me", "more", "most", "my", "myself", "no",
    "nor", "not", "now", "of", "off", "on", "once", "only", "or", "other",
    "our", "ours", "ourselves", "out", "over", "own", "s", "same", "she",
    "should", "so", "some", "such", "t", "than", "that", "the", "their",
    "theirs", "them", "themselves", "then", "there", "these", "they", "this",
    "those", "through", "to", "too", "under", "until", "up", "very", "was",
    "we", "were", "what", "when", "where", "which", "while", "who", "whom",
    "why", "will", "with", "you", "your", "yours", "yourself", "yourselves",
}


_ANALYTICS = None


def cleanup_analytics_files():
    global _ANALYTICS
    if _ANALYTICS is not None:
        _ANALYTICS.close()
        _ANALYTICS = None

    for path in glob.glob(f"{ANALYTICS_DB_BASE}*"):
        try:
            os.remove(path)
        except FileNotFoundError:
            pass

    for path in (REPORT_JSON_PATH, REPORT_TEXT_PATH):
        try:
            os.remove(path)
        except FileNotFoundError:
            pass


class AnalyticsStore:
    def __init__(self):
        self._db = shelve.open(ANALYTICS_DB_BASE)
        self.pages = set(self._db.get("pages", set()))
        self.subdomains = Counter(self._db.get("subdomains", {}))
        self.word_counts = Counter(self._db.get("word_counts", {}))
        self.longest_page = self._db.get(
            "longest_page",
            {"url": None, "word_count": 0},
        )
        self.content_hashes = set(self._db.get("content_hashes", set()))
        atexit.register(self.close)
        self.write_reports()

    def close(self):
        if getattr(self, "_db", None) is None:
            return
        self._persist()
        self._db.close()
        self._db = None

    def has_seen_content(self, content_hash):
        return content_hash in self.content_hashes

    def record_page(self, url, all_tokens, indexable_tokens, content_hash):
        if url in self.pages:
            return

        self.pages.add(url)
        hostname = urlparse(url).hostname or ""
        if hostname.endswith(".uci.edu") or hostname == "uci.edu":
            self.subdomains[hostname] += 1

        self.word_counts.update(indexable_tokens)
        if len(all_tokens) > self.longest_page["word_count"]:
            self.longest_page = {"url": url, "word_count": len(all_tokens)}

        self.content_hashes.add(content_hash)
        self._persist()
        self.write_reports()

    def _persist(self):
        if self._db is None:
            return
        self._db["pages"] = self.pages
        self._db["subdomains"] = dict(self.subdomains)
        self._db["word_counts"] = dict(self.word_counts)
        self._db["longest_page"] = self.longest_page
        self._db["content_hashes"] = self.content_hashes
        self._db.sync()

    def write_reports(self):
        top_words = self.word_counts.most_common(50)
        subdomains = sorted(self.subdomains.items())
        payload = {
            "unique_pages": len(self.pages),
            "longest_page": self.longest_page,
            "top_50_words": top_words,
            "subdomains": [
                {"subdomain": subdomain, "unique_pages": count}
                for subdomain, count in subdomains
            ],
        }
        with open(REPORT_JSON_PATH, "w", encoding="utf-8") as json_file:
            json.dump(payload, json_file, indent=2)

        lines = [
            f"Unique pages: {len(self.pages)}",
            "",
            "Longest page:",
            f"{self.longest_page['url']}, {self.longest_page['word_count']}",
            "",
            "Top 50 words:",
        ]
        lines.extend(f"{word}, {count}" for word, count in top_words)
        lines.append("")
        lines.append(f"Subdomains: {len(subdomains)}")
        lines.append("Subdomain counts:")
        lines.extend(f"{subdomain}, {count}" for subdomain, count in subdomains)

        with open(REPORT_TEXT_PATH, "w", encoding="utf-8") as report_file:
            report_file.write("\n".join(lines) + "\n")


def get_analytics():
    global _ANALYTICS
    if _ANALYTICS is None:
        _ANALYTICS = AnalyticsStore()
    return _ANALYTICS
