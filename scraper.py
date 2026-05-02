import re
from collections import Counter
from hashlib import sha256
from urllib.parse import parse_qsl, urljoin, urlparse

from bs4 import BeautifulSoup

from crawl_analytics import STOP_WORDS, get_analytics
from utils import normalize


ALLOWED_DOMAINS = (
    "ics.uci.edu",
    "cs.uci.edu",
    "informatics.uci.edu",
    "stat.uci.edu",
)

NON_HTML_EXTENSIONS = re.compile(
    r".*\.(css|js|bmp|gif|jpe?g|ico|png|tiff?|mid|mp2|mp3|mp4|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso|epub|dll|cnf|tgz|sha1|thmx|mso|arff|rtf|jar|csv|rm|smil|wmv|swf|wma|zip|rar|gz|rss|xml)$"
)

TOKEN_PATTERN = re.compile(r"[a-z0-9]+(?:'[a-z0-9]+)?")
TRAP_QUERY_KEYS = {
    "do", "eventdisplay", "ical", "idx", "ns", "people", "replytocom",
    "share", "print", "output", "format", "orderby", "sort", "sessionid",
}
TRAP_PATH_SNIPPETS = (
    "/wp-content/",
    "/wp-json/",
    "/calendar/",
    "/events/day/",
    "/events/list/",
    "/events/month/",
    "/events/today/",
    "/events/week/",
)
BLOCKED_HOSTS = {
    "myip.ics.uci.edu",
    "swiki.ics.uci.edu",
    "wiki.ics.uci.edu",
}


def scraper(url, resp):
    links = extract_next_links(url, resp)
    return [link for link in links if is_valid(link)]


def extract_next_links(url, resp):
    # Implementation required.
    # url: the URL that was used to get the page
    # resp.url: the actual url of the page
    # resp.status: the status code returned by the server. 200 is OK, you got the page. Other numbers mean that there was some kind of problem.
    # resp.error: when status is not 200, you can check the error here, if needed.
    # resp.raw_response: this is where the page actually is. More specifically, the raw_response has two parts:
    #         resp.raw_response.url: the url, again
    #         resp.raw_response.content: the content of the page!
    # Return a list with the hyperlinks (as strings) scrapped from resp.raw_response.content
    if resp.status != 200 or resp.raw_response is None:
        return []

    raw_response = resp.raw_response
    content_type = raw_response.headers.get("Content-Type", "").lower()
    if "text/html" not in content_type and "application/xhtml+xml" not in content_type:
        return []

    content = raw_response.content
    if not content:
        return []

    soup = BeautifulSoup(content, "html.parser")
    for tag_name in ("script", "style", "noscript", "svg", "iframe"):
        for tag in soup.find_all(tag_name):
            tag.decompose()

    text = soup.get_text(" ", strip=True)
    all_tokens = tokenize(text)
    indexable_tokens = [token for token in all_tokens if token not in STOP_WORDS]
    page_url = getattr(raw_response, "url", None) or resp.url or url
    requested_url = normalize(resp.url or url)
    canonical_url = requested_url

    analytics = get_analytics()
    page_signature = sha256(" ".join(all_tokens).encode("utf-8")).hexdigest()
    duplicate_content = analytics.has_seen_content(page_signature)
    analytics.record_page(canonical_url, all_tokens, indexable_tokens, page_signature)

    if not should_follow_links(canonical_url, all_tokens):
        return []

    extracted_links = []
    seen_links = set()
    if duplicate_content:
        return extracted_links

    for anchor in soup.find_all("a", href=True):
        absolute_url = normalize(urljoin(page_url, anchor["href"]))
        if absolute_url not in seen_links and is_valid(absolute_url):
            seen_links.add(absolute_url)
            extracted_links.append(absolute_url)

    return extracted_links


def tokenize(text):
    return TOKEN_PATTERN.findall(text.lower())


def should_follow_links(url, tokens):
    if len(tokens) < 5:
        return False

    parsed = urlparse(url)
    segments = [segment for segment in parsed.path.lower().split("/") if segment]
    if len(segments) > 12:
        return False

    counts = Counter(segments)
    if counts and counts.most_common(1)[0][1] >= 4:
        return False

    return True


def has_allowed_domain(hostname):
    if not hostname:
        return False
    hostname = hostname.lower()
    if hostname in BLOCKED_HOSTS:
        return False
    return any(
        hostname == domain or hostname.endswith(f".{domain}")
        for domain in ALLOWED_DOMAINS
    )


def is_trap_url(parsed):
    if len(parsed.geturl()) > 300:
        return True

    path = parsed.path.lower()
    if any(snippet in path for snippet in TRAP_PATH_SNIPPETS):
        return True

    if parsed.hostname in {"ics.uci.edu", "www.ics.uci.edu"} and path.startswith("/people"):
        return True

    segments = [segment for segment in path.split("/") if segment]
    if len(segments) > 12:
        return True

    counts = Counter(segments)
    if counts and counts.most_common(1)[0][1] >= 4:
        return True

    query_pairs = parse_qsl(parsed.query, keep_blank_values=True)
    if len(query_pairs) > 5 or len(parsed.query) > 200:
        return True

    query_keys = [key.lower() for key, _ in query_pairs]
    if any(key in TRAP_QUERY_KEYS for key in query_keys):
        return True

    if any(key.startswith("filter[") or key.startswith("tribe__") for key in query_keys):
        return True

    if len(query_keys) != len(set(query_keys)):
        return True

    return False


def is_valid(url):
    # Decide whether to crawl this url or not.
    # If you decide to crawl it, return True; otherwise return False.
    # There are already some conditions that return False.
    try:
        parsed = urlparse(url)
        if parsed.scheme not in {"http", "https"}:
            return False

        if not has_allowed_domain(parsed.hostname):
            return False

        normalized_url = normalize(url)
        normalized_parsed = urlparse(normalized_url)
        if NON_HTML_EXTENSIONS.match(normalized_parsed.path.lower()):
            return False

        if is_trap_url(normalized_parsed):
            return False

        return True

    except TypeError:
        print("TypeError for ", url)
        raise
