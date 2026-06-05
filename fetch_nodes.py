import os
import re
import json
import base64
import socket
import logging
import time
import random
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

#from validate_nodes_mihomo import validate_nodes_with_mihomo
#from debug_nodes_mihomo import validate_nodes_with_mihomo

# External Libraries
import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
import yaml

# =========================
# CONFIGURATION
# =========================
SEARCH_KEYWORDS = [
    "v2ray share", "clash subscribe", "free ssr", "trojan subscribe", 
    "warp config", "china nodes", "订阅", "节点", "梯子", "翻墙"
]
MAX_REPOS_PER_KEYWORD = 5
MAX_WORKERS = 150  # Raise workers significantly (GitHub runners can handle up to 200)
TIMEOUT_SECONDS = 1.5  # Drop to 1.5s. Fast/usable nodes respond in under 1 second.
MAX_FILE_SIZE = 1 * 1024 * 1024  # 1MB limit
MAX_PROXIES_PER_FILE = 500
MAX_FOLDER_DEPTH = 3  # Node configs are typically at root or 1-2 levels deep
STALE_DAYS = 30  # Skip files not updated in 30+ days (likely archived/bloat)
MAX_NODES_PER_REPO = 3000  # Sample to this limit if repo extracts more
MAX_FINAL_NODES = 500  # Maximum nodes in final output (nodes.txt)

# Whitelist configuration
WHITELIST_THRESHOLD = 100  # Repos with >= 100 raw nodes are added to whitelist
WHITELIST_REFRESH_DAYS = 7  # Refresh whitelist entries after N days

# Repos to skip - typically frontend-heavy with no node configs
SKIP_REPO_PATTERNS = [
    r"\.github\.io$",  # GitHub Pages sites
    r"blog",
    r"website",
    r"tutorial",
    r"docker",
    r"awesome",
]

OUTPUT_DIR = Path("data")
OUTPUT_DIR.mkdir(exist_ok=True)

# File to track repos with zero nodes (blacklist)
BLACKLIST_FILE = OUTPUT_DIR / "zero_node_repos.json"
# File to track high-value repos with 100+ nodes (whitelist)
WHITELIST_FILE = OUTPUT_DIR / "high_value_repos.json"

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

# Configure a robust HTTP Session with automated retries
session = requests.Session()
retries = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
session.mount("https://", HTTPAdapter(max_retries=retries))

GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
if GITHUB_TOKEN:
    session.headers.update({"Authorization": f"token {GITHUB_TOKEN}"})
    logger.info("Using GitHub API with token authentication")
else:
    logger.warning("No GITHUB_TOKEN provided. Rate limited to 60 requests/hour")
session.headers.update({"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"})

# =========================
# QUALITY TRACKER
# =========================
class RepoTracker:
    def __init__(self):
        self.stats = {}
        self.node_sources = {}  # Track which repo each node came from
    
    def init_repo(self, repo_full_name, stars, forks):
        if repo_full_name not in self.stats:
            self.stats[repo_full_name] = {
                "repository": repo_full_name,
                "stars": stars,
                "forks": forks,
                "extracted_nodes": 0,
                "valid_nodes": 0,
                "quality_score": 0.0
            }

    def add_counts(self, repo_full_name, extracted=0, valid=0):
        if repo_full_name in self.stats:
            self.stats[repo_full_name]["extracted_nodes"] += extracted
            self.stats[repo_full_name]["valid_nodes"] += valid

    def track_node_source(self, node_str, repo_name):
        """Track which repository a node came from"""
        if node_str not in self.node_sources:
            self.node_sources[node_str] = repo_name

    def calculate_and_save(self, path):
        output_list = []
        for name, data in self.stats.items():
            star_score = min(data["stars"] / 50 * 40, 40)
            fork_score = min(data["forks"] / 20 * 20, 20)
            node_score = 40 if data["valid_nodes"] > 0 else 0
            data["quality_score"] = round(star_score + fork_score + node_score, 2)
            output_list.append(data)
            
        output_list.sort(key=lambda x: x["quality_score"], reverse=True)
        try:
            with open(path, "w", encoding="utf-8") as f:
                json.dump({
                    "repositories": output_list, 
                    "updated_at": time.strftime("%Y-%m-%d %H:%M:%S")
                }, f, indent=2)
            logger.info(f"Repository quality stats saved to {path}")
        except IOError as e:
            logger.error(f"Failed to write quality stats: {e}")

tracker = RepoTracker()

# =========================
# BLACKLIST MANAGEMENT
# =========================
def load_zero_node_blacklist():
    """Load the set of repos that previously had zero nodes."""
    if BLACKLIST_FILE.exists():
        try:
            with open(BLACKLIST_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                return set(data.get("zero_node_repos", []))
        except Exception as e:
            logger.warning(f"Failed to load blacklist: {e}")
    return set()

def save_zero_node_blacklist(zero_repos):
    """Save repos with zero nodes to a blacklist file."""
    try:
        with open(BLACKLIST_FILE, "w", encoding="utf-8") as f:
            json.dump({
                "zero_node_repos": sorted(list(zero_repos)),
                "updated_at": time.strftime("%Y-%m-%d %H:%M:%S")
            }, f, indent=2)
        logger.info(f"Saved {len(zero_repos)} zero-node repos to blacklist")
    except IOError as e:
        logger.error(f"Failed to save blacklist: {e}")

def is_repo_blacklisted(repo_name, blacklist):
    """Check if repo is in the zero-node blacklist."""
    return repo_name in blacklist

# =========================
# WHITELIST MANAGEMENT
# =========================
def load_high_value_whitelist():
    """Load the high-value repos whitelist with 100+ nodes."""
    if WHITELIST_FILE.exists():
        try:
            with open(WHITELIST_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                # Convert list to dict keyed by repo name for O(1) lookup
                whitelist_dict = {}
                for entry in data.get("high_value_repos", []):
                    repo_name = entry.get("repository")
                    if repo_name:
                        whitelist_dict[repo_name] = entry
                return whitelist_dict
        except Exception as e:
            logger.warning(f"Failed to load whitelist: {e}")
    return {}

def save_high_value_whitelist(whitelist_dict):
    """Save high-value repos to whitelist file."""
    try:
        # Convert dict back to list for JSON serialization
        whitelist_list = list(whitelist_dict.values())
        whitelist_list.sort(key=lambda x: x.get("extracted_nodes", 0), reverse=True)
        
        with open(WHITELIST_FILE, "w", encoding="utf-8") as f:
            json.dump({
                "high_value_repos": whitelist_list,
                "updated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
                "total_whitelisted_repos": len(whitelist_list),
                "total_cached_nodes": sum(e.get("extracted_nodes", 0) for e in whitelist_list)
            }, f, indent=2)
        logger.info(f"Saved {len(whitelist_list)} high-value repos to whitelist")
    except IOError as e:
        logger.error(f"Failed to save whitelist: {e}")

def should_refresh_whitelist_entry(whitelist_entry):
    """Check if a whitelist entry needs to be refreshed based on age."""
    try:
        last_scan_str = whitelist_entry.get("last_full_scan")
        if not last_scan_str:
            return True
        
        last_scan = datetime.fromisoformat(last_scan_str.replace('Z', '+00:00'))
        refresh_cutoff = datetime.now(last_scan.tzinfo) - timedelta(days=WHITELIST_REFRESH_DAYS)
        
        needs_refresh = last_scan < refresh_cutoff
        if needs_refresh:
            logger.debug(f"Whitelist entry for {whitelist_entry.get('repository')} needs refresh (last scan: {last_scan_str})")
        return needs_refresh
    except Exception as e:
        logger.debug(f"Failed to check whitelist refresh: {e}")
        return True

def check_whitelist(repo_name, whitelist):
    """Check if repo is in whitelist and return entry if not expired."""
    entry = whitelist.get(repo_name)
    if entry and not should_refresh_whitelist_entry(entry):
        return entry
    return None

def extract_from_whitelist_files(whitelist_entry):
    """Extract nodes directly from whitelist-tracked files without tree traversal."""
    nodes = []
    repo_name = whitelist_entry.get("repository")
    node_files = whitelist_entry.get("node_files", [])
    
    if not node_files:
        logger.warning(f"No tracked files for whitelisted repo {repo_name}")
        return nodes
    
    for file_info in node_files:
        url = file_info.get("url")
        path = file_info.get("path", "")
        
        if not url:
            logger.debug(f"Skipping file with no URL: {path}")
            continue
        
        try:
            content_res = session.get(url, timeout=7)
            if content_res.status_code == 200:
                extracted = decode_and_extract(content_res.content, path)
                nodes.extend(extracted)
                logger.debug(f"Extracted {len(extracted)} nodes from whitelist file {path}")
            else:
                logger.debug(f"Failed to fetch whitelist file {path}: HTTP {content_res.status_code}")
        except Exception as e:
            logger.debug(f"Error fetching whitelist file {url}: {e}")
    
    # Apply MAX_NODES_PER_REPO limit to whitelist nodes (需求1)
    if nodes:
        nodes = list(set(nodes))  # Remove duplicates first
        if len(nodes) > MAX_NODES_PER_REPO:
            logger.info(f"Sampling {MAX_NODES_PER_REPO} nodes from {len(nodes)} in whitelist for {repo_name}")
            nodes = random.sample(nodes, MAX_NODES_PER_REPO)
    
    return nodes

def update_whitelist_entry(repo_name, stars, forks, node_files, extracted_count, whitelist):
    """Add or update a repo entry in the whitelist."""
    if extracted_count < WHITELIST_THRESHOLD:
        # Don't add to whitelist if below threshold
        return
    
    entry = {
        "repository": repo_name,
        "stars": stars,
        "forks": forks,
        "extracted_nodes": extracted_count,
        "valid_nodes": 0,  # Will be updated during validation phase
        "quality_score": 0.0,
        "node_files": node_files,
        "last_full_scan": datetime.utcnow().isoformat() + "Z",
        "last_nodes_update": datetime.utcnow().isoformat() + "Z"
    }
    
    whitelist[repo_name] = entry
    logger.info(f"Added {repo_name} to whitelist with {extracted_count} raw nodes")

# =========================
# INTELLIGENT FILE FILTERING LAYER
# =========================
def should_skip_repo(repo_name, blacklist):
    """Check if repo matches skip patterns (likely frontend/bloated) or is blacklisted."""
    # Check blacklist first
    if is_repo_blacklisted(repo_name, blacklist):
        logger.info(f"Skipping {repo_name} (blacklisted - previously had zero nodes)")
        return True
    
    for pattern in SKIP_REPO_PATTERNS:
        if re.search(pattern, repo_name, re.IGNORECASE):
            logger.info(f"Skipping {repo_name} (matches pattern: {pattern})")
            return True
    return False

def calculate_folder_depth(path):
    """Calculate folder depth: root=0, a/b=1, a/b/c=2, etc."""
    return path.count(os.sep)

def is_file_recent(commit_date_str):
    """Check if file was updated within STALE_DAYS."""
    try:
        if not commit_date_str:
            return True  # No date info, assume recent
        
        # Parse ISO format date from GitHub API
        file_date = datetime.fromisoformat(commit_date_str.replace('Z', '+00:00'))
        cutoff_date = datetime.now(file_date.tzinfo) - timedelta(days=STALE_DAYS)
        
        is_recent = file_date >= cutoff_date
        if not is_recent:
            logger.debug(f"File stale: last commit {commit_date_str} > {STALE_DAYS} days ago")
        return is_recent
    except Exception as e:
        logger.debug(f"Failed to check file recency: {e}")
        return True  # Default to accepting if date parsing fails

def is_file_likely_nodes(path, file_size, commit_date=None):
    """Intelligent file filtering based on name, size, depth, and recency."""
    path_lower = path.lower()
    depth = calculate_folder_depth(path)
    
    # RECENCY FILTER: Skip stale files (likely archived/unmaintained)
    if commit_date and not is_file_recent(commit_date):
        logger.debug(f"Skipping {path}: not updated in {STALE_DAYS} days")
        return False
    
    # DEPTH FILTER: Node configs are almost never deeply nested
    if depth > MAX_FOLDER_DEPTH:
        logger.debug(f"Skipping {path}: depth {depth} > {MAX_FOLDER_DEPTH}")
        return False
    
    # SIZE FILTER: Legitimate node configs are <100KB
    if file_size > MAX_FILE_SIZE:
        logger.debug(f"Skipping {path}: size {file_size} bytes > {MAX_FILE_SIZE}")
        return False
    
    # WHITELIST EXTENSIONS: Only parse these file types
    ALLOWED_EXTENSIONS = [".txt", ".yaml", ".yml", ".json", ".sub", ".subscribe"]
    has_node_ext = any(path_lower.endswith(ext) for ext in ALLOWED_EXTENSIONS)
    if not has_node_ext:
        return False
    
    # BLACKLIST PATTERNS: Skip known bloat files regardless of extension
    BLOAT_PATTERNS = [
        r"\.min\.(js|css)$",        # Minified assets
        r"node_modules",             # NPM dependencies
        r"dist/",                    # Build output
        r"build/",                   # Build output
        r"\.next/",                  # Next.js cache
        r"\.webpack",                # Webpack cache
        r"coverage/",                # Test coverage
        r"\.test\.",                 # Test files
        r"\.spec\.",                 # Spec files
        r"(vendor|lib|third_party)", # Vendored code
        r"\.git/",                   # Git metadata
    ]
    for pattern in BLOAT_PATTERNS:
        if re.search(pattern, path_lower):
            logger.debug(f"Skipping {path}: matches bloat pattern {pattern}")
            return False
    
    # SMART NAME HEURISTICS: Prioritize files that look like node configs
    GOOD_NAMES = [
        "node", "proxy", "subscribe", "config", "clash", "v2ray", 
        "trojan", "ssr", "vmess", "vless", "free", "list"
    ]
    has_good_name = any(name in path_lower for name in GOOD_NAMES)
    
    # ACCEPT if: good name + shallow depth, OR minimal bloat signature
    if has_good_name or depth <= 1:
        return True
    
    logger.debug(f"Skipping {path}: no good name patterns and depth {depth} > 1")
    return False

# =========================
# HIGH-PERFORMANCE PARSING LAYER
# =========================
def parse_clash_yaml(yaml_content):
    """Safely extracts nodes from a Clash YAML structure without memory blowup."""
    nodes = []
    try:
        data = yaml.safe_load(yaml_content)
        if not data or not isinstance(data, dict):
            return nodes
        
        proxies = data.get("proxies", [])
        if not isinstance(proxies, list):
            return nodes

        # Enforce a safety ceiling to prevent CPU hanging on abusive files
        for p in proxies[:MAX_PROXIES_PER_FILE]:
            if not isinstance(p, dict):
                continue
            ptype = p.get("type", "").lower()
            server = p.get("server")
            port = p.get("port")
            name = re.sub(r'[\s"\'#]', '_', p.get("name", "node"))  # Clean characters

            if not server or not port:
                continue

            # Map YAML dictionaries back into shareable application protocol links
            if ptype == "ss":
                cipher = p.get("cipher", "")
                password = p.get("password", "")
                userpass = base64.b64encode(f"{cipher}:{password}".encode()).decode()
                nodes.append(f"ss://{userpass}@{server}:{port}#{name}")
            elif ptype == "ssr":
                # Basic SSR string reconstruction
                nodes.append(f"ssr://{server}:{port}::::")
            elif ptype == "vmess":
                vmess_meta = {
                    "v": "2", "ps": name, "add": str(server), "port": str(port),
                    "id": p.get("uuid", ""), "aid": str(p.get("alterId", 0)),
                    "scy": "auto", "net": p.get("network", "tcp"),
                    "type": "none", "host": "", "path": "", "tls": "tls" if p.get("tls") else ""
                }
                v_str = base64.b64encode(json.dumps(vmess_meta).encode()).decode()
                nodes.append(f"vmess://{v_str}")
            elif ptype in ["vless", "trojan", "tuic", "hysteria", "hysteria2"]:
                uuid_or_pass = p.get("uuid") or p.get("password") or ""
                nodes.append(f"{ptype}://{uuid_or_pass}@{server}:{port}#{name}")
    except Exception as e:
        logger.debug(f"YAML parsing error: {e}")
    return nodes

def extract_nodes_from_text(text):
    """Extract proxy protocol links with optimized regex."""
    if not text:
        return []
    # Strict single-pass pattern check targeting valid proxy schemas
    pattern = r'(vmess|vless|ss|ssr|trojan|tuic|hysteria2|hysteria):\/\/[^\s"\':<>#\^]+(?:#[^\s]*)?'
    return [match.group(0) for match in re.finditer(pattern, text, re.IGNORECASE)]

def decode_and_extract(raw_bytes, filename=""):
    """Linear execution parser without nested retry loops."""
    if not raw_bytes or len(raw_bytes) > MAX_FILE_SIZE:
        logger.debug(f"Skipping {filename}: exceeds size limit or empty")
        return []
        
    text = raw_bytes.decode('utf-8', errors='ignore').strip()
    
    # Path A: Target Structured Clash Configurations
    if filename.lower().endswith((".yaml", ".yml")):
        yaml_nodes = parse_clash_yaml(text)
        if yaml_nodes:
            return yaml_nodes

    # Path B: Try Extracting Standard Links Directly
    nodes = extract_nodes_from_text(text)
    if nodes:
        return nodes
    
    # Path C: Single Base64 Fallback Attempt (No recursion loop)
    try:
        # Clean whitespaces that disrupt standard base64 strings
        cleaned_bytes = re.sub(br'\s+', b'', raw_bytes)
        padded = cleaned_bytes + b'=' * (-len(cleaned_bytes) % 4)
        decoded_text = base64.b64decode(padded).decode('utf-8', errors='ignore').strip()
        
        if filename.lower().endswith((".yaml", ".yml")):
            yaml_nodes = parse_clash_yaml(decoded_text)
            if yaml_nodes:
                return yaml_nodes
        return extract_nodes_from_text(decoded_text)
    except Exception as e:
        logger.debug(f"Base64 decode failed for {filename}: {e}")
        
    return []

# =========================
# STRICT HIGH-FIDELITY VALIDATION LAYER
# =========================
# Block common dummy IP networks and public DNS hijacking landing zones
BLOCKED_IP_PREFIXES = (
    "127.", "0.", "10.", "172.16.", "192.168.", "169.254.",  # Private/Local ranges
)
# Add known DNS hijacking/wildcard target landing IPs if your ISP uses them (e.g., placeholder hosts)
BLOCKED_EXACT_IPS = {
    "208.67.222.222", "208.67.220.220",  # OpenDNS block pages if triggered
}

# Global state trackers to drop duplicates instantly across parallel threads
tested_hosts = set()
failed_hosts = set()

def parse_target_host_port(node_str):
    """Extract host and port from various proxy protocol formats (high-speed version)."""
    try:
        payload = node_str.split("://")[-1]
        payload = payload.split("#")[0]  # Strip naming remark tags completely
        
        if "vmess://" in node_str:
            try:
                # Add base64 padding fallback safely
                decoded_vmess = json.loads(base64.b64decode(payload + '=' * (-len(payload) % 4)).decode('utf-8'))
                return decoded_vmess.get('add'), int(decoded_vmess.get('port', 0))
            except:
                pass

        if "@" in payload:
            payload = payload.split("@")[-1]
            
        host_port = payload.split("?")[0]
        if ":" in host_port:
            parts = host_port.split(":")
            return parts[0], int(parts[1])
    except:
        pass
    return None, None

def test_tcp(node_str):
    """Test if a node's host:port is reachable via TCP with DNS resolution and IP filtering."""
    host, port = parse_target_host_port(node_str)
    if not host or not port:
        return node_str, False
        
    # Block local loopbacks and typical mock string targets
    if host in ["127.0.0.1", "0.0.0.0", "localhost"] or ".example." in host:
        return node_str, False

    host_key = f"{host}:{port}"
    
    # Early Check: If this exact IP and Port already failed in another thread, drop it instantly
    if host_key in failed_hosts:
        return node_str, False

    try:
        # CRITICAL: Resolve domain to an IP address first to evaluate its destination authenticity
        resolved_ip = socket.gethostbyname(host)
        
        # Filter out internal loopbacks or generic structural dummy targets
        if any(resolved_ip.startswith(prefix) for prefix in BLOCKED_IP_PREFIXES) or resolved_ip in BLOCKED_EXACT_IPS:
            failed_hosts.add(host_key)
            return node_str, False

        # Establish the strict TCP network socket handshake
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(TIMEOUT_SECONDS)
        sock.connect((resolved_ip, port))
        sock.close()
        
        return node_str, True
    except:
        # Record failure signature globally
        failed_hosts.add(host_key)
        return node_str, False

# =========================
# MAIN EXECUTIVE LOOP
# =========================
def main():
    logger.info("Starting upgraded node generation process...")
    
    # Load blacklist of repos with zero nodes
    zero_node_blacklist = load_zero_node_blacklist()
    logger.info(f"Loaded {len(zero_node_blacklist)} repos from zero-node blacklist")
    
    # Load whitelist of high-value repos with 100+ nodes
    high_value_whitelist = load_high_value_whitelist()
    logger.info(f"Loaded {len(high_value_whitelist)} repos from high-value whitelist")
    
    unique_raw_nodes = set()
    processed_repos = set()
    zero_node_repos = set()  # Track new repos with zero nodes this run
    whitelist_update = {}  # Track repos to add to whitelist
    whitelist_hits = 0  # Track whitelist cache hits
    current_year = time.strftime("%Y")

    for keyword in SEARCH_KEYWORDS:
        # Use GitHub API for searching repositories
        api_url = "https://api.github.com/search/repositories"
        params = {
            "q": f"{keyword} pushed:>{current_year}-01-01 sort:updated",
            "per_page": MAX_REPOS_PER_KEYWORD
        }
        
        try:
            logger.info(f"Searching for keyword: {keyword}")
            res = session.get(api_url, params=params, timeout=10)
            if res.status_code != 200:
                logger.warning(f"Search failed for '{keyword}': HTTP {res.status_code}")
                continue
            data = res.json()
        except requests.RequestException as e:
            logger.error(f"Failed to query keyword '{keyword}': {e}")
            continue

        for item in data.get("items", []):
            repo_name = item["full_name"]
            
            # Skip repo if it matches bloat patterns or is blacklisted
            if should_skip_repo(repo_name, zero_node_blacklist):
                continue
            
            if repo_name in processed_repos:
                continue
            processed_repos.add(repo_name)

            tracker.init_repo(repo_name, item["stargazers_count"], item["forks_count"])
            logger.info(f"Targeting repository: {repo_name}")

            repo_raw_nodes = []
            
            # ===== NEW: Check whitelist first =====
            whitelist_entry = check_whitelist(repo_name, high_value_whitelist)
            if whitelist_entry:
                logger.info(f"✓ Using cached whitelist entry for {repo_name} (cached {whitelist_entry.get('extracted_nodes')} nodes)")
                repo_raw_nodes = extract_from_whitelist_files(whitelist_entry)
                if repo_raw_nodes:
                    whitelist_hits += 1
                    tracker.add_counts(repo_name, extracted=len(repo_raw_nodes))
                    unique_raw_nodes.update(repo_raw_nodes)
                    for node in repo_raw_nodes:
                        tracker.track_node_source(node, repo_name)
                    logger.info(f" -> Retrieved {len(repo_raw_nodes)} cached nodes from whitelist")
                continue
            # ===== END: Whitelist check =====

            # If not in whitelist or needs refresh, do full tree traversal
            try:
                branch = item['default_branch']
                # Get repository info including tree SHA
                repo_api_url = f"https://api.github.com/repos/{repo_name}"
                repo_res = session.get(repo_api_url, timeout=10)
                if repo_res.status_code != 200:
                    logger.warning(f"Failed to fetch repo info for {repo_name}")
                    continue
                
                repo_data = repo_res.json()
                # Get the tree SHA from the default branch
                branch_url = f"https://api.github.com/repos/{repo_name}/branches/{branch}"
                branch_res = session.get(branch_url, timeout=10)
                if branch_res.status_code != 200:
                    logger.warning(f"Failed to fetch branch info for {repo_name}/{branch}")
                    continue
                
                sha = branch_res.json()["commit"]["commit"]["tree"]["sha"]

                # Get tree with all files
                files_url = f"https://api.github.com/repos/{repo_name}/git/trees/{sha}?recursive=1"
                files_res = session.get(files_url, timeout=10)
                if files_res.status_code != 200:
                    logger.warning(f"Failed to fetch file tree for {repo_name}")
                    continue
                files_data = files_res.json()
            except Exception as e:
                logger.error(f"Error fetching metadata for {repo_name}: {e}")
                continue

            node_files_info = []  # Track files that contributed nodes for whitelist
            for file_obj in files_data.get("tree", []):
                path = file_obj.get("path", "")
                file_size = file_obj.get("size", 0)
                
                # INTELLIGENT FILTERING: depth + size + name + patterns + recency
                if not is_file_likely_nodes(path, file_size):
                    continue
                
                # Use raw.githubusercontent.com for raw file content
                raw_url = f"https://raw.githubusercontent.com/{repo_name}/{branch}/{path}"
                try:
                    content_res = session.get(raw_url, timeout=7)
                    if content_res.status_code == 200:
                        extracted = decode_and_extract(content_res.content, path)
                        if extracted:
                            repo_raw_nodes.extend(extracted)
                            # Record this file for whitelist tracking
                            node_files_info.append({
                                "path": path,
                                "node_count": len(extracted),
                                "last_checked": datetime.utcnow().isoformat() + "Z",
                                "url": raw_url
                            })
                            for node in extracted:
                                tracker.track_node_source(node, repo_name)
                except Exception as e:
                    logger.debug(f"Failed to fetch {raw_url}: {e}")
                    continue

            if repo_raw_nodes:
                repo_raw_nodes = list(set(repo_raw_nodes))
                
                # If repo has >MAX_NODES_PER_REPO nodes, randomly sample
                if len(repo_raw_nodes) > MAX_NODES_PER_REPO:
                    logger.info(f"Sampling {MAX_NODES_PER_REPO} nodes from {len(repo_raw_nodes)} in {repo_name}")
                    repo_raw_nodes = random.sample(repo_raw_nodes, MAX_NODES_PER_REPO)
                
                tracker.add_counts(repo_name, extracted=len(repo_raw_nodes))
                unique_raw_nodes.update(repo_raw_nodes)
                logger.info(f" -> Found {len(repo_raw_nodes)} raw nodes from {repo_name}.")
                
                # ===== NEW: Add to whitelist if exceeds threshold =====
                if len(repo_raw_nodes) >= WHITELIST_THRESHOLD:
                    whitelist_update[repo_name] = {
                        "repo_name": repo_name,
                        "stars": item["stargazers_count"],
                        "forks": item["forks_count"],
                        "extracted_count": len(repo_raw_nodes),
                        "node_files": node_files_info
                    }
                    logger.info(f"✓ Marked {repo_name} for whitelist (has {len(repo_raw_nodes)} nodes)")
                # ===== END: Whitelist tracking =====
            else:
                # Track repos with zero nodes
                zero_node_repos.add(repo_name)
                logger.info(f" -> No nodes found in {repo_name} (will be blacklisted)")

    # ===== NEW: Update whitelist with newly discovered high-value repos =====
    for repo_name, repo_info in whitelist_update.items():
        update_whitelist_entry(
            repo_info["repo_name"],
            repo_info["stars"],
            repo_info["forks"],
            repo_info["node_files"],
            repo_info["extracted_count"],
            high_value_whitelist
        )
    # ===== END: Whitelist update =====

    # Update and save the blacklist with new zero-node repos
    zero_node_blacklist.update(zero_node_repos)
    save_zero_node_blacklist(zero_node_blacklist)
    
    # Save updated whitelist
    save_high_value_whitelist(high_value_whitelist)
    logger.info(f"Whitelist cache hits this run: {whitelist_hits}")

    raw_node_list = list(unique_raw_nodes)
    logger.info(f"Total unique raw nodes found: {len(raw_node_list)}.")
    
    node_all_path = OUTPUT_DIR / "nodeALL.txt"
    try:
        with open(node_all_path, "w", encoding="utf-8") as f:
            f.write("\n".join(raw_node_list))
        logger.info(f"Wrote {len(raw_node_list)} raw nodes to nodeALL.txt")
    except IOError as e:
        logger.error(f"Failed to write raw nodes file: {e}")
 

    valid_nodes_list = []
    mihomo_results = False
    
    if mihomo_results:
        # Mihomo 成功
        valid_nodes_list = mihomo_results
        logger.info(f"Mihomo validation completed: {len(valid_nodes_list)} valid nodes")
        for node_str in valid_nodes_list:
            source_repo = tracker.node_sources.get(node_str)
            if source_repo:
                tracker.add_counts(source_repo, valid=1)
    else:
        # 仅在 Mihomo 失败或为空时，降级到 TCP 验证 (作为真正的 Fallback)
        logger.warning("Mihomo unavailable, falling back to TCP validation...")
        
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(test_tcp, node): node for node in raw_node_list}
            for future in as_completed(futures):
                try:
                    node_str, is_valid = future.result()
                    if is_valid:
                        valid_nodes_list.append(node_str)
                        source_repo = tracker.node_sources.get(node_str)
                        if source_repo:
                            tracker.add_counts(source_repo, valid=1)
                except Exception as e:
                    logger.error(f"Error processing validation result: {e}")
        logger.info(f"TCP validation completed: {len(valid_nodes_list)} valid nodes")

    logger.info(f"Validated {len(valid_nodes_list)} working nodes out of {len(raw_node_list)}")

    # Write output files once after all processing is complete
    try:
        # Apply MAX_FINAL_NODES limit (需求2): If more than MAX_FINAL_NODES, randomly sample
        final_nodes_list = valid_nodes_list
        if len(final_nodes_list) > MAX_FINAL_NODES:
            logger.info(f"Sampling {MAX_FINAL_NODES} nodes from {len(final_nodes_list)} for final output")
            final_nodes_list = random.sample(final_nodes_list, MAX_FINAL_NODES)
        
        # Base64-encode verified items into data/nodes.txt
        valid_payload_string = "\n".join(final_nodes_list)
        base64_encoded_bytes = base64.b64encode(valid_payload_string.encode('utf-8'))
        with open(OUTPUT_DIR / "nodes.txt", "wb") as f:
            f.write(base64_encoded_bytes)
        logger.info(f"Wrote {len(final_nodes_list)} verified nodes (base64-encoded) to nodes.txt")

        # Save repository quality metrics
        tracker.calculate_and_save(OUTPUT_DIR / "repository_quality.json")
        logger.info("Processing complete! Data output written to 'data/' folder.")
    except IOError as e:
        logger.error(f"Failed to write output files: {e}")

if __name__ == "__main__":
    main()
