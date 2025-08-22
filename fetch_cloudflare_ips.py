# -*- coding: utf-8 -*-
"""
Cloudflare 优选IP自动抓取脚本
--------------------------------
- 支持静态/动态网页抓取，自动去重、排序、地区过滤、排除等功能
- 配置灵活，支持多数据源、CSS选择器、IP数量限制、地区API等
- 日志详细，异常处理健壮，兼容多平台
- 新增IP有效性验证，过滤无效地址
"""

# ===== 标准库导入 =====
import os
import re
import logging
import argparse
import time
from typing import List, Set, Optional, Dict, Any, Union, Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
import ipaddress  # 用于支持CIDR格式网段判断
import threading
from functools import wraps
import json

# ===== 第三方库导入 =====
try:
    import requests
    from requests.adapters import HTTPAdapter, Retry
    from bs4 import BeautifulSoup
    from playwright.sync_api import sync_playwright, Page
    import asyncio
    import aiohttp
except ImportError as e:
    print(f"缺少依赖: {e}. 请先运行 pip install -r requirements.txt 并安装playwright浏览器。")
    raise

# ===== 可选依赖（兼容性处理） =====
try:
    import yaml
except ImportError:
    yaml = None
    print("未检测到 PyYAML，请先运行 pip install pyyaml")

# ===== 常量定义 =====
USER_AGENT: str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
DEFAULT_JS_TIMEOUT: int = 30000
DEFAULT_WAIT_TIMEOUT: int = 5000
MIN_IP_BLOCK: int = 3
MAX_THREAD_NUM: int = 4

# ===== 配置区 =====
# 所有配置均从 config.yaml 读取，缺失项直接报错
# 详见README.md和config.yaml注释

def load_config(config_path: str = 'config.yaml') -> Dict[str, Any]:
    """
    读取并校验 config.yaml 配置文件。
    :param config_path: 配置文件路径
    :return: 配置字典
    :raises RuntimeError, FileNotFoundError, ValueError, KeyError: 配置异常
    """
    if not yaml:
        raise RuntimeError('未检测到 PyYAML，请先运行 pip install pyyaml')
    if not os.path.exists(config_path):
        raise FileNotFoundError('未找到 config.yaml 配置文件，请先创建')
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
            if not isinstance(config, dict):
                raise ValueError('config.yaml 格式错误，需为字典结构')
            required = ['sources', 'pattern', 'output', 'timeout', 'log', 'max_workers', 'log_level', 'js_retry', 'js_retry_interval']
            for k in required:
                if k not in config:
                    raise KeyError(f'config.yaml 缺少必需字段: {k}')
            # 兼容sources为字符串或字典
            new_sources = []
            for item in config['sources']:
                if isinstance(item, str):
                    new_sources.append({
                        'url': item, 
                        'selector': None,
                        'page_type': None,
                        'wait_time': DEFAULT_WAIT_TIMEOUT,
                        'actions': None,
                        'extra_headers': None,
                        'response_format': None,
                        'json_path': None
                    })
                elif isinstance(item, dict):
                    new_sources.append({
                        'url': item['url'], 
                        'selector': item.get('selector'),
                        'page_type': item.get('page_type'),
                        'wait_time': item.get('wait_time', DEFAULT_WAIT_TIMEOUT),
                        'actions': item.get('actions'),
                        'extra_headers': item.get('extra_headers'),
                        'response_format': item.get('response_format'),
                        'json_path': item.get('json_path')
                    })
                else:
                    raise ValueError('sources 列表元素必须为字符串或包含url/selector的字典')
            config['sources'] = new_sources
            # 其他默认值...（保持原有逻辑）
            if 'max_ips_per_url' not in config:
                config['max_ips_per_url'] = 0
            if 'per_url_limit_mode' not in config:
                config['per_url_limit_mode'] = 'random'
            if 'exclude_ips' not in config:
                config['exclude_ips'] = []
            if 'allowed_regions' not in config:
                config['allowed_regions'] = []
            if 'ip_geo_api' not in config:
                config['ip_geo_api'] = ''
            # 新增配置项默认值
            if 'auto_detect' not in config:
                config['auto_detect'] = True
            if 'xpath_support' not in config:
                config['xpath_support'] = False
            if 'follow_redirects' not in config:
                config['follow_redirects'] = True
            if 'enable_telegram_notification' not in config:
                config['enable_telegram_notification'] = False
            return config
    except Exception as e:
        raise RuntimeError(f"读取配置文件失败: {e}")

# ---------------- 日志配置 ----------------
def setup_logging(log_file: str, log_level: str = 'INFO') -> None:
    """
    配置日志输出到文件和控制台。
    :param log_file: 日志文件名
    :param log_level: 日志等级（如INFO、DEBUG等）
    """
    level = getattr(logging, log_level.upper(), logging.INFO)
    logging.basicConfig(
        level=level,
        format='%(asctime)s %(levelname)s %(message)s',
        handlers=[
            logging.FileHandler(log_file, mode='w', encoding='utf-8'),
            logging.StreamHandler()
        ]
    )

# ---------------- 工具函数 ----------------
# ===== 新增：验证IPv4地址有效性 =====
def is_valid_ipv4(ip_str: str) -> bool:
    """检查是否为有效的IPv4地址"""
    try:
        ipaddress.IPv4Address(ip_str)
        return True
    except ipaddress.AddressValueError:
        return False

def extract_ips(text: str, pattern: str) -> List[str]:
    """
    从文本中提取所有IP地址，并保持原始顺序。
    :param text: 输入文本
    :param pattern: IP正则表达式
    :return: IP列表 (按找到的顺序)
    """
    # 使用正则表达式提取所有IP，顺序与原文一致
    raw_ips = re.findall(pattern, text)
    # 过滤无效IP地址 (如64.110.104.301)
    valid_ips = [ip for ip in raw_ips if is_valid_ipv4(ip)]
    invalid_count = len(raw_ips) - len(valid_ips)
    if invalid_count > 0:
        logging.warning(f"[VALIDATION] 发现 {invalid_count} 个无效IP地址已被过滤")
    return valid_ips

def save_ips(ip_list: List[str], filename: str) -> None:
    """
    保存IP列表到文件，保持顺序。
    :param ip_list: IP列表
    :param filename: 输出文件名
    """
    try:
        with open(filename, 'w', encoding='utf-8') as file:
            for ip in ip_list:
                file.write(ip + '\n')
        logging.info(f"共保存 {len(ip_list)} 个唯一IP到 {filename}")
    except Exception as e:
        logging.error(f"写入文件失败: {filename}，错误: {e}")

# ---------------- requests重试配置 ----------------
def get_retry_session(timeout: int) -> requests.Session:
    """
    获取带重试机制的requests.Session。
    :param timeout: 超时时间
    :return: 配置好的Session
    """
    session = requests.Session()
    retries = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retries)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    session.request = lambda *args, **kwargs: requests.Session.request(session, *args, timeout=timeout, **kwargs)
    return session

# ---------------- 智能抓取 ----------------
def extract_ips_from_html(html: str, pattern: str, selector: Optional[str] = None) -> List[str]:
    """
    智能提取IP，优先用selector，其次自动检测IP密集块，最后全局遍历。
    :param html: 网页HTML
    :param pattern: IP正则
    :param selector: 可选，CSS选择器
    :return: IP列表（顺序与页面一致）
    """
    soup = BeautifulSoup(html, 'html.parser')
    # 1. 优先用selector
    if selector:
        selected = soup.select(selector)
        if selected:
            ip_list = []
            for elem in selected:
                ip_list.extend(extract_ips(elem.get_text(), pattern))
            if ip_list:
                logging.info(f"[EXTRACT] 使用selector '{selector}' 提取到{len(ip_list)}个IP")
                return list(dict.fromkeys(ip_list))
    # 2. 自动检测IP密集块
    candidates = []
    for tag in ['pre', 'code', 'table', 'div', 'section', 'article']:
        for elem in soup.find_all(tag):
            text = elem.get_text()
            ips = extract_ips(text, pattern)
            if len(ips) >= MIN_IP_BLOCK:
                candidates.append((len(ips), ips))
    if candidates:
        candidates.sort(reverse=True)
        ip_list = candidates[0][1]
        logging.info(f"[EXTRACT] 自动检测到IP密集块({len(ip_list)}个IP, tag优先级)")
        return list(dict.fromkeys(ip_list))
    # 3. 全局遍历
    all_text = soup.get_text()
    ip_list = extract_ips(all_text, pattern)
    logging.info(f"[EXTRACT] 全局遍历提取到{len(ip_list)}个IP")
    return list(dict.fromkeys(ip_list))

def fetch_ip_auto(
    url: str,
    pattern: str,
    timeout: int,
    session: requests.Session,
    page: Optional[Page] = None,
    js_retry: int = 3,
    js_retry_interval: float = 2.0,
    selector: Optional[str] = None
) -> List[str]:
    """
    智能自动抓取IP，优先静态，失败自动切换JS动态。
    :param url: 目标URL
    :param pattern: IP正则
    :param timeout: 超时时间
    :param session: requests.Session
    :param page: Playwright页面对象
    :param js_retry: JS动态重试次数
    :param js_retry_interval: JS重试间隔
    :param selector: CSS选择器
    :return: IP列表
    """
    logging.info(f"[AUTO] 正在抓取: {url}")
    extracted_ips: List[str] = []
    try:
        headers = {"User-Agent": USER_AGENT}
        response = session.get(url, headers=headers)
        response.raise_for_status()
        text = response.text
        extracted_ips = extract_ips_from_html(text, pattern, selector)
        logging.info(f"[DEBUG] {url} 静态抓取前10个IP: {extracted_ips[:10]}")
        if extracted_ips:
            logging.info(f"[AUTO] 静态抓取成功: {url}，共{len(extracted_ips)}个IP")
            return extracted_ips
        else:
            logging.info(f"[AUTO] 静态抓取无IP，尝试JS动态: {url}")
    except requests.RequestException as e:
        logging.warning(f"[AUTO] 静态抓取失败: {url}，网络错误: {e}，尝试JS动态")
    except Exception as e:
        logging.warning(f"[AUTO] 静态抓取失败: {url}，解析错误: {e}，尝试JS动态")
    if page is not None:
        try:
            page.set_extra_http_headers({"User-Agent": USER_AGENT})
        except Exception:
            pass
        found_ip_list = []
        def handle_response(response):
            try:
                text = response.text()
                ip_list = extract_ips(text, pattern)
                if len(ip_list) >= MIN_IP_BLOCK:
                    found_ip_list.extend(ip_list)
            except Exception:
                pass
        page.on("response", handle_response)
        for attempt in range(1, js_retry + 1):
            try:
                page.goto(url, timeout=DEFAULT_JS_TIMEOUT)
                page.wait_for_timeout(DEFAULT_WAIT_TIMEOUT)
                if found_ip_list:
                    found_ip_list = list(dict.fromkeys(found_ip_list))
                    logging.info(f"[AUTO] 监听接口自动提取到 {len(found_ip_list)} 个IP: {found_ip_list[:10]}")
                    return found_ip_list
                page_content = page.content()
                if '<html' in page_content.lower():
                    soup = BeautifulSoup(page_content, 'html.parser')
                    ip_list: List[str] = []
                    table = soup.find('table')
                    if table:
                        for row in table.find_all('tr'):
                            for cell in row.find_all('td'):
                                ip_list.extend(extract_ips(cell.get_text(), pattern))
                    else:
                        elements = soup.find_all('tr') if soup.find_all('tr') else soup.find_all('li')
                        for element in elements:
                            ip_list.extend(extract_ips(element.get_text(), pattern))
                    extracted_ips = list(dict.fromkeys(ip_list))
                    logging.info(f"[DEBUG] {url} JS动态抓取前10个IP: {extracted_ips[:10]}")
                else:
                    ip_list = extract_ips(page_content, pattern)
                    extracted_ips = list(dict.fromkeys(ip_list))
                    logging.info(f"[DEBUG] {url} JS动态纯文本前10个IP: {extracted_ips[:10]}")
                if extracted_ips:
                    logging.info(f"[AUTO] JS动态抓取成功: {url}，共{len(extracted_ips)}个IP")
                    return extracted_ips
                else:
                    logging.warning(f"[AUTO] JS动态抓取无IP: {url}，第{attempt}次")
            except Exception as e:
                logging.error(f"[AUTO] JS动态抓取失败: {url}，第{attempt}次，错误: {e}")
            if attempt < js_retry:
                time.sleep(js_retry_interval)
        logging.error(f"[AUTO] JS动态抓取多次失败: {url}")
    else:
        logging.error(f"[AUTO] 未提供page对象，无法进行JS动态抓取: {url}")
    return []

async def fetch_ip_static_async(url: str, pattern: str, timeout: int, session: aiohttp.ClientSession, selector: Optional[str] = None) -> tuple[str, List[str], bool]:
    """
    异步静态页面抓取任务，返回(url, IP列表 (有序且唯一), 是否成功)。
    :param url: 目标URL
    :param pattern: IP正则
    :param timeout: 超时时间
    :param session: aiohttp.ClientSession
    :param selector: 可选，CSS选择器
    :return: (url, IP列表 (有序且唯一), 是否成功)
    """
    try:
        user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
        headers = {"User-Agent": user_agent}
        async with session.get(url, timeout=timeout, headers=headers) as response:
            if response.status != 200:
                logging.warning(f"[ASYNC] 静态抓取失败: {url}，HTTP状态码: {response.status}")
                return (url, [], False)
            text = await response.text()
            ordered_unique_ips: List[str] = extract_ips_from_html(text, pattern, selector)
            logging.info(f"[DEBUG] {url} 静态抓取前10个IP: {ordered_unique_ips[:10]}")
            if ordered_unique_ips:
                logging.info(f"[ASYNC] 静态抓取成功: {url}，共{len(ordered_unique_ips)}个IP")
                return (url, ordered_unique_ips, True)
            else:
                logging.info(f"[ASYNC] 静态抓取无IP，加入JS动态队列: {url}")
                return (url, [], False)
    except asyncio.TimeoutError:
        logging.warning(f"[ASYNC] 静态抓取超时: {url}，加入JS动态队列")
        return (url, [], False)
    except Exception as e:
        logging.warning(f"[ASYNC] 静态抓取失败: {url}，错误: {e}，加入JS动态队列")
        return (url, [], False)

# ---------------- 新增：IP数量限制 ----------------
def limit_ips(ip_collection: Union[List[str], Set[str]], max_count: int, mode: str = 'random') -> List[str]:
    """
    限制IP集合/列表的数量，根据指定模式返回有限的IP列表（有序）。
    :param ip_collection: 原始IP列表 (用于top模式，需保持顺序) 或集合 (用于random模式)
    :param max_count: 最大保留数量，0表示不限制
    :param mode: 限制模式，'random'为随机保留，'top'为保留页面靠前的
    :return: 限制后的IP列表（有序）
    """
    collection_list = list(ip_collection)
    collection_len = len(collection_list)
    if max_count <= 0 or collection_len <= max_count:
        return collection_list
    if mode == 'top':
        return collection_list[:max_count]
    elif mode == 'random':
        import random
        return random.sample(collection_list, max_count)
    else:
        logging.warning(f"[LIMIT] 未知的限制模式: {mode}，使用默认的随机模式")
        import random
        return random.sample(collection_list, max_count)

async def async_static_crawl(sources: List[Dict[str, str]], pattern: str, timeout: int, max_ips: int = 0, limit_mode: str = 'random') -> tuple[Dict[str, List[str]], List[str]]:
    """
    并发抓取所有静态页面，返回每个URL的IP列表和需要JS动态抓取的URL。
    :param sources: [{url, selector}]列表
    :param pattern: IP正则
    :param timeout: 超时时间
    :param max_ips: 每个URL最多保留的IP数量，0表示不限制
    :param limit_mode: 限制模式，'random'为随机保留，'top'为保留页面靠前的
    :return: (每个URL的IP列表字典, 需要JS动态抓取的URL列表)
    """
    url_ips_dict: Dict[str, List[str]] = {}
    need_js_urls: List[str] = []
    connector = aiohttp.TCPConnector(ssl=False)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [fetch_ip_static_async(item['url'], pattern, timeout, session, item.get('selector')) for item in sources]
        results = await asyncio.gather(*tasks)
        for url, fetched_ip_list, success in results:
            if success:
                processed_ips_list: List[str]
                if max_ips > 0 and len(fetched_ip_list) > max_ips:
                    original_count = len(fetched_ip_list)
                    processed_ips_list = limit_ips(fetched_ip_list, max_ips, limit_mode)
                    logging.info(f"[LIMIT] URL {url} IP数量从 {original_count} 限制为 {len(processed_ips_list)}")
                else:
                    processed_ips_list = fetched_ip_list
                url_ips_dict[url] = processed_ips_list
            else:
                need_js_urls.append(url)
    return url_ips_dict, need_js_urls

# ---------------- 新增：IP排除功能 ----------------
def build_ip_exclude_checker(exclude_patterns: List[str]) -> Callable[[str], bool]:
    """
    构建IP排除检查器，支持精确匹配和CIDR格式网段匹配。
    :param exclude_patterns: 排除IP/网段列表
    :return: 检查函数，接收IP字符串，返回是否应该排除
    """
    if not exclude_patterns:
        # 没有排除规则，返回始终为False的函数
        return lambda ip: False
    
    # 预处理排除列表，分为精确匹配和网段匹配
    exact_ips = set()
    networks = []
    
    for pattern in exclude_patterns:
        pattern = pattern.strip()
        if '/' in pattern:
            # CIDR格式网段
            try:
                networks.append(ipaddress.ip_network(pattern, strict=False))
            except ValueError as e:
                logging.warning(f"无效的CIDR格式网段: {pattern}, 错误: {e}")
        else:
            # 精确匹配的IP
            exact_ips.add(pattern)
    
    def is_excluded(ip: str) -> bool:
        """
        检查IP是否应被排除。
        :param ip: IP地址字符串
        :return: 如果应该排除则为True，否则为False
        """
        # 先检查精确匹配
        if ip in exact_ips:
            return True
        
        # 再检查网段匹配
        if networks:
            try:
                ip_obj = ipaddress.ip_address(ip)
                return any(ip_obj in network for network in networks)
            except ValueError:
                logging.warning(f"无效的IP地址: {ip}")
        
        return False
    
    return is_excluded

# 速率限制装饰器（每秒最多N次）
def rate_limited(max_per_second: int):
    """
    速率限制装饰器（每秒最多N次）。
    :param max_per_second: 每秒最大调用次数
    :return: 装饰器
    """
    min_interval = 1.0 / float(max_per_second)
    lock = threading.Lock()
    last_time = [0.0]
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            with lock:
                elapsed = time.time() - last_time[0]
                wait = min_interval - elapsed
                if wait > 0:
                    time.sleep(wait)
                result = func(*args, **kwargs)
                last_time[0] = time.time()
                return result
        return wrapper
    return decorator

# ---------------- 新增：地区过滤相关函数 ----------------
@rate_limited(5)  # 默认每秒最多5次
def get_ip_region(ip: str, api_template: str, timeout: int = 5, max_retries: int = 3, retry_interval: float = 1.0) -> str:
    """
    查询IP归属地，返回国家/地区代码（如CN、US等），增加重试和降级机制。
    :param ip: IP地址
    :param api_template: API模板，{ip}会被替换
    :param timeout: 超时时间
    :param max_retries: 最大重试次数
    :param retry_interval: 重试间隔（秒）
    :return: 国家/地区代码（大写），失败返回空字符串
    """
    if not api_template:
        return ''
    url = api_template.replace('{ip}', ip)
    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.get(url, timeout=timeout)
            resp.raise_for_status()
            data = resp.json()
            # 兼容常见API返回格式
            for key in ['countryCode', 'country_code', 'country', 'countrycode']:
                if key in data:
                    val = data[key]
                    if isinstance(val, str) and len(val) <= 3:
                        return val.upper()
            # ipinfo.io等
            if 'country' in data and isinstance(data['country'], str):
                return data['country'].upper()
        except Exception as e:
            logging.warning(f"[REGION] 查询IP归属地失败: {ip}, 第{attempt}次, 错误: {e}")
            if attempt < max_retries:
                time.sleep(retry_interval)
    # 多次失败降级，返回空字符串
    return ''

def filter_ips_by_region(ip_list: List[str], allowed_regions: List[str], api_template: str, timeout: int = 5) -> List[str]:
    """
    只保留指定地区的IP，保持顺序。
    :param ip_list: 原始IP列表
    :param allowed_regions: 允许的地区代码列表
    :param api_template: 归属地API模板
    :param timeout: 查询超时时间
    :return: 过滤后的IP列表
    """
    if not allowed_regions or not api_template:
        return ip_list
    allowed_set = set([r.upper() for r in allowed_regions if isinstance(r, str)])
    filtered = []
    for ip in ip_list:
        region = get_ip_region(ip, api_template, timeout, max_retries=3, retry_interval=1.0)
        if region in allowed_set:
            filtered.append(ip)
        else:
            logging.info(f"[REGION] 过滤掉IP: {ip}，归属地: {region if region else '未知'}")
    return filtered


# ---------------- 新增：Telegram 通知功能 ----------------
def send_telegram_notification(message: str, bot_token: str, chat_id: str) -> bool:
    """
    发送Telegram通知消息。
    :param message: 要发送的消息内容
    :param bot_token: Telegram Bot Token
    :param chat_id: Telegram Chat ID
    :return: True表示发送成功，False表示失败
    """
    if not bot_token or not chat_id:
        logging.warning("Telegram BOT_TOKEN 或 CHAT_ID 未设置，跳过通知。")
        return False
    
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": message,
        "parse_mode": "Markdown" # 支持Markdown格式，如粗体、斜体
    }
    try:
        response = requests.post(url, json=payload, timeout=10) # 设置请求超时
        response.raise_for_status() # 如果状态码不是200，则抛出HTTPError
        logging.info("Telegram通知发送成功。")
        return True
    except requests.exceptions.RequestException as e:
        logging.error(f"发送Telegram通知失败: {e}")
        return False


def playwright_dynamic_fetch_worker(args: tuple) -> tuple:
    """
    单个线程任务：独立创建浏览器实例，抓取一个URL的动态IP。
    :param args: (url, pattern, timeout, js_retry, js_retry_interval, selector)
    :return: (url, result_ips)
    """
    url, pattern, timeout, js_retry, js_retry_interval, selector = args
    from playwright.sync_api import sync_playwright
    session = get_retry_session(timeout)
    result_ips = []
    try:
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=True)
            page = browser.new_page()
            try:
                page.goto(url, timeout=DEFAULT_JS_TIMEOUT)
                ip_list = []
                selector_success = False
                if selector:
                    try:
                        page.wait_for_selector(selector, timeout=20000)
                        elems = page.query_selector_all(selector)
                        for elem in elems:
                            ip_list.extend(extract_ips(elem.inner_text(), pattern))
                        logging.info(f"[EXTRACT] 使用selector '{selector}' 提取到{len(ip_list)}个IP")
                        selector_success = len(ip_list) > 0
                    except Exception:
                        logging.warning(f"[PLAYWRIGHT] 未检测到selector {selector}，自动降级为全局遍历")
                if not selector or not selector_success:
                    # 遍历table、div等常见结构，补充全局遍历
                    for row in page.query_selector_all('table tr'):
                        for cell in row.query_selector_all('td'):
                            ip_list.extend(extract_ips(cell.inner_text(), pattern))
                    logging.info(f"[EXTRACT] table遍历提取到{len(ip_list)}个IP")
                    for elem in page.query_selector_all('div'):
                        ip_list.extend(extract_ips(elem.inner_text(), pattern))
                    logging.info(f"[EXTRACT] div遍历提取到{len(ip_list)}个IP")
                    all_text = page.content()
                    ip_list.extend(extract_ips(all_text, pattern))
                    logging.info(f"[EXTRACT] 全局遍历提取到{len(ip_list)}个IP")
                result_ips = ip_list
                logging.info(f"[DEBUG] {url} 动态抓取前10个IP: {result_ips[:10]}")
            finally:
                page.close()
                browser.close()
    except Exception as e:
        logging.error(f"[THREAD] Playwright动态抓取失败: {url}, 错误: {e}")
    return url, result_ips

# ===== 新增：根据页面特性自动检测类型 =====
def detect_page_type(html_content: str, url: str) -> str:
    """
    自动分析页面内容，检测其最可能的类型。
    :param html_content: 网页内容
    :param url: 页面URL，用于辅助判断
    :return: 页面类型 (static/dynamic/api/table)
    """
    try:
        # 检测是否为API返回的JSON
        if html_content.strip().startswith('{') or html_content.strip().startswith('['):
            try:
                json.loads(html_content)
                logging.info(f"[AUTO-DETECT] {url} 检测为API类型")
                return 'api'
            except:
                pass

        # 判断是否为静态HTML
        if '<table' in html_content.lower() and '<tr' in html_content.lower() and '<td' in html_content.lower():
            logging.info(f"[AUTO-DETECT] {url} 检测为表格(table)类型")
            return 'table'
        
        # 判断是否可能需要JS渲染 (检查常见JS框架特征)
        js_framework_patterns = [
            'vue', 'react', 'angular', 'axios.get', 'fetch(', 'ajax', 
            'document.getElementById', 'addEventListener', 
            '<div id="app"', 'v-for', 'ng-app'
        ]
        
        js_content_triggers = [
            'setTimeout(', 'setInterval(', '.innerText', '.innerHTML', 
            'createElement', 'appendChild', 'updateTable', 'loadData'
        ]
        
        lazy_load_patterns = [
            'lazy-load', 'data-src=', 'loading="lazy"', 'onload=', 
            'DOMContentLoaded', 'window.onload'
        ]
        
        # 判断页面特征
        dynamic_score = 0
        
        # 检查JS框架特征
        for pattern in js_framework_patterns:
            if pattern in html_content.lower():
                dynamic_score += 2
        
        # 检查动态内容特征
        for trigger in js_content_triggers:
            if trigger in html_content:
                dynamic_score += 1
                
        # 检查懒加载特征
        for pattern in lazy_load_patterns:
            if pattern in html_content.lower():
                dynamic_score += 1
                
        # 如果页面中包含空表格和加载提示，很可能是动态加载
        if ('<table' in html_content.lower() and 
            ('<tbody></tbody>' in html_content.lower() or '<tr></tr>' in html_content.lower())):
            dynamic_score += 3
            
        # 检查空壳容器
        empty_containers = [
            '<div id="app"></div>', '<div id="root"></div>', 
            '<div class="container"></div>', '<div class="content"></div>'
        ]
        for container in empty_containers:
            if container in html_content.lower():
                dynamic_score += 3
                
        # 判断逻辑
        if dynamic_score >= 3:
            logging.info(f"[AUTO-DETECT] {url} 检测为动态(dynamic)类型 (分数: {dynamic_score})")
            return 'dynamic'
        else:
            logging.info(f"[AUTO-DETECT] {url} 检测为静态(static)类型 (分数: {dynamic_score})")
            return 'static'
            
    except Exception as e:
        logging.warning(f"[AUTO-DETECT] 自动检测页面类型出错: {url}, 错误: {e}, 使用默认static类型")
        return 'static'

# ===== 新增：从API响应中提取IP =====
def extract_ips_from_api(response_text: str, pattern: str, json_path: Optional[str] = None) -> List[str]:
    """
    从API响应中提取IP地址。
    :param response_text: API响应文本
    :param pattern: IP正则表达式
    :param json_path: JSON路径，用于提取IP列表，如"data.ips"
    :return: IP列表（顺序与API返回一致）
    """
    try:
        # 尝试解析为JSON
        json_data = json.loads(response_text)
        
        # 如果指定了JSON路径
        if json_path:
            # 按路径逐层获取
            parts = json_path.split('.')
            data = json_data
            for part in parts:
                if isinstance(data, dict) and part in data:
                    data = data[part]
                else:
                    logging.warning(f"[API] JSON路径 {json_path} 中的 {part} 未找到")
                    return []
            
            # 提取IP列表
            if isinstance(data, list):
                # 如果直接是IP列表
                ip_list = []
                for item in data:
                    if isinstance(item, str) and re.match(pattern, item):
                        ip_list.append(item)
                    elif isinstance(item, dict) and 'ip' in item:
                        ip = item['ip']
                        if isinstance(ip, str) and re.match(pattern, ip):
                            ip_list.append(ip)
                if ip_list:
                    logging.info(f"[API] 从JSON路径 {json_path} 提取到 {len(ip_list)} 个IP")
                    return ip_list
            
            # 如果是字符串，可能是逗号分隔的IP列表
            elif isinstance(data, str):
                ip_list = extract_ips(data, pattern)
                if ip_list:
                    logging.info(f"[API] 从JSON路径 {json_path} 提取到 {len(ip_list)} 个IP")
                    return ip_list
        
        # 如果没有指定JSON路径或者指定路径提取失败，尝试智能提取
        # 1. 尝试在整个JSON字符串中直接提取IP
        ip_list = extract_ips(response_text, pattern)
        if ip_list:
            logging.info(f"[API] 从整个JSON响应中提取到 {len(ip_list)} 个IP")
            return ip_list
        
        # 2. 尝试搜索常见的IP字段名
        common_ip_fields = ['ip', 'address', 'ipAddress', 'hostIP', 'serverIP', 'endpoint']
        for field in common_ip_fields:
            ips = []
            # 递归搜索json数据中的IP字段
            def search_ip_field(data, field_name):
                nonlocal ips
                if isinstance(data, dict):
                    for key, value in data.items():
                        if key == field_name and isinstance(value, str) and re.match(pattern, value):
                            ips.append(value)
                        elif isinstance(value, (dict, list)):
                            search_ip_field(value, field_name)
                elif isinstance(data, list):
                    for item in data:
                        search_ip_field(item, field_name)
            
            search_ip_field(json_data, field)
            if ips:
                logging.info(f"[API] 搜索字段 '{field}' 提取到 {len(ips)} 个IP")
                return ips
        
        logging.warning("[API] 未能从API响应中提取到IP")
        return []
    except json.JSONDecodeError:
        # 如果不是有效JSON，尝试直接用正则提取IP
        logging.warning("[API] API响应解析JSON失败，尝试直接正则提取IP")
        ip_list = extract_ips(response_text, pattern)
        if ip_list:
            logging.info(f"[API] 从非JSON响应中提取到 {len(ip_list)} 个IP")
            return ip_list
        return []
    except Exception as e:
        logging.error(f"[API] 提取IP异常: {e}")
        return []

def extract_ips_from_table(html: str, pattern: str, selector: Optional[str] = None) -> List[str]:
    """
    专门从表格结构中提取IP，处理各种表格格式。
    :param html: 网页HTML
    :param pattern: IP正则
    :param selector: 可选，CSS选择器
    :return: IP列表（顺序与页面一致）
    """
    soup = BeautifulSoup(html, 'html.parser')
    ip_list = []
    
    # 1. 优先用selector
    if selector:
        elements = soup.select(selector)
        if elements:
            for elem in elements:
                tables = elem.find_all('table')
                if tables:
                    # 处理选择器内的表格
                    for table in tables:
                        process_table(table, ip_list, pattern)
                else:
                    # 如果选择器没有找到表格，但可能选择器本身就是表格或表格行
                    if elem.name == 'table':
                        process_table(elem, ip_list, pattern)
                    elif elem.name == 'tr':
                        process_table_row(elem, ip_list, pattern)
                    else:
                        # 直接从元素文本中提取
                        ips = extract_ips(elem.get_text(), pattern)
                        ip_list.extend(ips)
            
            if ip_list:
                logging.info(f"[TABLE] 使用selector '{selector}' 提取表格中的IP，共{len(ip_list)}个")
                return list(dict.fromkeys(ip_list))
    
    # 2. 自动提取所有表格
    tables = soup.find_all('table')
    table_results = []
    for table in tables:
        table_ips = []
        process_table(table, table_ips, pattern)
        if table_ips:
            table_results.append((len(table_ips), table_ips))
    
    # 按表格中IP数量排序
    if table_results:
        table_results.sort(reverse=True)
        ip_list = table_results[0][1]  # 取IP最多的表格
        logging.info(f"[TABLE] 自动提取到表格中的IP，共{len(ip_list)}个")
        return list(dict.fromkeys(ip_list))
    
    # 3. 找不到表格或表格中没有IP，尝试从列表中提取
    list_elements = soup.find_all('ul')
    for ul in list_elements:
        for li in ul.find_all('li'):
            ip_list.extend(extract_ips(li.get_text(), pattern))
    
    if ip_list:
        logging.info(f"[TABLE] 从列表中提取到IP，共{len(ip_list)}个")
        return list(dict.fromkeys(ip_list))
    
    # 4. 最后尝试全局提取
    ip_list = extract_ips(html, pattern)
    logging.info(f"[TABLE] 全局提取到IP，共{len(ip_list)}个")
    return list(dict.fromkeys(ip_list))

def process_table(table, ip_list, pattern):
    """
    处理单个表格，提取IP。
    :param table: BeautifulSoup表格元素
    :param ip_list: 保存IP的列表
    :param pattern: IP正则
    """
    # 处理表头行
    thead = table.find('thead')
    if thead:
        for row in thead.find_all('tr'):
            process_table_row(row, ip_list, pattern)
    
    # 处理表格主体
    tbody = table.find('tbody')
    if tbody:
        for row in tbody.find_all('tr'):
            process_table_row(row, ip_list, pattern)
    else:
        # 没有tbody直接处理所有行
        for row in table.find_all('tr'):
            process_table_row(row, ip_list, pattern)

def process_table_row(row, ip_list, pattern):
    """
    处理表格的一行，提取IP。
    :param row: BeautifulSoup表格行元素
    :param ip_list: 保存IP的列表
    :param pattern: IP正则
    """
    # 优先查找带有特定类名的单元格（如推荐、优选等关键词）
    priority_cells = row.select('td.recommended, td.preferred, td.best, td.optimal, td.fast')
    if priority_cells:
        for cell in priority_cells:
            ip_list.extend(extract_ips(cell.get_text(), pattern))
        if ip_list:  # 如果已找到IP则返回
            return
    
    # 处理所有单元格
    for cell in row.find_all(['td', 'th']):
        ip_list.extend(extract_ips(cell.get_text(), pattern))

# ===== 新增：增强的动态页面处理 =====
def perform_page_actions(page: Page, actions: List[Dict[str, Any]]) -> None:
    """
    执行页面交互操作序列。
    :param page: Playwright Page对象
    :param actions: 操作列表
    """
    if not actions:
        return
    
    logging.info(f"[ACTIONS] 开始执行 {len(actions)} 个页面交互操作")
    
    for i, action in enumerate(actions):
        try:
            action_type = action.get('type', '').lower()
            
            if action_type == 'click':
                selector = action.get('selector')
                if not selector:
                    logging.warning(f"[ACTIONS] 操作 {i+1}: 点击操作缺少selector，跳过")
                    continue
                
                logging.info(f"[ACTIONS] 操作 {i+1}: 点击元素 '{selector}'")
                # 等待元素可点击
                page.wait_for_selector(selector, state='visible', timeout=10000)
                page.click(selector)
                
            elif action_type == 'wait':
                time_ms = action.get('time', 1000)  # 默认等待1秒
                logging.info(f"[ACTIONS] 操作 {i+1}: 等待 {time_ms}ms")
                page.wait_for_timeout(time_ms)
                
            elif action_type == 'input':
                selector = action.get('selector')
                value = action.get('value', '')
                if not selector:
                    logging.warning(f"[ACTIONS] 操作 {i+1}: 输入操作缺少selector，跳过")
                    continue
                
                logging.info(f"[ACTIONS] 操作 {i+1}: 向 '{selector}' 输入文本")
                page.fill(selector, value)
                
            elif action_type == 'select':
                selector = action.get('selector')
                value = action.get('value')
                if not selector or value is None:
                    logging.warning(f"[ACTIONS] 操作 {i+1}: 选择操作缺少selector或value，跳过")
                    continue
                
                logging.info(f"[ACTIONS] 操作 {i+1}: 在 '{selector}' 中选择 '{value}'")
                page.select_option(selector, value)
                
            elif action_type == 'scroll':
                selector = action.get('selector')
                if selector:
                    logging.info(f"[ACTIONS] 操作 {i+1}: 滚动到元素 '{selector}'")
                    page.scroll_into_view_if_needed(selector)
                else:
                    x = action.get('x', 0)
                    y = action.get('y', 0)
                    logging.info(f"[ACTIONS] 操作 {i+1}: 滚动到位置 ({x}, {y})")
                    page.evaluate(f"window.scrollTo({x}, {y})")
                    
            elif action_type == 'wait_for_selector':
                selector = action.get('selector')
                timeout = action.get('timeout', 30000)
                if not selector:
                    logging.warning(f"[ACTIONS] 操作 {i+1}: 等待元素操作缺少selector，跳过")
                    continue
                
                logging.info(f"[ACTIONS] 操作 {i+1}: 等待元素 '{selector}' 出现")
                page.wait_for_selector(selector, timeout=timeout)
                
            elif action_type == 'wait_for_load':
                state = action.get('state', 'load')  # load, domcontentloaded, networkidle
                timeout = action.get('timeout', 30000)
                logging.info(f"[ACTIONS] 操作 {i+1}: 等待页面 {state} 状态")
                page.wait_for_load_state(state, timeout=timeout)
                
            elif action_type == 'evaluate':
                script = action.get('script', '')
                if not script:
                    logging.warning(f"[ACTIONS] 操作 {i+1}: 执行脚本操作缺少script，跳过")
                    continue
                
                logging.info(f"[ACTIONS] 操作 {i+1}: 执行脚本")
                page.evaluate(script)
                
            else:
                logging.warning(f"[ACTIONS] 操作 {i+1}: 未知操作类型 '{action_type}'，跳过")
                
            # 每个操作后短暂等待，避免页面响应不及时
            page.wait_for_timeout(500)
            
        except Exception as e:
            logging.error(f"[ACTIONS] 操作 {i+1} 执行失败: {e}")
    
    logging.info("[ACTIONS] 页面交互操作执行完成")

def fetch_ip_enhanced(
    source: Dict[str, Any],
    pattern: str,
    timeout: int,
    session: requests.Session,
    page: Optional[Page] = None,
    js_retry: int = 3,
    js_retry_interval: float = 2.0,
    auto_detect: bool = True
) -> List[str]:
    """
    增强版IP抓取，根据页面类型自动选择最优抓取策略。
    :param source: 数据源配置
    :param pattern: IP正则
    :param timeout: 超时时间
    :param session: requests.Session
    :param page: Playwright页面对象
    :param js_retry: JS动态重试次数
    :param js_retry_interval: JS重试间隔
    :param auto_detect: 是否自动检测页面类型
    :return: IP列表
    """
    url = source['url']
    selector = source.get('selector')
    page_type = source.get('page_type')
    wait_time = source.get('wait_time', DEFAULT_WAIT_TIMEOUT)
    actions = source.get('actions')
    extra_headers = source.get('extra_headers', {})
    response_format = source.get('response_format')
    json_path = source.get('json_path')
    
    logging.info(f"[ENHANCED] 开始抓取: {url}")
    extracted_ips: List[str] = []
    
    # 合并自定义请求头
    headers = {"User-Agent": USER_AGENT}
    if extra_headers:
        headers.update(extra_headers)
    
    # 第一步：尝试静态请求获取响应内容
    try:
        response = session.get(url, headers=headers)
        response.raise_for_status()
        content = response.text
        
        # 如果未指定页面类型且开启自动检测，进行检测
        detected_type = None
        if auto_detect and not page_type:
            detected_type = detect_page_type(content, url)
            page_type = detected_type
        
        logging.info(f"[ENHANCED] {url} 页面类型: {page_type or '未指定'}")
        
        # 根据页面类型使用不同提取策略
        if page_type == 'api':
            # API类型，直接解析JSON
            extracted_ips = extract_ips_from_api(content, pattern, json_path)
            if extracted_ips:
                logging.info(f"[ENHANCED] API响应解析成功: {url}，共{len(extracted_ips)}个IP")
                return extracted_ips
            else:
                logging.warning(f"[ENHANCED] API响应解析无IP: {url}，尝试动态抓取")
        
        elif page_type == 'table':
            # 表格类型，使用专用表格提取器
            extracted_ips = extract_ips_from_table(content, pattern, selector)
            if extracted_ips:
                logging.info(f"[ENHANCED] 表格解析成功: {url}，共{len(extracted_ips)}个IP")
                return extracted_ips
            else:
                logging.warning(f"[ENHANCED] 表格解析无IP: {url}，尝试动态抓取")
        
        elif page_type == 'static' or not page_type:
            # 静态页面，使用通用提取器
            extracted_ips = extract_ips_from_html(content, pattern, selector)
            if extracted_ips:
                logging.info(f"[ENHANCED] 静态解析成功: {url}，共{len(extracted_ips)}个IP")
                return extracted_ips
            else:
                logging.warning(f"[ENHANCED] 静态解析无IP: {url}，尝试动态抓取")
    
    except requests.RequestException as e:
        logging.warning(f"[ENHANCED] 静态请求失败: {url}，错误: {e}，尝试动态抓取")
    except Exception as e:
        logging.warning(f"[ENHANCED] 解析异常: {url}，错误: {e}，尝试动态抓取")
    
    # 如果静态抓取失败或指定了动态页面类型，尝试动态抓取
    if page and (page_type == 'dynamic' or not extracted_ips):
        try:
            page.set_extra_http_headers(headers)
            for attempt in range(1, js_retry + 1):
                try:
                    logging.info(f"[ENHANCED] 动态抓取: {url}，第{attempt}次尝试")
                    page.goto(url, timeout=DEFAULT_JS_TIMEOUT)
                    
                    # 执行页面交互操作
                    if actions:
                        perform_page_actions(page, actions)
                    else:
                        # 默认等待页面加载完成
                        page.wait_for_load_state('networkidle', timeout=wait_time)
                        page.wait_for_timeout(1000)  # 额外等待1秒
                    
                    # 获取页面内容并提取IP
                    page_content = page.content()
                    
                    # 根据页面类型使用不同提取策略
                    if page_type == 'table':
                        extracted_ips = extract_ips_from_table(page_content, pattern, selector)
                    else:
                        extracted_ips = extract_ips_from_html(page_content, pattern, selector)
                    
                    if extracted_ips:
                        logging.info(f"[ENHANCED] 动态抓取成功: {url}，共{len(extracted_ips)}个IP")
                        return extracted_ips
                    else:
                        logging.warning(f"[ENHANCED] 动态抓取无IP: {url}，第{attempt}次")
                        
                except Exception as e:
                    logging.error(f"[ENHANCED] 动态抓取异常: {url}，第{attempt}次，错误: {e}")
                
                if attempt < js_retry:
                    time.sleep(js_retry_interval)
            
            logging.error(f"[ENHANCED] 动态抓取多次失败: {url}")
        except Exception as e:
            logging.error(f"[ENHANCED] 动态抓取初始化失败: {url}，错误: {e}")
    elif page_type == 'dynamic' and not page:
        logging.error(f"[ENHANCED] 需要动态抓取但未提供page对象: {url}")
    
    return extracted_ips

# ---------------- 主流程 ----------------
def main() -> None:
    """
    主程序入口，只从 config.yaml 读取配置，缺失项报错。
    1. 读取配置并校验
    2. 异步并发静态抓取
    3. Playwright 动态抓取（带重试）
    4. 结果去重并保存
    """
    config = load_config()
    sources = config['sources']
    pattern = config['pattern']
    output = config['output']
    timeout = config['timeout']
    log_file = config['log']
    max_workers = config['max_workers']
    log_level = config['log_level']
    js_retry = config['js_retry']
    js_retry_interval = config['js_retry_interval']
    max_ips_per_url = config['max_ips_per_url']
    per_url_limit_mode = config['per_url_limit_mode']
    exclude_ips_config = config['exclude_ips']
    auto_detect = config.get('auto_detect', True)
    xpath_support = config.get('xpath_support', False)
    follow_redirects = config.get('follow_redirects', True)
    enable_telegram_notification = config.get('enable_telegram_notification', False)

    setup_logging(log_file, log_level)
    logging.info(f"开始执行Cloudflare IP抓取，自动检测: {auto_detect}, XPath支持: {xpath_support}")
    
    if os.path.exists(output):
        try:
            os.remove(output)
        except Exception as e:
            logging.error(f"无法删除旧的输出文件: {output}，错误: {e}")

    url_ips_map: Dict[str, List[str]] = {}
    static_sources = []
    dynamic_sources = []
    
    # 根据页面类型分类数据源
    for source in sources:
        url = source['url']
        page_type = source.get('page_type')
        # 如果明确指定为dynamic或配置了actions，直接归为动态
        if page_type == 'dynamic' or source.get('actions'):
            dynamic_sources.append(source)
            logging.info(f"URL {url} 已归类为动态抓取")
        else:
            # 其他类型先尝试静态抓取
            static_sources.append(source)
            logging.info(f"URL {url} 已归类为静态抓取（可能降级为动态）")
    
    # 创建全局session（支持重定向）
    session = get_retry_session(timeout)
    if follow_redirects:
        session.max_redirects = 5
    
    # 处理静态抓取
    for source in static_sources:
        try:
            extracted_ips = fetch_ip_enhanced(
                source=source,
                pattern=pattern,
                timeout=timeout,
                session=session,
                page=None,  # 静态模式不需要page
                auto_detect=auto_detect
            )
            
            if max_ips_per_url > 0 and len(extracted_ips) > max_ips_per_url:
                original_count = len(extracted_ips)
                processed_ips = limit_ips(extracted_ips, max_ips_per_url, per_url_limit_mode)
                logging.info(f"[LIMIT] URL {source['url']} IP数量从 {original_count} 限制为 {len(processed_ips)}")
                url_ips_map[source['url']] = processed_ips
            else:
                url_ips_map[source['url']] = extracted_ips
            
            if not extracted_ips:
                # 静态抓取失败，加入动态队列
                dynamic_sources.append(source)
                logging.info(f"URL {source['url']} 静态抓取失败，已加入动态队列")
                
        except Exception as e:
            logging.error(f"处理静态源异常: {source['url']}, 错误: {e}")
            # 出错也加入动态队列
            dynamic_sources.append(source)
    
    # 处理动态抓取
    if dynamic_sources:
        # 使用Playwright处理动态页面
        try:
            with sync_playwright() as playwright:
                browser = playwright.chromium.launch(headless=True)
                context = browser.new_context()
                page = context.new_page()
                
                for source in dynamic_sources:
                    try:
                        extracted_ips = fetch_ip_enhanced(
                            source=source,
                            pattern=pattern,
                            timeout=timeout,
                            session=session,
                            page=page,
                            js_retry=js_retry,
                            js_retry_interval=js_retry_interval,
                            auto_detect=auto_detect
                        )
                        
                        if max_ips_per_url > 0 and len(extracted_ips) > max_ips_per_url:
                            original_count = len(extracted_ips)
                            processed_ips = limit_ips(extracted_ips, max_ips_per_url, per_url_limit_mode)
                            logging.info(f"[LIMIT] URL {source['url']} IP数量从 {original_count} 限制为 {len(processed_ips)}")
                            url_ips_map[source['url']] = processed_ips
                        else:
                            url_ips_map[source['url']] = extracted_ips
                            
                    except Exception as e:
                        logging.error(f"处理动态源异常: {source['url']}, 错误: {e}")
                
                page.close()
                context.close()
                browser.close()
        except Exception as e:
            logging.error(f"Playwright初始化失败: {e}")

    # 排除IP和地区过滤
    is_excluded_func = build_ip_exclude_checker(exclude_ips_config)
    excluded_count = 0

    merged_ips = []
    for url, ips_list_for_url in url_ips_map.items():
        original_count_before_exclude = len(ips_list_for_url)
        retained_ips = [ip for ip in ips_list_for_url if not is_excluded_func(ip)]
        excluded_in_source = original_count_before_exclude - len(retained_ips)
        if excluded_in_source > 0:
            logging.info(f"[EXCLUDE] URL {url} 排除了 {excluded_in_source} 个IP，保留 {len(retained_ips)} 个IP")
        excluded_count += excluded_in_source
        logging.info(f"URL {url} 贡献了 {len(retained_ips)} 个IP")
        # 新增：日志输出每个URL最终筛选出来的IP（前20个）
        if len(retained_ips) > 20:
            logging.info(f"[RESULT] URL {url} 最终筛选IP（前20个）: {retained_ips[:20]} ... 共{len(retained_ips)}个")
        else:
            logging.info(f"[RESULT] URL {url} 最终筛选IP: {retained_ips}")
        merged_ips.extend(retained_ips)

    final_all_ips = list(dict.fromkeys(merged_ips))

    allowed_regions = config.get('allowed_regions', [])
    ip_geo_api = config.get('ip_geo_api', '')
    if allowed_regions and ip_geo_api:
        before_region_count = len(final_all_ips)
        final_all_ips = filter_ips_by_region(final_all_ips, allowed_regions, ip_geo_api)
        after_region_count = len(final_all_ips)
        logging.info(f"[REGION] 地区过滤后，IP数量从 {before_region_count} 降至 {after_region_count}")

    save_ips(final_all_ips, output)
    logging.info(f"最终合并了 {len(url_ips_map)} 个URL的IP，排除了 {excluded_count} 个IP，共 {len(final_all_ips)} 个唯一IP")

    if enable_telegram_notification:
        telegram_bot_token = os.getenv('TELEGRAM_BOT_TOKEN')
        telegram_chat_id = os.getenv('TELEGRAM_CHAT_ID')

        if telegram_bot_token and telegram_chat_id:
            notification_message = (
                f"✅ Cloudflare 优选IP抓取完成！\n\n"
                f"📊 **IP数量**: {len(final_all_ips)} 个\n"
                f"🗑️ **排除IP**: {excluded_count} 个\n"
                f"💾 **保存至**: `{output}`\n"
            )
            send_telegram_notification(notification_message, telegram_bot_token, telegram_chat_id)
        else:
            logging.warning("Telegram通知已启用，但未找到 TELEGRAM_BOT_TOKEN 或 TELEGRAM_CHAT_ID 环境变量。请检查GitHub Secrets配置。")
    else:
        logging.info("Telegram通知未启用。")

# ===== 主流程入口 =====
if __name__ == '__main__':
    main()
