# -*- coding: utf-8 -*-
"""
Cloudflare IP 抓取工具函数
--------------------------------
- IP提取、验证、过滤
- 网络请求（带重试机制）
- 页面渲染（Playwright）
- 配置文件加载
- 日志配置
- 其他辅助功能
"""

import os
import re
import logging
import time
import json
import ipaddress
import threading
from functools import wraps
from typing import List, Set, Optional, Dict, Any, Union, Callable, Tuple

import requests
from requests.adapters import HTTPAdapter, Retry
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, Page
import asyncio
import aiohttp
import yaml

# ===== 常量定义 =====
USER_AGENT: str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
DEFAULT_JS_TIMEOUT: int = 30000
DEFAULT_WAIT_TIMEOUT: int = 5000
MIN_IP_BLOCK: int = 3

# ===== 日志配置 =====
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

# ===== 配置加载 =====
def load_config(config_path: str = 'config.yaml') -> Dict[str, Any]:
    """
    读取并校验 config.yaml 配置文件。
    :param config_path: 配置文件路径
    :return: 配置字典
    :raises RuntimeError, FileNotFoundError, ValueError, KeyError: 配置异常
    """
    if not os.path.exists(config_path):
        raise FileNotFoundError(f'未找到配置文件: {config_path}')
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
            if not isinstance(config, dict):
                raise ValueError('config.yaml 格式错误，需为字典结构')
            
            # 必需字段检查
            required = ['sources', 'pattern', 'output', 'timeout', 'log', 'max_workers', 'log_level', 'js_retry', 'js_retry_interval']
            for k in required:
                if k not in config:
                    raise KeyError(f'缺少必需字段: {k}')
            
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
            
            # 设置默认值
            config.setdefault('max_ips_per_url', 0)
            config.setdefault('per_url_limit_mode', 'random')
            config.setdefault('exclude_ips', [])
            config.setdefault('allowed_regions', [])
            config.setdefault('ip_geo_api', '')
            config.setdefault('auto_detect', True)
            config.setdefault('xpath_support', False)
            config.setdefault('follow_redirects', True)
            config.setdefault('enable_telegram_notification', False)
            
            return config
    except Exception as e:
        raise RuntimeError(f"读取配置文件失败: {e}")

# ===== IP工具函数 =====
def is_valid_ipv6(ip_str: str) -> bool:
    """检查是否为有效的IPv6地址"""
    try:
        # 尝试解析为IPv6地址
        ipaddress.IPv6Address(ip_str)
        return True
    except ipaddress.AddressValueError:
        # 尝试处理可能的CIDR格式
        if '/' in ip_str:
            try:
                # 尝试解析为IPv6网络
                ipaddress.IPv6Network(ip_str, strict=False)
                return True
            except:
                return False
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
    
    # 调试：记录找到的原始IP
    if raw_ips:
        logging.info(f"[EXTRACT] 找到 {len(raw_ips)} 个原始IP: {raw_ips[:5]}...")
    
    # 过滤无效IP地址
    valid_ips = []
    for ip in raw_ips:
        # 清理IP字符串（去除多余空格等）
        clean_ip = ip.strip()
        if is_valid_ipv6(clean_ip):
            valid_ips.append(clean_ip)
        else:
            logging.debug(f"[VALIDATION] 无效IP: {clean_ip}")
    
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

# ===== 网络请求 =====
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

# ===== 页面内容处理 =====
def extract_ips_from_html(html: str, pattern: str, selector: Optional[str] = None) -> List[str]:
    # 保存HTML用于调试
    save_html_for_debugging(html, "debug.html")
    """
    智能提取IP，优先用selector，其次自动检测IP密集块，最后全局遍历。
    :param html: 网页HTML
    :param pattern: IP正则
    :param selector: 可选，CSS选择器
    :return: IP列表 (按找到的顺序)
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

# ===== 异步抓取 =====
async def fetch_ip_static_async(url: str, pattern: str, timeout: int, session: aiohttp.ClientSession, selector: Optional[str] = None) -> Tuple[str, List[str], bool]:
    """
    异步静态页面抓取任务，返回(url, IP列表 (有序且唯一), 是否成功)。
    :param url: 目标URL
    :param pattern: IP正则表达式
    :param timeout: 超时时间
    :param session: aiohttp.ClientSession
    :param selector: 可选，CSS选择器
    :return: (url, IP列表 (有序且唯一), 是否成功)
    """
    try:
        headers = {"User-Agent": USER_AGENT}
        async with session.get(url, headers=headers, timeout=timeout) as response:
            if response.status != 200:
                logging.warning(f"[ASYNC] 静态抓取失败: {url}，HTTP状态码: {response.status}")
                return (url, [], False)
            text = await response.text()
            ip_list = extract_ips_from_html(text, pattern, selector)
            if ip_list:
                logging.info(f"[ASYNC] 静态抓取成功: {url}，共{len(ip_list)}个IP")
                return (url, ip_list, True)
            else:
                logging.info(f"[ASYNC] 静态抓取无IP: {url}")
                return (url, [], False)
    except asyncio.TimeoutError:
        logging.warning(f"[ASYNC] 静态抓取超时: {url}")
        return (url, [], False)
    except Exception as e:
        logging.warning(f"[ASYNC] 静态抓取失败: {url}，错误: {e}")
        return (url, [], False)

# ===== 动态页面抓取 =====
def fetch_ip_dynamic(url: str, pattern: str, timeout: int, page: Page, selector: Optional[str] = None, js_retry: int = 3, js_retry_interval: float = 2.0) -> List[str]:
    logging.info(f"[DYNAMIC] 开始动态抓取: {url}")
    extracted_ips = []
    
    try:
        # 设置更长的超时时间
        page.set_default_timeout(60000)
        
        for attempt in range(1, js_retry + 1):
            try:
                logging.info(f"[DYNAMIC] 尝试 #{attempt}: {url}")
                page.goto(url, timeout=60000)
                
                # 等待页面完全加载
                page.wait_for_load_state("networkidle", timeout=30000)
                page.wait_for_timeout(5000)  # 额外等待5秒
                
                # 获取页面内容
                content = page.content()
                
                # 尝试多种提取方法
                if selector:
                    try:
                        elements = page.query_selector_all(selector)
                        for elem in elements:
                            text = elem.inner_text()
                            ips = extract_ips(text, pattern)
                            if ips:
                                extracted_ips.extend(ips)
                                logging.info(f"[DYNAMIC] 使用选择器找到 {len(ips)} 个IP")
                    except:
                        logging.warning(f"[DYNAMIC] 选择器 '{selector}' 无效")
                
                # 如果选择器未找到IP，尝试表格提取
                if not extracted_ips:
                    tables = page.query_selector_all('table')
                    for table in tables:
                        rows = table.query_selector_all('tr')
                        for row in rows:
                            cells = row.query_selector_all('td')
                            for cell in cells:
                                text = cell.inner_text()
                                ips = extract_ips(text, pattern)
                                if ips:
                                    extracted_ips.extend(ips)
                                    logging.info(f"[DYNAMIC] 表格中找到 {len(ips)} 个IP")
                
                # 如果仍未找到，尝试全局提取
                if not extracted_ips:
                    text = page.inner_text('body')
                    ips = extract_ips(text, pattern)
                    if ips:
                        extracted_ips.extend(ips)
                        logging.info(f"[DYNAMIC] 全局文本中找到 {len(ips)} 个IP")
                
                if extracted_ips:
                    logging.info(f"[DYNAMIC] 动态抓取成功: {url}，共{len(extracted_ips)}个IP")
                    return list(set(extracted_ips))  # 去重
                
                logging.warning(f"[DYNAMIC] 动态抓取无IP: {url}，第{attempt}次")
                
            except Exception as e:
                logging.error(f"[DYNAMIC] 动态抓取异常: {url}，第{attempt}次，错误: {e}")
            
            if attempt < js_retry:
                time.sleep(js_retry_interval)
        
        logging.error(f"[DYNAMIC] 动态抓取多次失败: {url}")
    except Exception as e:
        logging.error(f"[DYNAMIC] 动态抓取初始化失败: {url}，错误: {e}")
    
    return extracted_ips

# ===== IP数量限制 =====
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

# ===== IP排除功能 =====
def build_ip_exclude_checker(exclude_patterns: List[str]) -> Callable[[str], bool]:
    """
    构建IP排除检查器，支持精确匹配和CIDR格式网段匹配。
    :param exclude_patterns: 排除IP/网段列表
    :return: 检查函数，接收IP字符串，返回是否应该排除
    """
    if not exclude_patterns:
        return lambda ip: False
    
    # 预处理排除列表，分为精确匹配和网段匹配
    exact_ips = set()
    networks = []
    
    for pattern in exclude_patterns:
        pattern = pattern.strip()
        if '/' in pattern:
            try:
                networks.append(ipaddress.ip_network(pattern, strict=False))
            except ValueError as e:
                logging.warning(f"无效的CIDR格式网段: {pattern}, 错误: {e}")
        else:
            exact_ips.add(pattern)
    
    def is_excluded(ip: str) -> bool:
        if ip in exact_ips:
            return True
        if networks:
            try:
                ip_obj = ipaddress.ip_address(ip)
                return any(ip_obj in network for network in networks)
            except ValueError:
                pass
        return False
    
    return is_excluded

# ===== 速率限制装饰器 =====
def rate_limited(max_per_second: int):
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

# ===== 地区过滤 =====
@rate_limited(5)
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
        region = get_ip_region(ip, api_template, timeout)
        if region in allowed_set:
            filtered.append(ip)
        else:
            logging.info(f"[REGION] 过滤掉IP: {ip}，归属地: {region if region else '未知'}")
    return filtered

def save_html_for_debugging(html: str, filename: str):
    """保存HTML内容用于调试"""
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(html)
        logging.debug(f"已保存HTML内容到 {filename}")
    except Exception as e:
        logging.error(f"保存HTML失败: {e}")

# ===== Telegram 通知 =====
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
        "parse_mode": "Markdown"
    }
    try:
        response = requests.post(url, json=payload, timeout=10)
        response.raise_for_status()
        logging.info("Telegram通知发送成功。")
        return True
    except Exception as e:
        logging.error(f"发送Telegram通知失败: {e}")
        return False
