# -*- coding: utf-8 -*-
"""
Cloudflare IP 抓取主程序
--------------------------------
- 从 config.yaml 读取配置
- 并发抓取多个数据源
- 整合结果并保存
- 支持静态/动态页面抓取
- 支持IP去重、排序、地区过滤、排除等功能
"""

import os
import asyncio
import logging
import time
from typing import List, Dict, Any, Set, Callable

from ip_utils import (
    load_config, setup_logging, get_retry_session, extract_ips_from_html,
    fetch_ip_static_async, fetch_ip_dynamic, limit_ips, build_ip_exclude_checker,
    filter_ips_by_region, save_ips, send_telegram_notification
)
from playwright.sync_api import sync_playwright, Page

# ===== 主流程 =====
async def async_static_crawl(sources: List[Dict[str, Any]], pattern: str, timeout: int, max_ips: int = 0, limit_mode: str = 'random') -> tuple[Dict[str, List[str]], List[str]]:
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

def main() -> None:
    """
    主程序入口，只从 config.yaml 读取配置，缺失项报错。
    1. 读取配置并校验
    2. 异步并发静态抓取
    3. Playwright 动态抓取（带重试）
    4. 结果去重并保存
    """
    try:
        config = load_config()
    except Exception as e:
        logging.error(f"配置加载失败: {e}")
        return
    
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
    allowed_regions = config['allowed_regions']
    ip_geo_api = config['ip_geo_api']
    auto_detect = config.get('auto_detect', True)
    follow_redirects = config.get('follow_redirects', True)
    enable_telegram_notification = config.get('enable_telegram_notification', False)

    setup_logging(log_file, log_level)
    logging.info(f"开始执行Cloudflare IPv6抓取，自动检测: {auto_detect}")
    
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
        if page_type == 'dynamic' or source.get('actions'):
            dynamic_sources.append(source)
            logging.info(f"URL {url} 已归类为动态抓取")
        else:
            static_sources.append(source)
            logging.info(f"URL {url} 已归类为静态抓取（可能降级为动态）")
    
    # 创建全局session（支持重定向）
    session = get_retry_session(timeout)
    if follow_redirects:
        session.max_redirects = 5
    
    # 处理静态抓取
    loop = asyncio.get_event_loop()
    url_ips_dict, need_js_urls = loop.run_until_complete(
        async_static_crawl(static_sources, pattern, timeout, max_ips_per_url, per_url_limit_mode)
    )
    url_ips_map.update(url_ips_dict)
    
    # 将静态抓取失败的URL加入动态抓取队列
    for url in need_js_urls:
        dynamic_sources.append(next(item for item in static_sources if item['url'] == url))
    
    # 处理动态抓取
    if dynamic_sources:
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=True)
            context = browser.new_context()
            page = context.new_page()
            for source in dynamic_sources:
                url = source['url']
                selector = source.get('selector')
                extracted_ips = fetch_ip_dynamic(url, pattern, timeout, page, selector, js_retry, js_retry_interval)
                if max_ips_per_url > 0 and len(extracted_ips) > max_ips_per_url:
                    original_count = len(extracted_ips)
                    processed_ips = limit_ips(extracted_ips, max_ips_per_url, per_url_limit_mode)
                    logging.info(f"[LIMIT] URL {url} IP数量从 {original_count} 限制为 {len(processed_ips)}")
                    url_ips_map[url] = processed_ips
                else:
                    url_ips_map[url] = extracted_ips
            page.close()
            context.close()
            browser.close()
    
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
        merged_ips.extend(retained_ips)

    final_all_ips = list(dict.fromkeys(merged_ips))

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
            message = (
                f"✅ Cloudflare IPv6优选IP抓取完成！\n\n"
                f"📊 **IP数量**: {len(final_all_ips)} 个\n"
                f"🗑️ **排除IP**: {excluded_count} 个\n"
                f"💾 **保存至**: `{output}`\n"
            )
            send_telegram_notification(message, telegram_bot_token, telegram_chat_id)
        else:
            logging.warning("Telegram通知已启用，但未找到 TELEGRAM_BOT_TOKEN 或 TELEGRAM_CHAT_ID 环境变量。请检查GitHub Secrets配置。")
    else:
        logging.info("Telegram通知未启用。")

if __name__ == '__main__':
    main()
