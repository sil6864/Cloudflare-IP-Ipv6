# Cloudflare 优选IP自动抓取与更新

## 项目简介
本项目每8小时自动抓取 Cloudflare 优选IP，生成 `ip.txt`，并通过 GitHub Actions 自动化更新。

## 主要功能
- 定时抓取优选网站页面的 Cloudflare IP：
- 自动去重，保证IP唯一
- 自动推送最新IP列表到仓库
- 支持只保留指定地区（如中国大陆、香港等）的IP，地区和API可自定义

## 项目结构
- `fetch_cloudflare_ips.py`：主抓取脚本，负责抓取、解析、去重、保存IP
- `ip.txt`：最新抓取的IP列表
- `.github/workflows/update-cloudflare-ip-list.yml`：GitHub Actions自动化配置
- `README.md`：项目说明文档
- `requirements.txt`：Python依赖包列表
- `config.yaml`：业务参数配置文件

## 依赖安装
本地运行需先安装依赖：
```bash
pip install -r requirements.txt
python -m playwright install
```
> 注意：Playwright 浏览器需单独安装，务必执行 `python -m playwright install`
> 
> 本项目异步抓取部分依赖 aiohttp，请确保 requirements.txt 中包含 aiohttp。

## 用法说明
直接运行脚本：
```bash
python fetch_cloudflare_ips.py
```

支持通过 config.yaml 配置数据源、输出、日志等级等，命令行参数可覆盖配置文件。

## 配置文件说明（config.yaml）
> 仅包含业务参数，依赖请见 requirements.txt
示例：
```yaml
sources:
  - https://monitor.gacjie.cn/page/cloudflare/ipv4.html
  - https://ip.164746.xyz
  - https://cf.090227.xyz
  - https://stock.hostmonit.com/CloudFlareYes
pattern: '\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}'
output: "ip.txt"
timeout: 10
log: "fetch_cloudflare_ips.log"
max_workers: 4
log_level: "INFO"
js_retry: 3              # JS动态抓取最大重试次数
js_retry_interval: 2.0   # JS动态抓取重试间隔（秒）
max_ips_per_url: 10      # 每个URL最多保留多少个IP（0表示不限制）
per_url_limit_mode: "top"  # 超出限制时的筛选模式: random-随机保留, top-保留页面上实际靠前的IP
exclude_ips:           # 排除IP列表，支持单个IP和CIDR格式网段
  - "127.0.0.1"        # 排除单个IP，精确匹配
  - "192.168.1.0/24"   # 排除整个网段，CIDR格式
allowed_regions:        # 只保留这些地区的IP，使用国家/地区代码（如CN、US、JP等），留空或缺省则不过滤
  - "CN"
  - "HK"
ip_geo_api: "http://ip-api.com/json/{ip}"   # IP归属地API模板，{ip}会被替换为实际IP
```

优先级：命令行参数 > 配置文件 > 默认值

## 自动化流程（GitHub Actions）
- 每3小时自动运行脚本，抓取并更新IP列表
- 自动安装 Playwright 及浏览器，支持 JS 动态页面抓取
- 仅当`ip.txt`有变更时自动提交
- 支持手动触发和push触发

## 常见问题与解决办法
- 网络请求失败：请检查目标网站可用性或本地网络
- 依赖缺失：请确保已正确安装 requirements.txt 中所有依赖，并已执行 `python -m playwright install`
- 编码问题：脚本已指定utf-8编码，若仍有问题请反馈

## 其他说明
- 如需扩展更多 JS 动态页面抓取，可参考 fetch_js_ips 函数实现

如有建议或问题欢迎反馈！
