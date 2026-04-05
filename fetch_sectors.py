#!/usr/bin/env python3
"""
A股板块数据抓取脚本
每个交易日 15:35（北京时间）自动运行，抓取行业/概念/地域板块收盘数据
"""

import json
import time
import urllib.request
import urllib.parse
import os
from datetime import datetime, timezone, timedelta

TZ_BJ = timezone(timedelta(hours=8))

HEADERS = {
    'User-Agent': (
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
        'AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/120.0.0.0 Safari/537.36'
    ),
    'Referer': 'https://quote.eastmoney.com/',
    'Accept': 'application/json, text/plain, */*',
}


def fetch_sectors(fs, retries=3):
    """分页抓取所有板块数据（每页100条，最多10页）"""
    fields = 'f2,f3,f4,f8,f12,f14,f15,f16,f17,f18,f20,f21,f62'
    all_items = []

    for pn in range(1, 11):  # 最多10页，约1000条
        url = (
            'https://push2.eastmoney.com/api/qt/clist/get'
            f'?pn={pn}&pz=100&po=1&np=1&fltt=2&invt=2&fid=f3'
            f'&fs={urllib.parse.quote(fs)}&fields={fields}'
        )
        success = False
        for attempt in range(retries):
            try:
                req = urllib.request.Request(url, headers=HEADERS)
                with urllib.request.urlopen(req, timeout=20) as resp:
                    data = json.loads(resp.read().decode('utf-8'))
                items = (data or {}).get('data', {})
                if not items:
                    return {'data': {'diff': all_items}}
                items = items.get('diff') or []
                if not items:
                    return {'data': {'diff': all_items}}
                all_items.extend(items)
                success = True
                break
            except Exception as e:
                print(f'  [第{pn}页 重试 {attempt+1}/{retries}] {e}')
                if attempt < retries - 1:
                    time.sleep(2)

        if not success:
            print(f'  第{pn}页失败，停止翻页')
            break

        time.sleep(0.3)  # 避免请求过快

    return {'data': {'diff': all_items}}


def parse_data(raw):
    items = (raw or {}).get('data', {}).get('diff', []) or []
    result = []
    for item in items:
        name = item.get('f14', '')
        pct = item.get('f3')
        if not name or pct is None or pct == '-':
            continue
        try:
            pct_f = float(pct)
        except (TypeError, ValueError):
            continue
        result.append({
            'code':  item.get('f12', ''),
            'name':  name,
            'price': item.get('f2'),
            'pct':   round(pct_f, 2),
            'chg':   item.get('f4'),
            'vol':   item.get('f20'),
            'flow':  item.get('f62'),
        })
    return result


def main():
    now_bj = datetime.now(TZ_BJ)
    print(f'=== A股板块数据抓取 {now_bj.strftime("%Y-%m-%d %H:%M:%S")} (北京时间) ===')

    results = {}
    tabs = [
        ('industry', 'm:90+t:2+f:!50', '行业板块'),
        ('concept',  'm:90+t:3+f:!50', '概念板块'),
        ('geo',      'm:90+t:1+f:!50', '地域板块'),
    ]

    for key, fs, label in tabs:
        print(f'抓取 {label}...')
        raw = fetch_sectors(fs)
        data = parse_data(raw)
        results[key] = data
        print(f'  ✓ 获取 {len(data)} 个板块')
        time.sleep(1.5)

    total = sum(len(v) for v in results.values())
    if total == 0:
        print('⚠️  所有板块均无数据，可能为非交易时段，本次不写入')
        return

    output = {
        'updated':    now_bj.strftime('%Y-%m-%d %H:%M'),
        'updated_ts': int(now_bj.timestamp()),
        'industry':   results['industry'],
        'concept':    results['concept'],
        'geo':        results['geo'],
    }

    os.makedirs('data', exist_ok=True)
    out_path = 'data/sectors.json'
    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False, separators=(',', ':'))

    size_kb = os.path.getsize(out_path) / 1024
    print(f'\n✅ 保存到 {out_path}  ({size_kb:.1f} KB)')
    print(f'   行业: {len(results["industry"])}  概念: {len(results["concept"])}  地域: {len(results["geo"])}')


if __name__ == '__main__':
    main()
