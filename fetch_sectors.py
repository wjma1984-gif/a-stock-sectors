#!/usr/bin/env python3
"""
A股板块数据抓取脚本
每个交易日 15:35（北京时间）自动运行，抓取行业/概念/地域板块收盘数据
同时抓取申万一级行业（31个）涨跌幅，作为大板块分类的标准数据源
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

    for pn in range(1, 11):
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

        time.sleep(0.3)

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


def fetch_sw_l1():
    """
    用 AKShare 获取申万一级行业（31个）最新一个交易日涨跌幅
    返回列表: [{code, name, pct, date}, ...]
    """
    try:
        import akshare as ak
    except ImportError:
        print('  ⚠️  akshare 未安装，跳过申万数据')
        return []

    try:
        info = ak.sw_index_first_info()
        codes = [
            (row['行业代码'].replace('.SI', ''), row['行业名称'])
            for _, row in info.iterrows()
        ]
    except Exception as e:
        print(f'  ⚠️  获取申万行业列表失败: {e}')
        return []

    results = []
    for code, name in codes:
        for attempt in range(3):
            try:
                hist = ak.index_hist_sw(symbol=code, period='day')
                hist = hist.sort_values('日期').tail(2)
                if len(hist) == 2:
                    prev = float(hist.iloc[0]['收盘'])
                    cur  = float(hist.iloc[1]['收盘'])
                    pct  = round((cur - prev) / prev * 100, 2)
                    date = str(hist.iloc[1]['日期'])
                    open_p = float(hist.iloc[1]['开盘'])
                    high   = float(hist.iloc[1]['最高'])
                    low    = float(hist.iloc[1]['最低'])
                    vol    = float(hist.iloc[1]['成交量'])
                    results.append({
                        'code': code,
                        'name': name,
                        'pct':  pct,
                        'date': date,
                        'open': round(open_p, 2),
                        'high': round(high, 2),
                        'low':  round(low, 2),
                        'close': round(cur, 2),
                        'prev_close': round(prev, 2),
                        'vol': round(vol, 2),
                    })
                break
            except Exception as e:
                if attempt < 2:
                    time.sleep(2)
                else:
                    print(f'  ⚠️  {name}({code}) 获取失败: {e}')
        time.sleep(0.5)

    print(f'  ✓ 申万一级行业: {len(results)}/31 个')
    return results


def main():
    now_bj = datetime.now(TZ_BJ)
    print(f'=== A股板块数据抓取 {now_bj.strftime("%Y-%m-%d %H:%M:%S")} (北京时间) ===')

    # 抓取东方财富行业/概念/地域板块
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

    # 抓取申万一级行业
    print('抓取申万一级行业...')
    sw_l1 = fetch_sw_l1()

    total = sum(len(v) for v in results.values())
    if total == 0 and not sw_l1:
        print('⚠️  所有板块均无数据，可能为非交易时段，本次不写入')
        return

    output = {
        'updated':    now_bj.strftime('%Y-%m-%d %H:%M'),
        'updated_ts': int(now_bj.timestamp()),
        'sw_l1':      sw_l1,
        'industry':   results.get('industry', []),
        'concept':    results.get('concept', []),
        'geo':        results.get('geo', []),
    }

    os.makedirs('data', exist_ok=True)
    out_path = 'data/sectors.json'
    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False, separators=(',', ':'))

    size_kb = os.path.getsize(out_path) / 1024
    print(f'\n✅ 保存到 {out_path}  ({size_kb:.1f} KB)')
    print(f'   申万一级: {len(sw_l1)}  行业: {len(results.get("industry",[]))}  概念: {len(results.get("concept",[]))}  地域: {len(results.get("geo",[]))}')


if __name__ == '__main__':
    main()
