"""MACRO PULSE ティッカー（S&P500・前営業日比）の過去の表示を、各日の時点の05_events.csvの版で再計算する
（2026-10-03 指示書M-2 STEP 3、[[MACRO-PULSE-TICKER-FUTURE-ROW-1]]の確認用。読み取り専用）

各日 D について、D 03:00 UTC（日次の実行の後）より前の最後の版を git から読み、旧ロジック（release_date最大の行・
日付ごとの先頭行の最後の2日）と新ロジック（今日以前の行をupdated_at順、値が違う直前の行と比較）を、
正解（その時点で最後に書かれたsp500_t0と、FRED SP500でその前営業日の終値）と比べる。

使い方: python scripts/analysis/macro_ticker_history_check.py
"""
import pandas as pd, json, subprocess, io, os
from datetime import date, timedelta
os.environ['MSYS_NO_PATHCONV']='1'
P='docs/market-monitor/macro-pulse/data/05_events.csv'
d=json.load(open('common/macro_data/series/SP500.json')); d=d if isinstance(d,list) else d['records']
sp=sorted((x['as_of'],float(x['value'])) for x in d)
def commit_before(ts):
    return subprocess.run(['git','log','-1','--format=%H',f'--before={ts}','origin/kaihatsu','--',P],capture_output=True,text=True).stdout.strip()
res=[]; D=date(2026,5,1); cache={}
while D<=date(2026,10,3):
    c=commit_before(f"{D.isoformat()}T03:00:00Z")
    if c not in cache:
        txt=subprocess.run(['git','show',f'{c}:{P}'],capture_output=True,text=True,encoding='utf-8').stdout
        e=pd.read_csv(io.StringIO(txt),dtype=str).fillna('')
        cache={c:e}
    e=cache[c]; e=e[e.sp500_t0!='']; e=e.assign(v=pd.to_numeric(e.sp500_t0,errors='coerce')).dropna(subset=['v'])
    e=e[e.v!=0]
    o=e.sort_values('release_date',kind='stable')
    bymax=o[o.release_date==o.release_date.max()].iloc[0]; per=o.drop_duplicates('release_date',keep='first')
    old_cur=bymax.v; old_chg=per.v.iloc[-1]-per.v.iloc[-2]
    n=e[e.release_date<=D.isoformat()].sort_values(['updated_at','release_date'],kind='stable')
    new_cur=n.v.iloc[-1]; prev=[x for x in n.v.iloc[:-1][::-1] if x!=new_cur]; new_chg=new_cur-prev[0] if prev else None
    truth=e.sort_values('updated_at').v.iloc[-1]
    a=[dd for dd,v in sp if abs(v-truth)<1e-6]; a=max(a) if a else None
    tp=[v for dd,v in sp if a and dd<a]; tchg=truth-tp[-1] if tp else None
    res.append(dict(day=D,commit=c[:10],old_cur=abs(old_cur-truth)<1e-6,old_chg=tchg is not None and abs(old_chg-tchg)<0.005,
        new_cur=abs(new_cur-truth)<1e-6,new_chg=tchg is not None and new_chg is not None and abs(new_chg-tchg)<0.005, truth_date=a))
    D+=timedelta(1)
r=pd.DataFrame(res)
print('days',len(r),'distinct versions',r.commit.nunique())
for k in ['old_cur','old_chg','new_cur','new_chg']: print(k,'ok',int(r[k].sum()),'wrong',int((~r[k]).sum()))
print(r[~r.new_chg][['day','truth_date']].to_string())
