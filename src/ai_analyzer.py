import os
from openai import OpenAI # xAIはOpenAI互換SDKを使用可能

def analyze_with_xai(ticker, data, adjs, press_release):
    client = OpenAI(
        api_key=os.environ.get("XAI_API_KEY"),
        base_url="https://api.x.ai/v1",
    )
    
    prompt = f"""
    【投資分析ミッション】
    銘柄: {ticker}
    調整項目: {adjs}
    プレスリリース内容: {press_release}
    
    上記に基づき、この調整が「実態を表す前向きなもの」か「利益を飾る不健全なもの」か厳しく評価せよ。
    """
    # 課金キーのため、特に重要なフラグが立った時のみ呼び出すロジックにします
