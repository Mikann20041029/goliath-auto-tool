import os
import random
import re
from openai import OpenAI

# 認証設定
client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))

def generate_perfect_site():
    # トピックを固定してまずは確実な成功を目指す
    topic = "BMI健康管理と理想の体型シミュレーター"
    
    print(f"💎 サイト生成開始: {topic}")

    prompt = f"""
    Create a complete, professional single-file HTML website for '{topic}'.
    - Use Tailwind CSS for a high-end, modern, and clean UI.
    - Include a massive, 2000+ character expert article in Japanese about health for Google AdSense.
    - Features: A fully working JavaScript BMI calculator tool.
    - Multi-language buttons (JP, EN, FR, DE).
    - Format: Return ONLY raw HTML starting with <!DOCTYPE html>. 
    - NO explanation text, NO markdown code blocks (```html). Just pure HTML.
    """

    try:
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": prompt}]
        )
        
        content = response.choices[0].message.content.strip()

        # 【最重要】AIがマークダウン記号を混ぜた場合、それを強制削除する
        if content.startswith("```"):
            content = re.sub(r'^```[a-z]*\n?', '', content, flags=re.IGNORECASE)
            content = re.sub(r'\n?```$', '', content)

        # 念のため、先頭が <!DOCTYPE で始まっていない場合のゴミを除去
        if not content.startswith("<!DOCTYPE"):
            start_index = content.find("<!DOCTYPE")
            if start_index != -1:
                content = content[start_index:]

        with open("index.html", "w", encoding="utf-8") as f:
            f.write(content)
        
        print(f"✅ index.html の書き出しに成功しました。")
    except Exception as e:
        print(f"❌ エラー発生: {e}")
        exit(1)

if __name__ == "__main__":
    generate_perfect_site()
