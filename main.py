import os
import json
import random
from openai import OpenAI
from atproto import Client as BskyClient
from mastodon import Mastodon

client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))

def generate_perfect_content():
    # トピックをより具体的に指定
    topics = ["BMI健康診断ツール", "複利資産運用シミュレーター", "毎日の消費カロリー計算機"]
    topic = random.choice(topics)
    
    print(f"💎 究極のサイトを生成中: {topic}")

    prompt = f"""
    Create a complete, single-file professional website for '{topic}'.
    Requirements:
    - Use Tailwind CSS for a high-end, modern UI.
    - Include a long, 2000+ character expert article in Japanese for Google AdSense SEO.
    - Interactive tool functionality with JavaScript (fully working).
    - Multi-language buttons (JP, EN, FR, DE).
    - ABSOLUTELY NO markdown backticks like ```html. 
    - Output ONLY the raw HTML code starting with <!DOCTYPE html>.
    """

    response = client.chat.completions.create(
        model="gpt-4o",
        messages=[{"role": "user", "content": prompt}]
    )
    
    # 応答から余計な装飾（```htmlなど）を徹底排除
    html_content = response.choices[0].message.content.strip()
    if html_content.startswith("```"):
        html_content = "\n".join(html_content.split("\n")[1:-1])

    with open("index.html", "w", encoding="utf-8") as f:
        f.write(html_content)
    
    return topic

if __name__ == "__main__":
    generate_perfect_content()
    print("✅ 完璧な index.html を書き出しました。")
