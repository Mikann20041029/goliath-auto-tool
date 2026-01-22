import os
import requests
import json
from openai import OpenAI

# 1. 初期設定（Secretsから読み込み）
client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))

def generate_tool_and_article():
    # 悩みのテーマ（例：ダイエット、プログラミング、家計簿などからランダムに選択）
    topics = ["fitness", "saving money", "productivity", "cooking", "mental health"]
    import random
    selected_topic = random.choice(topics)

    # AIにツールと記事を生成させる
    prompt = f"""
    Create a useful web tool and a blog post about {selected_topic}.
    The tool must be a single HTML file using JavaScript and Tailwind CSS.
    The blog post must be at least 2000 characters in Japanese, professional, and helpful for Google AdSense approval.
    Return the result in JSON format:
    {{
        "title": "tool title",
        "filename": "unique-filename.html",
        "article_content": "long article content here...",
        "tool_code": "full html code here..."
    }}
    """

    response = client.chat.completions.create(
        model="gpt-4o",
        messages=[{"role": "user", "content": prompt}],
        response_format={ "type": "json_object" }
    )
    
    data = json.loads(response.choices[0].message.content)
    
    # ファイルとして保存
    with open(data['filename'], "w", encoding="utf-8") as f:
        f.write(data['tool_code'])
    
    print(f"Generated: {data['title']}")
    return data

if __name__ == "__main__":
    generate_tool_and_article()
