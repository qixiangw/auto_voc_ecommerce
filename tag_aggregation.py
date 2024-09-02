import unittest
from collections import Counter
import boto3
import json
from boto3.dynamodb.conditions import Key

# 初始化 DynamoDB 客户端
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('sample_reviews')

# 用于存储所有 terms 的字典
all_terms = {
    'advantages': [],
    'disadvantages': [],
    'motivations': [],
    'expectations': []
}


def tag_aggregation(category, terms):
    bedrock = boto3.client(
        service_name='bedrock-runtime',
        region_name='us-west-2'
    )

    # Define the model ID for Claude 3.5 Sonnet
    model_id = "anthropic.claude-3-5-sonnet-20240620-v1:0"
    aspects = category
    system_prompt = """You are an experienced e-commerce reviews ananlyor.Your task is to perform aspect-based sentiment analysis (ABSA) on customer reviews"""

    prompt = f"""Given the following list of terms in <term> with aspects in {aspects}. Please cluster these terms into 10 representative term. Return the term names in JSON format like <output>. With out any Explanation and Format tag.
    - Terms with similar or the same meaning should be merged into one term, while preserving the original intent.
    - The each terms you summarized must be simplified between 2 and 3 words with its corresponding detailed description.
    <term>{terms}</term>
    <output>
        <terms>
            <item>
                <term>fresh food</term>
                <description>Ingredients or dishes made with fresh, high-quality</description>
            </item>
        </terms>
    </output>
    """
    messages = [{"role": "user", "content": [{"type": "text", "text": prompt}]}, {"role": "assistant", "content": [
        {"type": "text",
         "text": 'Based on the given review and rules, here is reslut in the requested JSON format:'}]}]

    request_body = json.dumps({
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": 1000,
        "system": system_prompt,
        "messages": messages
    }
    )

    response = bedrock.invoke_model(body=request_body, modelId=model_id)
    response_body = json.loads(response.get('body').read())
    response_text = response_body['content'][0]['text']

    return response_text

def main(table):

    # 扫描整个表
    response = table.scan()
    # 处理扫描结果
    while 'Items' in response:
        for item in response['Items']:
            if 'Tags' in item:
                try:
                    # 解析 Tags 字段的 JSON 内容
                    tags = json.loads(item['Tags'])

                    # 遍历所有类别
                    for category in all_terms.keys():
                        if category in tags:
                            # 提取所有 'term' 并添加到相应的列表中
                            terms = [entry['term'] for entry in tags[category] if 'term' in entry]
                            all_terms[category].extend(terms)
                except json.JSONDecodeError:
                    print(f"Error decoding JSON for item: {item['Tags']}")

        # 检查是否有更多数据需要扫描
        if 'LastEvaluatedKey' in response:
            response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        else:
            break

    for category, terms in all_terms.items():
        unique_terms = list(set(terms))  # 去重
        if len(unique_terms) <= 10:
            print(f"Top {category} terms:", unique_terms)
            return unique_terms
        else:
            # 使用Counter获取最常见的术语
            common_terms = Counter(terms).most_common(20)
            common_terms = [term for term, _ in common_terms]
            clustered_terms = tag_aggregation(category, common_terms)
            print(f"Top {category} terms (clustered):", clustered_terms[:10])
            return clustered_terms



    # final_tag = tag_generator(category, terms)



'''
category = 'advantages'
# 'fresh ingredients', 'good service',
terms = ['affordable price', 'good taste', 'good value', 'good environment', 'tasty food', 'taste good', 'good price', 'high value', 'variety rich', 'kids like', 'delicious food', 'fresh salmon', 'fresh food', 'nice environment', 'good quality', 'friendly service', 'variety choices', 'good location']
common_terms = Counter(terms).most_common(20)
print(common_terms)
final_tag = tag_generator(category, terms)
print(final_tag)
'''

