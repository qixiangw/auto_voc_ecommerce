import boto3
from botocore.exceptions import ClientError

import pandas as pd
from decimal import Decimal
from boto3.dynamodb.conditions import Attr
from botocore.exceptions import ClientError
import random
import json

def create_table_if_not_exists(table_name, key_schema, attribute_definitions):
    try:
        table = dynamodb.create_table(
            TableName=table_name,
            KeySchema=key_schema,
            AttributeDefinitions=attribute_definitions,
            ProvisionedThroughput={
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5
            }
        )
        table.meta.client.get_waiter('table_exists').wait(TableName=table_name)
        print(f"Table {table_name} created successfully.")
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceInUseException':
            print(f"Table {table_name} already exists.")
            table = dynamodb.Table(table_name)
        else:
            print(f"Unexpected error: {e}")
            raise
    return table

def analyze_sample_review(content,category):

    bedrock = boto3.client(
        service_name='bedrock-runtime',
        region_name='us-west-2'
    )

    # Define the model ID for Claude 3.5 Sonnet
    model_id = "anthropic.claude-3-5-sonnet-20240620-v1:0"

    system_prompt = """You are an experienced e-commerce reviews ananlyor.Your task is to perform aspect-based sentiment analysis (ABSA) on customer reviews"""

    prompt = f"""Your platform is focus on {category}， extract aspects(noun phrases)  from reviews in <reviews> with aspects in <aspects> tags.Following Aspect Term Extraction (ATE) rules in <rule>. 
    Answer in English lowercase letters in JSON format like <output>. With out any Explanation and Format tag.
    <reviews>
    {content}
    </reviews >
    
    <rule>
    - Aspects are noun phrases that refer to specific features of the product or service.
    - The each term you summarized must be simplified less than 5 words.
    - Extract all aspects mentioned in the reviews. Don’t include implicit aspects.
    - Copy aspect terms verbatim, without generalizing or modifying them. 
    - Opinions about the product or service as a whole are not aspects
    </rule>
    
    <aspects>
    <aspect>advantages</aspect>
    <aspect>disadvantages</aspect>
    <aspect>motivations</aspect>
    <aspect>expectations</aspect>
    </aspects>
    
    <output>
        <advantages>
            <item>
                <term>fabric</term>
                <sentiment>positive</sentiment>
            </item>
            <item>
                <term>structured collar</term>
                <sentiment>positive</sentiment>
            </item>
        </advantages>
        <disadvantages>
            <item>
                <term>arms</term>
                <sentiment>negative</sentiment>
            </item>
            <item>
                <term>sleeves</term>
                <sentiment>negative</sentiment>
            </item>
         </disadvantages>
        <motivations>
            <item>
                <term>wearing it as dress</term>
                <sentiment>neutral</sentiment>
            </item>
        </motivations>
        <expectations>
        </expectations>
    </output>
    """
    messages = [{"role": "user", "content": [{"type": "text", "text": prompt}]},{"role": "assistant", "content": [{"type": "text", "text": 'Based on the given review and rules, here is the extracted aspect-based sentiment analysis in the requested JSON format:'}]}]

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


def process_item(row):
    Asin = row.get('Asin')
    content = row.get('Content')

    if pd.isna(Asin) or pd.isna(content):
        return None

    return {
        'Asin': Asin,
        'Content': content
    }

# 现在你可以使用 tables['original_reviews'] 和 tables['sample_reviews'] 来访问这些表

def main(file_path,categroy,tables):
    df = pd.read_excel(file_path)
    df_valid = [i for i, row in df.iterrows() if not pd.isna(row['Asin']) and not pd.isna(row['Content'])]
    # 保存所有数据到原始数据表
    for index in df_valid:
        row = df.iloc[index]
        item = {
            'Asin': row['Asin'],
            'ReviewID': str(index),
            'Content': row['Asin']
        }
        original_table = tables['original_reviews']
        try:
            original_table.put_item(Item=item)
        except ClientError as e:
            print(f"Error saving item {index}: {e}")


    # 随机抽样10%的数据
    valid_indices = [i for i, row in df.iterrows() if not pd.isna(row['Asin']) and not pd.isna(row['Content'])]
    sample_size = int(len(valid_indices) * 0.1)
    sample_indices = random.sample(valid_indices, sample_size)

    # 对抽样数据进行分析并保存
    for index in sample_indices:
        row = df.iloc[index]
        content = row['Content']

        # 分析评论
        tags = analyze_sample_review(content,category=categroy)

        # 保存到抽样数据表
        item = {
            'Asin': row['Asin'],
            'ReviewID': str(index),
            'Content': content,
            'Tags': tags
        }
        sample_table = tables['sample_reviews']
        try:
            sample_table.put_item(Item=item)
        except ClientError as e:
            print(f"Error saving item {index}: {e}")

    print("All data processed and stored in DynamoDB")
'''
def test():
    df = pd.read_excel('/Users/wqx/Downloads/服装-2024-04-26 16_52_43.xlsx')
    print(len(df))
    valid_indices = [i for i, row in df.iterrows() if not pd.isna(row['Asin']) and not pd.isna(row['Content'])]
    print(len(valid_indices))

    # 连接到DynamoDB
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('sample_reviews')

    sample_size = int(len(valid_indices) * 0.02)
    sample_indices = random.sample(valid_indices, sample_size)
    for index in sample_indices:
        row = df.iloc[index]
        content = row['Content']

        # 分析评论
        tags = analyze_sample_review(content,category="clothes")

        item = {
            'Asin': row['Asin'],
            'ReviewID': str(index),
            'Content': content,
            'Tags': tags
        }

        #sample_table = tables['sample_reviews']
        table.put_item(Item=item)


        #print(tags)
        #print('------------------')

'''
if __name__ == '__main__':
    file_path = '/Users/wqx/Downloads/服装-2024-04-26 16_52_43.xlsx'
    category = "clothes"

    dynamodb = boto3.resource('dynamodb')

    # 定义表结构
    table_structures = {
        'original_reviews': {
            'KeySchema': [
                {'AttributeName': 'Asin', 'KeyType': 'HASH'},
                {'AttributeName': 'ReviewID', 'KeyType': 'RANGE'}
            ],
            'AttributeDefinitions': [
                {'AttributeName': 'Asin', 'AttributeType': 'S'},
                {'AttributeName': 'ReviewID', 'AttributeType': 'S'}
            ]
        },
        'sample_reviews': {
            'KeySchema': [
                {'AttributeName': 'Asin', 'KeyType': 'HASH'},
                {'AttributeName': 'ReviewID', 'KeyType': 'RANGE'}
            ],
            'AttributeDefinitions': [
                {'AttributeName': 'Asin', 'AttributeType': 'S'},
                {'AttributeName': 'ReviewID', 'AttributeType': 'S'}
            ]
        }
    }

    # 创建表（如果不存在）
    tables = {}
    for table_name, structure in table_structures.items():
        tables[table_name] = create_table_if_not_exists(
            table_name,
            structure['KeySchema'],
            structure['AttributeDefinitions']
        )

    main(file_path,category,tables)
    # test()
