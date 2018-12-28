# PDF表格提取

该方法用于提取年报数据表格

## 第三方库

```python
pip install pdfplumber
```

## PDFExtarct.py

### 主要函数/get_general_table

用来提取一般表格。
```
变量输入：
raw_df: 由plumber直接提取后，转成dataframe类型
table_key_words: 表格中的关键词 list
column_location: 关键词所在列 int
rule: 1 -> 多行合并; 2 -> 向上合并
k: 按照第k列规律进行合并
输出：
提取后的dataframe类型
```

### test

```python
file_path = r'C:/Users/luoming3/Desktop/Report_PDF/'
variable_input = [['经营情况'], ['分行业'], 0, 1, 0, 1, 'digital']
main(file_path, variable_input, "test.json")
```

## HangYe.py

用来提取年报中行业各公司的主营业务
```
输入：
file_path：pdf所在的文件夹路径
output_name：输出的文件名
输出：
json格式
```

### test

```python
# file_path: pdf文件所在的文件夹路径
file_path = ur'C:/Users/luoming3/Desktop/Report_PDF/'
main(file_path, 'Industry-data.json')
```
