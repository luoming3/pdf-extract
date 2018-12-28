# coding: utf-8

import re
import os
import time
import json
import codecs

import pdfplumber
import pandas as pd


def linebreak(x):
    try:
        if re.search(r'\n', x):
            return True
        else:
            return False
    except TypeError:
        return False


class PdfExtract(object):

    def __init__(self, pdf):
        self.pdf = pdf
        self.company_name = ''
        self.catalog = ''
        for page in self.pdf.pages[0:10]:
            con_text = page.extract_text()
            if con_text:
                if not self.company_name:
                    name_pattern = ur'[（）\u4e00-\u9fa5]{2,}公司'
                    company_name = re.search(name_pattern, con_text)
                    if company_name:
                        self.company_name = company_name.group()
                if not self.catalog:
                    catalog_pattern = ur'([、\u4e00-\u9fa5]+)[ \t\r\f\v]*?[\u2026\.]+[ \t\r\f\v]*?(\d+)'
                    # 目录
                    catalog = re.findall(catalog_pattern, con_text)
                    self.catalog = catalog
                if self.catalog and self.company_name:
                    break
        if not self.catalog:
            # 没有搜索到目录
            pass

    def get_page_number(self, catalog_key_words):
        """
        :param catalog_key_words: 搜索表所在目录关键词 list
        :return: 该表的页面范围
        """
        words_str = u'|'.join(catalog_key_words)
        if self.catalog:
            for i in range(len(self.catalog)):
                findall_pattern = words_str
                if re.findall(findall_pattern, self.catalog[i][0]):
                    if self.catalog[5][1] == self.catalog[6][1] or \
                            (self.catalog[1][1] == self.catalog[2][1] and i == 1):
                        page_begin = int(self.catalog[i - 1][1])
                        page_end = int(self.catalog[i][1])
                    else:
                        page_begin = int(self.catalog[i][1])
                        page_end = int(self.catalog[i + 1][1])
                    return range(page_begin - 1, page_end + 1)
        else:
            # 未找到对应页码
            return []


def get_industry_table(raw_df, table_key_words, column_location=0, rule=1, k=0):
    """
    :param raw_df: 由plumber直接提取后，转成dataframe类型
    :param table_key_words: 表格中的关键词 list
    :param column_location: 关键词所在列 int
    :param rule: 1 -> 多行合并(单个cell有双行内容); 2 -> 向上合并（单个cell有单行数据且连续）
    :param k: 按照第k列规律进行合并
    :return: 格式化的表 dataframe类型
    """
    temp_pd = raw_df.where(raw_df != '')
    raw_df.drop(temp_pd.count()[temp_pd.count() == 0].index, axis=1, inplace=True)
    # noinspection PyBroadException
    try:
        key_detection = raw_df.iloc[:, column_location].isin(table_key_words)
        # 判断表格中是否有关键字
        if key_detection.any():
            key_index = key_detection[key_detection].index[0] - 1
            if (len(raw_df.iloc[:, 0]) - raw_df.iloc[:, 0].count() >= 2) and \
                    not raw_df.iloc[:, 0].apply(linebreak).any():
                # 逐列向左合并
                df_null = temp_pd.dropna(axis=1, how='all')
                df_empty = df_null.where(df_null.notnull(), '')
                col_num = 0
                while True:
                    try:
                        if (df_null.iloc[key_index:, col_num] + df_null.iloc[key_index:, col_num + 1]).isnull().all():
                            df_empty.iloc[:, col_num] = df_empty.iloc[:, col_num] + df_empty.iloc[:, col_num + 1]
                            df_empty.drop(df_empty.columns[col_num + 1], axis=1, inplace=True)
                            df_null = df_empty.where(df_empty != '')
                        else:
                            col_num += 1
                    except IndexError:
                        break
                # rule==1 某列有双行
                if rule == 1:
                    # 4行同时合并
                    result = df_empty.copy()
                    line_j = 0
                    while True:
                        try:
                            if ((raw_df.iloc[line_j, k] is not None and raw_df.iloc[line_j + 2, k] is not None) and (
                                (raw_df.iloc[line_j + 1, k] is None) and (raw_df.iloc[line_j + 3, k] is None))) or \
                                (((raw_df.iloc[line_j, k] is None) and (raw_df.iloc[line_j + 2, k] is None) and (
                                    raw_df.iloc[line_j + 3, k] is None)) and (raw_df.iloc[line_j + 1, k] is not None)):
                                result.iloc[line_j, :] = result.iloc[line_j, :] + result.iloc[line_j + 1, :] + \
                                                    result.iloc[line_j + 2, :] + result.iloc[line_j + 3, :]
                                result.drop(result.index[[line_j + 1, line_j + 2, line_j + 3]], inplace=True)
                                raw_df.drop(raw_df.index[[line_j + 1, line_j + 2, line_j + 3]], inplace=True)
                                line_j += 1
                            else:
                                line_j += 1
                        except IndexError:
                            break
                    result = result.replace(ur'\n|（元）|\(元\)', '', regex=True)
                    result.iloc[:, 0] = result.iloc[:, 0].replace(ur'[（(]\d[）)]|[\d一二]、|[ -.?!,":;]', '',
                                                                  regex=True)
                    result.iloc[:, 0] = result.iloc[:, 0].replace(ur'（注\d?）|收入', '', regex=True)
                    result = result.where(result != '').dropna(axis=0, how='all')
                    return result.where(result.notnull(), '')
                # rule == 2 表格中某列连续
                elif rule == 2:
                    # 逐行向上合并
                    up_j = 1
                    while True:
                        try:
                            if df_empty.iloc[up_j, k]:
                                up_j += 1
                            else:
                                df_empty.iloc[up_j - 1, :] = df_empty.iloc[up_j - 1, :] + df_empty.iloc[up_j, :]
                                df_empty.drop(df_empty.index[up_j], axis=0, inplace=True)
                        except IndexError:
                            break
                    df_empty = df_empty.replace(ur'\n|（元）|\(元\)', '', regex=True)
                    df_empty.iloc[:, 0] = df_empty.iloc[:, 0].replace(ur'[（(]\d[）)]|[\d一二]、|[ -.?!,":;]', '',
                                                                      regex=True)
                    df_empty.iloc[:, 0] = df_empty.iloc[:, 0].replace(ur'（注\d?）|收入', '', regex=True)
                    result = df_empty.where(df_empty != '').dropna(axis=0, how='all')
                    return result.where(result.notnull(), '')
            else:
                # 逐列向左合并
                df_null = temp_pd.dropna(axis=1, how='all')
                df_empty = df_null.where(df_null.notnull(), '')
                col_num = 0
                while True:
                    try:
                        if (df_null.iloc[key_index:, col_num] + df_null.iloc[key_index:, col_num + 1]).isnull().all():
                            df_empty.iloc[:, col_num] = df_empty.iloc[:, col_num] + df_empty.iloc[:, col_num + 1]
                            df_empty.drop(df_empty.columns[col_num + 1], axis=1, inplace=True)
                            df_null = df_empty.where(df_empty != '')
                        else:
                            col_num += 1
                    except IndexError:
                        break
                df_empty = df_empty.replace(ur'\n|（元）|\(元\)', '', regex=True)
                df_empty.iloc[:, 0] = df_empty.iloc[:, 0].replace(ur'[（(]\d[）)]|[\d一二]、|[ -.?!,":;]', '', regex=True)
                df_empty.iloc[:, 0] = df_empty.iloc[:, 0].replace(ur'（注\d?）|收入', '', regex=True)
                result = df_empty.where(df_empty != '').dropna(axis=0, how='all')
                return result.where(result.notnull(), '')
        return pd.DataFrame({})
    except Exception:
        return pd.DataFrame({})


def main(dir_path, new_file_name):
    t0 = time.clock()
    count = 0
    pd.set_option('display.max_columns', None)

    file_path_list = os.listdir(dir_path)
    general_table = pd.DataFrame({})
    money_unit = u'元'

    with codecs.open(new_file_name, 'w', encoding='utf-8') as f:
        f.write('[')
        for each_file in file_path_list:
            if os.path.splitext(each_file)[1] == '.pdf':
                cp_code = ''
                count += 1
                # noinspection PyBroadException
                try:
                    pattern_code = ur'\d*'
                    cp_code = re.match(pattern_code, each_file).group()
                    print cp_code
                except Exception:
                    pass
                new_file = os.path.join(dir_path, each_file)
                pdf_extract = PdfExtract(pdfplumber.open(new_file))
                # noinspection PyBroadException
                try:
                    cp_name = pdf_extract.company_name
                    general_input = [[u'经营情况'], [u'分行业'], 0]
                    page_range = pdf_extract.get_page_number(general_input[0])
                    if not page_range:
                        general_table = pd.DataFrame({})
                    else:
                        # 搜索表格的起始页码
                        page_num = page_range[0]
                        search_switch = True
                        while search_switch:
                            tables_list = pdf_extract.pdf.pages[page_num].extract_tables()
                            # 逐个表格查找
                            for m in range(len(tables_list)):
                                raw_table = pd.DataFrame(tables_list[m])
                                general_table = get_industry_table(raw_table, general_input[1],
                                                                   general_input[2], rule=1, k=0)
                                if general_table.values.any():
                                    page_content = pdf_extract.pdf.pages[page_num].extract_text()
                                    pattern = ur'单位[：:](.*?元)'
                                    search_result = re.search(pattern, page_content)
                                    if search_result:
                                        money_unit = search_result.group(1)
                                    search_switch = False
                                    break
                            page_num += 1
                            if page_num > page_range[-1]:
                                break
                except Exception:
                    # 该文档不能被解析
                    continue
                page_num = 0
                table_dict = dict()
                table_dict[u'行业分类'] = []
                table_dict[u'分行业收入'] = []
                # 处理提取后的表格 (分行业营业收入表格提取)
                if cp_name and general_table.values.any():
                    table_dict[u'公司名称'] = cp_name
                    table_dict[u'股票代码'] = cp_code
                    # 行业结束搜索的关键词
                    keywords_list = [u'合计', u'主营业务分产品情况', u'分产品', u'分业务', u'分产品情况', u'内部抵销',
                                     u'小计', u'主营业务分地区情况', u'分地区', u'内部抵销数', u'减：内部抵销数',
                                     u'主营业务分业务情况', u'减：内部抵消', u'减内部抵消数', u'其中：内部抵消',
                                     u'减：公司内部抵消数', u'减内部抵消', u'内部抵消数', u'主营业务收入合计',
                                     u'主营业务对本公司营业收入影响较大的分地区的情况', u'主营业务分产品情冴',
                                     u'合并抵消数', u'主营业务分产品情况（未经抵销）', u'合并抵消', u'合并抵销',
                                     u'减：合并抵消数', u'平台间关联交易', u'分部间抵销', u'分部间抵消', u'抵消',
                                     u'公司内各业务分部抵销', u'抵销', u'公司内各业务分部相互抵销', u'分部间相互抵减',
                                     u'公司内部各业务、分部相互抵销', u'公司内各业务部间相互抵销', u'内部交易抵消',
                                     u'抵消数', u'板块抵消', u'公司内各业务分部相互抵消', u'内部抵消', u'行业之间抵消',
                                     u'公司内部行业抵减', u'其他主营业务分产品情况']
                    if general_table.iloc[:, 0].isin(keywords_list).any():
                        final_table = general_table
                    else:
                        next_raw_table = pd.DataFrame(pdf_extract.pdf.pages[page_num].extract_tables()[0])
                        next_table = get_industry_table(next_raw_table, keywords_list, 0, rule=1, k=0)
                        if not next_table.iloc[0, 1:3].any():
                            general_table.iloc[-1, :] = general_table.iloc[-1, :] + next_table.iloc[0, :]
                            next_table.drop(next_table.index[0], inplace=True)
                        else:
                            pass
                        final_table = pd.concat([general_table, next_table], ignore_index=True)
                    table = list(final_table.iloc[:, 0])
                    index_begin = None
                    index_end = None
                    for i in range(len(table)):
                        j = i + 1
                        while table[i] == u'分行业':
                            index_begin = i + 1
                            try:
                                if table[j] in keywords_list:
                                    index_end = j
                                    break
                                else:
                                    j += 1
                            except IndexError:
                                # 表格提取错误？
                                break
                        if index_end:
                            table_dict[u'行业分类'].extend(table[index_begin:index_end])
                            transition = final_table.iloc[:, 1][index_begin:index_end].replace(r',', '', regex=True)
                            transition = transition.replace('', 0).astype(float).values
                            if money_unit == u'万元':
                                transition = transition * 10000
                            elif money_unit == u'十万':
                                transition = transition * 100000
                            elif money_unit == u'百万元':
                                transition = transition * 1000000
                            elif money_unit == u'千万元':
                                transition = transition * 10000000
                            elif money_unit == u'亿元':
                                transition = transition * 100000000
                            trans_list = list(transition)
                            table_dict[u'分行业收入'].extend(trans_list)
                            break
                    if table_dict[u'行业分类']:
                        js_obj = json.dumps(table_dict)
                        f.write(js_obj + ',\n')
        f.write(u'{"end":"结束"}]')

    print u'时间：%.2f秒' % (time.clock() - t0), u'文件数：%d个' % count
    print u'平均时间：%.2f(秒/个)' % ((time.clock() - t0) / count)


if __name__ == '__main__':
    # file_path: pdf文件所在的文件夹路径
    file_path = ur'C:/Users/luoming3/Desktop/Report2/问题文档9/'

    main(file_path, 'Industry-data.json')
