# coding:utf-8

from __future__ import unicode_literals

import os
import re
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


def table2dict(general_table_input, axis=0, mode='digital'):
    """
    :param general_table_input: 提取好的表格
    :param axis: 0代表按行取字典，1代表按列取字典。默认为0
    :param mode: head代表按照名称作为字典的键，digital代表按照数字作为字典的键
    :return: 输出字典
    """
    (row, col) = general_table_input.shape
    result_dict = dict()
    if mode == 'digital':
        if axis == 0:
            for n in range(row):
                result_dict[n] = list(general_table_input.iloc[n, :])
            return result_dict
        elif axis == 1:
            for n in range(col):
                result_dict[n] = list(general_table_input.iloc[:, n])
            return result_dict
    elif mode == 'head':
        if axis == 0:
            for n in range(1, row):
                row_name = general_table_input.iloc[n, 0]
                result_dict[row_name] = list(general_table_input.iloc[n, 1:])
            return result_dict
        elif axis == 1:
            for n in range(1, col):
                col_name = general_table_input.iloc[0, n]
                result_dict[col_name] = list(general_table_input.iloc[1:, n])
            return result_dict


def column_merge(temp_pd):
    """
    从左到右，逐列将和全为null的列合并
    :param temp_pd:
    :return:
    """
    col_num = 0
    df_null = temp_pd.dropna(axis=1, how='all')
    df_empty = df_null.where(df_null.notnull(), '')
    while True:
        try:
            if (df_null.iloc[:, col_num] + df_null.iloc[:, col_num + 1]).isnull().all():
                df_empty.iloc[:, col_num] = df_empty.iloc[:, col_num] + df_empty.iloc[:, col_num + 1]
                df_empty.drop(df_empty.columns[col_num + 1], axis=1, inplace=True)
                df_null = df_empty.where(df_empty != '')
            else:
                col_num += 1
        except IndexError:
            break
    return df_empty, df_null


def multi_rows_merge(raw_df, df_empty, k=0):
    """
    四行合并成一行
    :param raw_df: 原始dataframe
    :param df_empty: df_empty
    :param k: 参考列
    :return:
    """
    line_j = 0
    while True:
        try:
            if ((raw_df.iloc[line_j, k] is not None and raw_df.iloc[line_j + 2, k] is not None) and
                ((raw_df.iloc[line_j + 1, k] is None) and (raw_df.iloc[line_j + 3, k] is None))) or \
                (((raw_df.iloc[line_j, k] is None) and (raw_df.iloc[line_j + 2, k] is None) and
                 (raw_df.iloc[line_j + 3, k] is None)) and (raw_df.iloc[line_j + 1, k] is not None)):
                df_empty.iloc[line_j, :] = df_empty.iloc[line_j, :] + df_empty.iloc[line_j + 1, :] + \
                                         df_empty.iloc[line_j + 2, :] + df_empty.iloc[line_j + 3, :]
                df_empty.drop(df_empty.index[[line_j + 1, line_j + 2, line_j + 3]], inplace=True)
                raw_df.drop(raw_df.index[[line_j + 1, line_j + 2, line_j + 3]], inplace=True)
                line_j += 1
            else:
                line_j += 1
        except IndexError:
            break
    result = df_empty.where(df_empty != '').dropna(axis=0, how='all')
    return result.where(result.notnull(), '')


def line_by_line_merge(df_empty, k=0):
    """
    从上往下逐行向上合并
    :param df_empty:
    :param k:
    :return:
    """
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
    result = df_empty.where(df_empty != '').dropna(axis=0, how='all')
    return result.where(result.notnull(), '')


class PDFExtract(object):
    def __init__(self, pdf):
        self.pdf = pdf
        self.company_name = ''
        self.catalog = ''
        for page in self.pdf.pages[:10]:
            con_text = page.extract_text()
            if con_text:
                if not self.company_name:
                    name_pattern = r'[（）\u4e00-\u9fa5]{2,}公司'
                    company_name = re.search(name_pattern, con_text)
                    if company_name:
                        self.company_name = company_name.group()
                        print self.company_name
                if not self.catalog:
                    catalog_pattern = r'([、\u4e00-\u9fa5]+)[ \t\r\f\v]*?[\u2026\.]+[ \t\r\f\v]*?(\d+)'
                    # 目录
                    catalog = re.findall(catalog_pattern, con_text)
                    self.catalog = catalog
                if self.catalog and self.company_name:
                    break
        if not self.catalog:
            pass
            print '没有搜索到目录'

    def get_page_number(self, catalog_key_word_list):
        """
        :param catalog_key_word_list: 该表所在目录的关键词列表
        :return: 该表的页面范围
        """
        if self.catalog:
            find_catalog_pattern = '|'.join(catalog_key_word_list)
            for i in range(len(self.catalog)):
                if re.findall(find_catalog_pattern, self.catalog[i][0]):
                    if self.catalog[5][1] == self.catalog[6][1] or \
                            (self.catalog[1][1] == self.catalog[2][1] and i == 1):
                        # 目录数字部分可能重复
                        page_begin = int(self.catalog[i - 1][1])
                        page_end = int(self.catalog[i][1])
                    else:
                        page_begin = int(self.catalog[i][1])
                        page_end = int(self.catalog[i + 1][1])
                    print '找到目录，结束目录循环'
                    return range(page_begin - 1, page_end + 1)
        else:
            print '未找到对应页码'
            return []

    @staticmethod
    def get_general_table(raw_df, table_key_words, column_location=0, rule=1, k=0):
        """
        :param raw_df: 由plumber直接提取后，转成dataframe类型
        :param table_key_words: 表格中的关键词 list
        :param column_location: 关键词所在列 int
        :param rule: 1 -> 多行合并; 2 -> 向上合并
        :param k: 按照第k列规律进行合并
        :return: 格式化的表 dataframe类型
        """
        # 删除空列
        temp_pd = raw_df.where(raw_df != '')
        raw_df.drop(temp_pd.count()[temp_pd.count() == 0].index, axis=1, inplace=True)

        try:
            if raw_df.iloc[:, column_location].isin(table_key_words).any():
                if (len(raw_df.iloc[:, 0]) - raw_df.iloc[:, 0].count() >= 2) and \
                        not raw_df.iloc[:, 0].apply(linebreak).any():
                    # 有阴影的表格
                    # 列合并
                    df_empty, df_null = column_merge(temp_pd)

                    if rule == 1:
                        # 四行合并成一行
                        return multi_rows_merge(raw_df, df_empty, k)
                    elif rule == 2:
                        # 行向上合并
                        return line_by_line_merge(df_empty, k)
                else:
                    # 无阴影表格
                    # 列合并
                    df_empty, df_null = column_merge(temp_pd)

                    result = df_empty.where(df_empty != '').dropna(axis=0, how='all')
                    return result.where(result.notnull(), '')
            return pd.DataFrame({})
        except Exception as error:
            print error
            return pd.DataFrame({})


def main(dir_path, general_input, output_name):
    pd.set_option('display.max_columns', None)
    t0 = time.clock()
    count = 0

    file_path_list = os.listdir(dir_path)
    general_table = pd.DataFrame({})

    with codecs.open(output_name, 'w', encoding='utf-8') as f:
        f.write('[')
        for each_file in file_path_list[0:3]:
            if os.path.splitext(each_file)[1] == '.pdf':
                new_file = os.path.join(file_path, each_file)
                print new_file
                count += 1
                pdf_extract = PDFExtract(pdfplumber.open(new_file))
                try:
                    cp_name = pdf_extract.company_name
                    page_range = pdf_extract.get_page_number(general_input[0])
                    if not page_range:
                        print '没找到对应页'
                        general_table = pd.DataFrame({})
                    else:
                        page_num = page_range[0]
                        search_switch = True
                        while search_switch:
                            tables_list = pdf_extract.pdf.pages[page_num].extract_tables()
                            for m in range(len(tables_list)):
                                raw_table = pd.DataFrame(tables_list[m])
                                general_table = pdf_extract.get_general_table(raw_table, general_input[1],
                                                                              general_input[2], rule=general_input[3],
                                                                              k=general_input[4])
                                if general_table.values.any():
                                    print '页和表：', page_num + 1, m + 1
                                    print general_table
                                    search_switch = False
                                    break
                            page_num += 1
                            if page_num > page_range[-1]:
                                break
                except Exception as error:
                    print error
                    continue
                if cp_name and general_table.values.any():
                    result_table = table2dict(general_table, axis=general_input[5], mode=general_input[6])
                    js_obj = json.dumps(result_table)
                    f.write(js_obj + ',\n')

        f.write('{"end":"结束"}]')

    print '时间：%.2f秒' % (time.clock() - t0), '文件数：%d个' % count
    print '平均时间：%.2f(秒/个)' % ((time.clock() - t0) / count)


if __name__ == '__main__':
    file_path = r'C:/Users/luoming3/Desktop/Report2/问题文档7/'
    # v_0 = raw_input('请输入该表所在目录名：')
    # v_1 = raw_input('请输入该表关键字：')
    # v_2 = int(raw_input('请输入该表关键字所在列(0代表第一列)：'))
    # v_3 = int(raw_input('请输入该表合并规则：\n1：某列连续并多行\n2：某列连续并单行'))
    # v_4 = int(raw_input('请输入该表合并规则所在列(0代表第一列)：'))
    # v_5 = int(raw_input('0：按行转化为字典；1：按列转化为字典'))
    # v_6 = raw_input('digital：按照行号或列号作为key；head: 按照第一行或第一列作为key')
    # variable_input = [[v_0], [v_1], v_2, v_3, v_4, v_5, v_6]

    variable_input = [['经营情况'], ['分行业'], 0, 1, 0, 1, 'digital']
    main(file_path, variable_input, "test.json")
