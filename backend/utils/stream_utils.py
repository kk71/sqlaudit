# Author: kk.Fang(fkfkbill@gmail.com)

import string
import random

import chardet

# from backend.models.oracle import


def check_file_encoding(file_stream):
    encoding_check = chardet.detect(file_stream)
    if float(encoding_check['confidence']) >= 0.65:
        encoding = encoding_check['encoding']
        encoding = 'gbk' if encoding == 'KOI8-R' else encoding
    else:
        encoding = 'gbk'
    return encoding


