import os
import json
import re
import logging
import logging.config
from datetime import datetime
import pandas as pd


class FormatCheck:
    def __init__(self, file, config):
        self.config = config
        self.file = file
        self.file_path = os.path.join(config["dir_path"], file)
        self.filename, self.ext = os.path.splitext(file)
        items = self.filename.split('_')
        self.datatype = items[-2]
        self.dev_model = items[-3]
        self.station_id = items[3]

    def raw_check(self):
        """
        检查基数据文件，返回文件错误码，默认为0000
        第一行：n1
        第二行：n2
        表头行：n3
        数据实体格式：n4
        """
        fc_flag = ['0', '0', '0', '0']
        n1, n2, n3, n4 = [], [], [], []
        with open(self.file_path, 'r') as f:
            # n1:第一行
            line1 = f.readline()
            line1 = line1.strip().split(',')
            # WMR
            if line1[0] != 'MWR':
                fc_flag[0] = '1'
                n1.append('MWR')
            # 数据格式版本号：2位整数，2位小数
            if self.__decimal_cnt(line1[1]) != 2 or self.__integer_cnt(line1[1]) != 2:
                fc_flag[0] = '1'
                n1.append('数据格式版本号格式错误')

            # n2:第二行
            line2 = f.readline()
            line2 = line2.strip().split(',')
            # 区站号：5位数字或第一位为字母，第二至五位位数字
            if not re.match(r'\w\d{4}', line2[0]):
                fc_flag[1] = '1'
                n2.append('区站号格式错误')
            # 经度：保留4位小数
            if self.__decimal_cnt(line2[1]) != 4:
                fc_flag[1] = '1'
                n2.append('经度小数位数不为4')
            # 纬度：保留4位小数
            if self.__decimal_cnt(line2[2]) != 4:
                fc_flag[1] = '1'
                n2.append('纬度小数位数不为4')
            # 观测场海拔高度：第一位为符号位,保留1位小数
            # if self.__decimal_cnt(line2[3]) != 1 or line2[3][0] not in ['+', '-']:
            #     fc_flag[1] = '1'
            #     n2.append('观测场海拔高度符号位错误')
            # 设备型号
            if line2[4] != self.dev_model:
                fc_flag[1] = '1'
                n2.append('设备型号不符')
            # 通道数：正整数
            if int(line2[5]) <= 0 or self.__decimal_cnt(line2[5]) != 0:
                fc_flag[1] = '1'
                n2.append('通道数不为正整数')
                ch_cnt = 0
            else:
                ch_cnt = int(line2[5])
            # 高度层结数：正整数
            try:
                if line2[6]:
                    fc_flag[1] = '1'
                    n2.append('基数据文件不包含高度层结数')
            except IndexError:
                pass

            # n3:表头行
            content = pd.read_table(f, sep=',')
            line3 = content.columns
            raw_head = self.config["heads"]["raw"]
            for i in range(len(raw_head) - 1):
                if line3[i] != raw_head[i]:
                    fc_flag[2] = '1'
                    n3.append(line3[i])
            if len(line3[len(raw_head) - 1:-1]) != ch_cnt:
                fc_flag[2] = '1'
                n3.append('通道数不符')
            for ch in line3[len(raw_head)-1:-1]:
                if not ch.startswith('Ch'):
                    fc_flag[2] = '1'
                    n3.append(ch)
            if line3[-1] != raw_head[-1]:
                fc_flag[2] = '1'
                n3.append(line3[-1])

            # n4:数据实体格式
            # Record:序号递增
            record = content.loc[:, 'Record'].values
            for i, value in enumerate(record):
                if value != i + 1:
                    fc_flag[3] = '1'
                    n4.append("Record未从1递增")
                    break
            # DateTime格式
            times = content.loc[:, 'DateTime']
            result = times.apply(lambda x: re.match(r'\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}', x)).values
            if None in result:
                fc_flag[3] = '1'
                n4.append("DateTime格式错误")
            # DateTime时间间隔
            flag1 = 1
            flag2 = 1
            for i in range(len(times)-1):
                delta = (datetime.strptime(times[i+1], '%Y-%m-%d %H:%M:%S') -
                         datetime.strptime(times[i], '%Y-%m-%d %H:%M:%S'))
                if (delta.seconds > 120 or delta.days < 0) and flag1:
                    flag1 = 0
                    n4.append(f'记录时间间隔异常，位置：{i+4}行')
                elif delta.seconds == 0 and flag2:
                    flag2 = 0
                    n4.append(f'记录时间重复，位置：{i+4}行')
                if flag1 == 0 and flag2 == 0:
                    break
            return self.file, ''.join(fc_flag), (n1, n2, n3, n4)

    def cp_check(self):
        """
        检查气象产品数据文件，返回文件错误码，默认为0000
        第一行：n1
        第二行：n2
        表头行：n3
        数据实体格式：n4
        """
        fc_flag = ['0', '0', '0', '0']
        n1, n2, n3, n4 = [], [], [], []
        with open(self.file_path, 'r') as f:
            # n1:第一行
            line1 = f.readline()
            line1 = line1.strip().split(',')
            # WMR
            if line1[0] != 'MWR':
                fc_flag[0] = '1'
                n1.append('MWR')
            # 数据格式版本号：2位整数，2位小数
            if self.__decimal_cnt(line1[1]) != 2 or self.__integer_cnt(line1[1]) != 2:
                fc_flag[0] = '1'
                n1.append('数据格式版本号格式错误')

            # n2:第二行
            line2 = f.readline()
            line2 = line2.strip().split(',')
            # 区站号：5位数字或第一位为字母，第二至五位位数字
            if not re.match(r'\w\d{4}', line2[0]):
                fc_flag[1] = '1'
                n2.append('区站号格式错误')
            # 经度：保留4位小数
            if self.__decimal_cnt(line2[1]) != 4:
                fc_flag[1] = '1'
                n2.append('经度小数位数不为4')
            # 纬度：保留4位小数
            if self.__decimal_cnt(line2[2]) != 4:
                fc_flag[1] = '1'
                n2.append('纬度小数位数不为4')
            # 观测场海拔高度：第一位为符号位,保留1位小数
            # if self.__decimal_cnt(line2[3]) != 1 or line2[3][0] not in ['+', '-']:
            #     fc_flag[1] = '1'
            #     n2.append('观测场海拔高度符号位错误')
            # 设备型号
            if line2[4] != self.dev_model:
                fc_flag[1] = '1'
                n2.append('设备型号不符')
            # 高度层结数：正整数
            if int(line2[5]) <= 0 or self.__decimal_cnt(line2[5]) != 0:
                fc_flag[1] = '1'
                n2.append('高度层结数不为正整数')
            # 通道数：不应出现在产品文件中
            try:
                if line2[6]:
                    fc_flag[1] = '1'
                    n2.append('产品数据文件不包含通道数')
                    lay_cnt = int(line2[6])
            except IndexError:
                lay_cnt = int(line2[5])

            # 表头行
            content = pd.read_table(f, sep=',')
            line3 = content.columns
            head = self.config["heads"]["cp"]
            for i in range(len(head) - 1):
                if line3[i] != head[i]:
                    fc_flag[2] = '1'
                    n3.append(line3[i])
            if len(line3[len(head) - 1:-1]) != lay_cnt:
                fc_flag[2] = '1'
                n3.append('高度层结数不符')
            for lay in line3[len(head)-1:-1]:
                if not lay.endswith('(km)'):
                    fc_flag[2] = '1'
                    n3.append("单位未放在括号中")
                    break
            if line3[-1] != head[-1]:
                fc_flag[2] = '1'
                n3.append(line3[-1])

            # n4:数据实体格式
            # Record:序号递增
            record = content.loc[:, 'Record'].values
            for i, value in enumerate(record):
                if value != i + 1:
                    fc_flag[3] = '1'
                    n4.append("Record未从1递增")
                    break
            # DateTime格式
            times = content.loc[:, 'DateTime']
            result = times.apply(lambda x: re.match(r'\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}', x)).values
            if None in result:
                fc_flag[3] = '1'
                n4.append("DateTime格式错误")

            # DateTime时间间隔
            times2 = []  # 每4个重复的时间为1组，只保留每组的时间
            for i in range(len(times)):
                if i % 4 == 0:
                    times2.append(times[i])
            flag1 = 1
            flag2 = 1
            for i in range(len(times2) - 1):
                delta = (datetime.strptime(times2[i + 1], '%Y-%m-%d %H:%M:%S') -
                         datetime.strptime(times2[i], '%Y-%m-%d %H:%M:%S'))
                if (delta.seconds > 120 or delta.days < 0) and flag1:
                    flag1 = 0
                    n4.append(f'记录时间间隔异常，位置：{4*i + 4}行')
                elif delta.seconds == 0 and flag2:
                    flag2 = 0
                    n4.append(f'记录时间重复，位置：{4*i + 4}行')
                if flag1 == 0 and flag2 == 0:
                    break

            return self.file, ''.join(fc_flag), (n1, n2, n3, n4)

    def sta_check(self):
        """
        检查设备状态数据文件，返回文件错误码，默认为00
        表头行：n1
        数据实体格式：n2
        """
        fc_flag = ['0', '0']
        n1, n2 = [], []
        with open(self.file_path, 'r') as f:
            # 表头行
            content = pd.read_table(f, sep=',')
            line1 = content.columns
            head = self.config["heads"]["sta"]
            for i in range(len(head)):
                if line1[i] != head[i]:
                    fc_flag[0] = '1'
                    n1.append(line1[i])

            # 数据实体格式
            # Record:序号递增
            record = content.loc[:, 'Record'].values
            for i, value in enumerate(record):
                if value != i + 1:
                    fc_flag[1] = '1'
                    n2.append("Record未从1递增")
                    break
            # DateTime
            times = content.loc[:, 'DateTime']
            result = times.apply(lambda x: re.match(r'\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}', x)).values
            if None in result:
                fc_flag[1] = '1'
                n2.append("DateTime格式错误")

            return self.file, ''.join(fc_flag), (n1, n2)

    def cal_check(self):
        """
        检查设备定标数据文件，默认为000
        第一行：n1
        表头行：n2
        定标数据组：n3
        """
        fc_flag = ['0', '0', '0']
        n1, n2, n3 = [], [], []
        # 分类文件内容
        with open(self.file_path, 'r') as f:
            text = f.readlines()
        lines1, heads, content = [], [], []
        for line in text:
            if re.match(r'\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}', line):
                lines1.append(line.rstrip('\n'))
            elif line.startswith('Record'):
                heads.append(line.rstrip('\n'))
            elif re.match(r'\d+,', line):
                content.append(line.rstrip('\n'))
            else:
                print("error", line.rstrip('\n'))

        # 第一行
        times = []
        for line in lines1:
            line = line.strip().split(',')
            if line[1] not in ['GAIN', 'ABSOLUTE', 'NOISE', 'TIPPING', 'OTHER']:
                fc_flag[0] = '1'
                n1.append('定标方法不符')
                break
            times.append(line[0])
        flag1 = 1
        flag2 = 1
        for i in range(len(times) - 1):
            delta = (datetime.strptime(times[i + 1], '%Y-%m-%d %H:%M:%S') -
                     datetime.strptime(times[i], '%Y-%m-%d %H:%M:%S'))
            if (delta.seconds > 120 or delta.days < 0) and flag1:
                flag1 = 0
                n1.append(f'记录时间间隔异常，位置：第{i+1}条')
            elif delta.seconds == 0 and flag2:
                flag2 = 0
                n1.append(f'记录时间重复，位置：第{i+1}条')
            if flag1 == 0 and flag2 == 0:
                break

        # 表头行
        for line in heads:
            line = line.strip().split(',')
            head = self.config["heads"]["cal"]
            for i in range(len(head)):
                if line[i] != head[i]:
                    fc_flag[1] = '1'
                    n2.append(line[i])
            for ch in line[len(head):]:
                if not ch.startswith('Ch '):
                    fc_flag[1] = '1'
                    n2.append(ch)

        # 定标数据组
        records, datatypes = [], []
        for line in content:
            line = line.strip().split(',')
            records.append(int(line[0]))
            datatypes.append(line[1])
        # record
        for i, value in enumerate(records):
            if value != i + 1:
                fc_flag[2] = '1'
                n3.append('Record未从1递增')
                break
        # datatype
        for datatype in datatypes:
            if datatype not in ["Alpha", "Noise Tn", "Gain", "TSysN", "N/A"]:
                fc_flag[2] = '1'
                n3.append('定标参数类型不符')
                break

        return self.file, ''.join(fc_flag), (n1, n2, n3)

    @staticmethod
    def __decimal_cnt(num):
        """返回小数个数"""
        if '.' in str(num):
            cnt = len(str(num).split('.')[1])
        else:
            cnt = 0
        return cnt

    @staticmethod
    def __integer_cnt(num):
        """返回整数个数"""
        return len(str(num).split('.')[0])


def main():
    try:
        with open(r"config/fc_config.json", "r", encoding='utf-8') as config_json:
            fc_config = json.load(config_json)

        # 载入日志配置
        with open(r"config/log_config.json", "r") as config_json:
            log_config = json.load(config_json)
        logging.config.dictConfig(log_config)
        fc_logger = logging.getLogger("fc_logger")
        console_logger = logging.getLogger("root")

        files = os.listdir(fc_config["dir_path"])
        for file in files:
            fc = FormatCheck(file, fc_config)
            if fc.datatype == 'RAW':
                result = fc.raw_check()
                fc_logger.info(f"文件名：{result[0]}\n错误码：{result[1]}\n错误内容：{result[2]}\n")
            elif fc.datatype == 'CP':
                result = fc.cp_check()
                fc_logger.info(f"文件名：{result[0]}\n错误码：{result[1]}\n错误内容：{result[2]}\n")
            elif fc.datatype == 'STA':
                result = fc.sta_check()
                fc_logger.info(f"文件名：{result[0]}\n错误码：{result[1]}\n错误内容：{result[2]}\n")
            elif fc.datatype == 'CAL':
                result = fc.cal_check()
                fc_logger.info(f"文件名：{result[0]}\n错误码：{result[1]}\n错误内容：{result[2]}\n")

    except Exception as e:
        fc_logger.error("检查程序出现错误，检查中止")
        console_logger.exception(e)


main()
