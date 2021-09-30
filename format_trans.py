import json
import os
import pandas as pd
import shutil
import re


class FormatTrans:
    def __init__(self, file, config):
        # 从配置文件中获取读取和写入的路径
        self.config = config
        self.filepath = os.path.join(config["dir_path"], file)
        if not os.path.exists(config["save_path"]):
            os.mkdir(config["save_path"])
        self.save_path = os.path.join(config["save_path"], file)
        # 文件名各个字段：资料属性，数据类型，文件生成频次
        self.filename, _ = os.path.splitext(file)
        items = self.filename.split('_')
        self.datatype = items[-2]
        self.station_id = items[3]

    def raw_trans(self):
        """检查并修改基数据文件，放到修改后的目录"""
        with open(self.filepath, 'r') as file:
            # 处理测站基本参数，在文件的前两行
            line1 = file.readline().strip().split(',')
            line2 = file.readline().strip().split(',')
            line1[0] = 'MWR'
            line1[1] = format(float(line1[1]), '.2f').zfill(5)
            line2[0] = self.station_id
            line2[1] = format(float(line2[1]), '.4f')
            line2[2] = format(float(line2[2]), '.4f')
            line2[3] = format(float(line2[3]), '.1f')
            line1 = ','.join(line1)+'\n'
            line2 = ','.join(line2)+'\n'

            # 读取观测数据实体部分，为文件剩余行
            content = pd.read_table(file, sep=',')
            # 令record从1递增
            content.loc[:, 'Record'] = range(1, len(content) + 1)
            # QCFlag_BT置为00000
            content.loc[:, 'QCFlag_BT'] = '00000'
            # 保留两位小数的字段
            keep_2_decimal = ['SurTem(℃)', 'SurHum(%)', 'SurPre(hPa)', 'Tir(℃)']
            for column in keep_2_decimal:
                content.loc[:, column] = content.loc[:, column].apply(lambda x: format(x, '.2f'))
            # 保留三位小数的字段
            keep_3_decimal = ['Az(deg)', 'El(deg)']
            for column in content.columns:
                if column.startswith('Ch'):
                    keep_3_decimal.append(column)
            for column in keep_3_decimal:
                content.loc[:, column] = content.loc[:, column].apply(lambda x: format(x, '.3f'))

            # 写入数据并将测站参数写入文件开头
            content.to_csv(self.save_path, index=False)
            with open(self.save_path, 'r+', encoding='utf-8') as save_file:
                content = save_file.read()
                save_file.seek(0, 0)
                save_file.write(line1)
                save_file.write(line2)
                save_file.write(content)

    def cp_trans(self):
        """检查并修改气象要素数据，放到修改后的目录"""
        with open(self.filepath, 'r') as file:
            # 处理测站基本参数，同上
            line1 = file.readline().strip().split(',')
            line2 = file.readline().strip().split(',')
            line1[0] = 'MWR'
            line1[1] = format(float(line1[1]), '.2f').zfill(5)
            line2[0] = self.station_id
            line2[1] = format(float(line2[1]), '.4f')
            line2[2] = format(float(line2[2]), '.4f')
            line2[3] = format(float(line2[3]), '.1f')
            line1 = ','.join(line1) + '\n'
            line2 = ','.join(line2) + '\n'

            # 读取观测数据实体部分，为文件剩余行
            content = pd.read_table(file, sep=',')
            content.loc[:, 'Record'] = range(1, len(content) + 1)
            # 保留两位小数的字段
            keep_2_decimal = ['SurTem(℃)', 'SurHum(%)', 'SurPre(hPa)', 'Tir(℃)',
                              'CloudBase(km)', 'Vint(mm)', 'Lqint(mm)']
            for column in keep_2_decimal:
                content.loc[:, column] = content.loc[:, column].apply(lambda x: format(x, '.2f'))
            # 保留三位小数的字段
            keep_3_decimal = []
            for column in content.columns:
                if column.endswith('(km)') and column != 'CloudBase(km)':
                    keep_3_decimal.append(column)
            for column in keep_3_decimal:
                content.loc[:, column] = content.loc[:, column].apply(lambda x: format(x, '.3f'))

            # 写入数据并将测站参数写入文件开头
            content.to_csv(self.save_path, index=False)
            with open(self.save_path, 'r+', encoding='utf-8') as save_file:
                content = save_file.read()
                save_file.seek(0, 0)
                save_file.write(line1)
                save_file.write(line2)
                save_file.write(content)

    def cal_trans(self):
        """检查并修改定标文件，放到修改后的目录"""
        with open(self.filepath) as file:
            with open(self.save_path, 'w') as save_file:
                record = 1
                while True:
                    line = file.readline()
                    if line:
                        # 查找每行开头为 数字加逗号 的字符串，对其数字部分改为顺序递增
                        if re.search(r'^\d+\,', line):
                            line = line.split(",")
                            line[0] = str(record)
                            record += 1
                            line = ','.join(line)
                        save_file.write(line)
                    else:
                        break

    def copy_file(self):
        """无需检查的内容直接复制至目标目录"""
        shutil.copy(self.filepath, self.config["save_path"])


def main():
    try:
        with open(r"config/fc_config.json", "r", encoding='utf-8') as config_json:
            config = json.load(config_json)

        dir_path = config["dir_path"]
        files = os.listdir(dir_path)
        for file in files:
            ft = FormatTrans(file, config)
            if ft.datatype == 'RAW':
                ft.raw_trans()
            elif ft.datatype == 'CP':
                ft.cp_trans()
            elif ft.datatype == 'CAL':
                ft.cal_trans()
            elif ft.datatype == 'STA':
                ft.copy_file()
    except Exception as e:
        print(e)


main()
