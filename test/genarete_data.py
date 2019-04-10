# -*- coding: UTF-8 -*-
import sys

from faker import Faker
reload(sys)
sys.setdefaultencoding('utf8')
fake = Faker(locale='zh_CN')


def gen_data():
    # 生成数据
    with open("1.txt", "w+") as f:
        for x in range(0, 1000000):
            id = str(x)
            name = fake.name()
            address = fake.address()
            company = fake.company()
            province = fake.province()
            city = fake.city_name()
            insert_time = '20190402'
            line = ','.join([id, name, address, company, province, city, insert_time])
            f.write(line)
            f.write("\n")


if __name__ == '__main__':
    gen_data()
