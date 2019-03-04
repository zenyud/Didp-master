# -*- coding: UTF-8 -*-  

# Date Time     : 2018/12/25
# Write By      : adtec(ZENGYU)
# Function Desc : 接入表字段信息
# History       : 2018/12/25  ZENGYU     Create
# Remarks       :


class HiveFieldInfo(object):
    """
        Hive 字段信息
    """

    def __init__(self, col_name, data_type, default_value, not_null, unique,
                 comment, col_seq):

        self.full_type = data_type
        self.col_name = col_name
        if "(" not in data_type:
            self.data_type = data_type

        else:
            index1 = data_type.index("(")
            self.data_type = data_type[:index1]
        self.default_value = default_value
        self.not_null = 0 if not_null == 'No' else 1
        self.unique = unique
        self.comment = comment
        self.col_seq = col_seq  # 字段序号

    def get_list(self):
        if "(" in self.full_type and ")" in self.full_type:
            index1 = self.full_type.index("(")
            index2 = self.full_type.index(")")
            list = self.full_type[index1 + 1:index2].split(",")
            return list
        else:
            return None

    @property
    def col_length(self):
        list = self.get_list()
        if list:
            return list[0]
        else:
            return None
            # return self.col_length

    @col_length.setter
    def col_length(self, v):
        self.col_length = v

    @property
    def col_scale(self):
        list = self.get_list()
        if list and len(list) == 2:
            if str(list[1]).isdigit():
                return list[1]
        else:
            return None

    @col_scale.setter
    def col_scale(self, v):
        self.col_scale = v

    @property
    def col_name_quote(self):
        return "`" + self.col_name + "`"

    def get_full_type(self):
        if self.col_length and self.col_scale:
            return str(
                self.data_type + "(" + self.col_length + "," + self.col_scale + ")")
        elif self.col_length and not self.col_scale:
            return str(self.data_type + "(" + self.col_length + ")")
        else:
            return str(self.data_type)


class MetaTypeInfo(object):
    """
       字段类型
    """
    def __init__(self, field_type, field_length, field_scale):
        """
        :param field_type: 字段类型
        :param field_length: 字段长度
        :param field_scale: 字段精度
        """
        self.field_type = field_type
        self.field_length = int(field_length) if field_length else None
        self.field_scale = int(field_scale) if field_scale else None

    def __eq__(self, obj):
        """
            重写__eq__方法
        :param obj:
        :return:
        """
        if type(obj) == type(self):
            if obj.field_length == self.field_length and obj.field_type.__eq__(
                    self.field_type) and obj.field_scale == self.field_scale:
                return True
            else:
                return False
        elif obj is None:
            return False
        else:
            super(MetaTypeInfo, self).__eq__(obj)

    @property
    def get_whole_type(self):
        types = ["DECIMAL", "DOUBLE", "FLOAT"]
        if self.field_length > 0:
            if self.field_scale > 0:
                return self.field_type + "(" + str(self.field_length) + "," + \
                       str(self.field_scale) + ")"
            else:
                if self.field_type in types:
                    return self.field_type + "(" + str(
                        self.field_length) + "," + \
                           str(self.field_scale) + ")"
                else:
                    return self.field_type + "(" + str(self.field_length) + ")"

        else:
            return self.field_type

    def set_whole_type(self, whole_type):
        """

            解析字段 获取字段长度和精度
        :param whole_type:  如 DECIMAL(M,N)
        :return:
        """
        if "(" in whole_type and ")" in whole_type:
            index1 = whole_type.index("(")
            index2 = whole_type.index(")")
            s = whole_type[index1 + 1, index2]
            self.field_type = whole_type[0:index1]
            list = s.split(",")
            if len(list) == 1:
                self.field_length = int(list[0])
            elif len(list) == 2:
                self.field_length, self.filed_scale = [int(x) for x in list]
        else:
            self.field_type = whole_type


class FieldState(object):
    """
        字段的状态
        标记当前字段的变更情况< 新增/删除/精度变化>
    """

    def __init__(self, col_name, full_seq, current_seq, ddl_type, hive_type,
                 comment_hive, comment_ddl, hive_no):
        # type: (object, object, object, MetaTypeInfo,MetaTypeInfo, object, object, object) -> FieldState
        self.col_name = col_name
        self.full_seq = full_seq  # 完整序号
        self.current_seq = current_seq  # 当前序号
        self.ddl_type = ddl_type  # 接入字段的数据类型
        self.hive_type = hive_type  # hive字段数据类型
        self.comment_hive = comment_hive  # hive的字段评论
        self.comment_ddl = comment_ddl  # 接入的 字段评论

        #  字段状态序号
        self.hive_no = hive_no  # -1:预览空位 ; -2:hive 需新增字段


if __name__ == '__main__':
    a = HiveFieldInfo("aa", "varchar(10)", None, None, None, "", 1)
    print a.get_full_type
