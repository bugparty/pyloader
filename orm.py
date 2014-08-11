__author__ = 'hanbowen'


class Model:
    def __init__(self):
        pass


class Field:
    pass


class StringField(Field):
    def __init__(self, size=64):
        self._type = 'VARCHAR'
        self._size = size


class DateField(Field):
    def __init__(self):
        self._type = 'DATE'


class IntField(Field):
    def __init__(self):
        self._type = 'INT'


class User(Model):
    username = StringField()
    age = IntField()
    birthday = DateField()


def create_table(model):
    '''A sample of the table description is like that

    mysql> desc tb02e946a0
        -> ;
    +-------+----------+------+-----+---------+-------+
    | Field | Type     | Null | Key | Default | Extra |
    +-------+----------+------+-----+---------+-------+
    | pos   | int(11)  | YES  |     | NULL    |       |
    | tree  | char(20) | YES  |     | NULL    |       |
    +-------+----------+------+-----+---------+-------+
    2 rows in set (0.18 sec)
    so we will simlulate this style
    '''

    create_table = 'CREATE TABLE %s'
    sql_head = create_table % model.__name__
    print 'creating table %s:' % model.__name__
    desc_model(model)


def gen_desc_model_row(cls, method):
    field = getattr(cls, method)
    m_type = field._type
    keys = dir(field)
    if ('_size' not in keys):
        row = '|{0:^12.12}|{1:^14.14}|{2:^6.6}|{3:^5.5}|{4:^9.9}|{5:^7.7}|'.format(
            method, field._type, 'Yes', '', 'NULL', '')
        return row
    else:
        row = '|{0:^12.12}|{1:^14.14}|{2:^6.6}|{3:^5.5}|{4:^9.9}|{5:^7.7}|'.format(
            method, field._type + "(%s)" % field._size, 'Yes', '', 'NULL', '')
        return row


def desc_model(model):
    table_header_str = '''+------------+--------------+------+-----+---------+-------+
|   Field    |      Type    | Null | Key | Default | Extra |
+------------+--------------+------+-----+---------+-------+'''
    table_end_str = '''+------------+--------------+------+-----+---------+-------+'''
    model_field_list = [method for method in dir(model)
                        if isinstance(getattr(model, method), Field)]
    print table_header_str
    for m in model_field_list:
        print gen_desc_model_row(model, m)
    print table_end_str


if __name__ == '__main__':
    create_table(User)