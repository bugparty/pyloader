import unittest


def stat_httpcode(lines, linenum):
    result = {}
    for line in lines:
        key = int(line[linenum])
        try:
            result[key] += 1
        except KeyError, e:
            # print 'key',e,'not exist,add it'
            result[key] = 1
    total = 0
    for v in result.items():
        total += v[1]
    deads = total - result[200]
    deadlink_rate = 1.0 * deads / total
    result['total'] = total
    result['rate'] = deadlink_rate
    result['death'] = deads

    return result


class StatisticsUnitTest(unittest.TestCase):
    def setUp(self):
        pass

    def test_stat_httpcode(self):
        lines = [
            [1, 1, 1, 302],
            [1, 1, 1, 1], [1, 1, 1, 1],
            [1, 1, 1, 777], [1, 1, 1, 777], [1, 1, 1, 777], [1, 1, 1, 777],
            [1, 1, 1, 404], [1, 1, 1, 404], [1, 1, 1, 404], [1, 1, 1, 404],
            [1, 1, 1, 404], [1, 1, 1, 404], [1, 1, 1, 404], [1, 1, 1, 404],

        ]
        result = stat_httpcode(lines, 3)
        self.assertEqual(1, result[302])
        self.assertEqual(2, result[1])
        self.assertEqual(4, result[777])
        self.assertEqual(8, result[404])


if __name__ == '__main__':
    unittest.main()