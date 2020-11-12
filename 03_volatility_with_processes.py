# -*- coding: utf-8 -*-


# Описание предметной области:
#
# При торгах на бирже совершаются сделки - один купил, второй продал.
# Покупают и продают ценные бумаги (акции, облигации, фьючерсы, етс). Ценные бумаги - это по сути долговые расписки.
# Ценные бумаги выпускаются партиями, от десятка до несколько миллионов штук.
# Каждая такая партия (выпуск) имеет свой торговый код на бирже - тикер - https://goo.gl/MJQ5Lq
# Все бумаги из этой партии (выпуска) одинаковы в цене, поэтому говорят о цене одной бумаги.
# У разных выпусков бумаг - разные цены, которые могут отличаться в сотни и тысячи раз.
# Каждая биржевая сделка характеризуется:
#   тикер ценнной бумаги
#   время сделки
#   цена сделки
#   обьем сделки (сколько ценных бумаг было куплено)
#
# В ходе торгов цены сделок могут со временем расти и понижаться. Величина изменения цен называтея волатильностью.
# Например, если бумага №1 торговалась с ценами 11, 11, 12, 11, 12, 11, 11, 11 - то она мало волатильна.
# А если у бумаги №2 цены сделок были: 20, 15, 23, 56, 100, 50, 3, 10 - то такая бумага имеет большую волатильность.
# Волатильность можно считать разными способами, мы будем считать сильно упрощенным способом -
# отклонение в процентах от полусуммы крайних значений цены за торговую сессию:
#   полусумма = (максимальная цена + минимальная цена) / 2
#   волатильность = ((максимальная цена - минимальная цена) / полусумма) * 100%
# Например для бумаги №1:
#   half_sum = (12 + 11) / 2 = 11.5
#   volatility = ((12 - 11) / half_sum) * 100 = 8.7%
# Для бумаги №2:
#   half_sum = (100 + 3) / 2 = 51.5
#   volatility = ((100 - 3) / half_sum) * 100 = 188.34%
#
# В реальности волатильность рассчитывается так: https://goo.gl/VJNmmY
#
# Задача: вычислить 3 тикера с максимальной и 3 тикера с минимальной волатильностью.
# Бумаги с нулевой волатильностью вывести отдельно.
# Результаты вывести на консоль в виде:
#   Максимальная волатильность:
#       ТИКЕР1 - ХХХ.ХХ %
#       ТИКЕР2 - ХХХ.ХХ %
#       ТИКЕР3 - ХХХ.ХХ %
#   Минимальная волатильность:
#       ТИКЕР4 - ХХХ.ХХ %
#       ТИКЕР5 - ХХХ.ХХ %
#       ТИКЕР6 - ХХХ.ХХ %
#   Нулевая волатильность:
#       ТИКЕР7, ТИКЕР8, ТИКЕР9, ТИКЕР10, ТИКЕР11, ТИКЕР12
# Волатильности указывать в порядке убывания. Тикеры с нулевой волатильностью упорядочить по имени.
#
# Подготовка исходных данных
# 1. Скачать файл https://drive.google.com/file/d/1l5sia-9c-t91iIPiGyBc1s9mQ8RgTNqb/view?usp=sharing
#       (обратите внимание на значок скачивания в правом верхнем углу,
#       см https://drive.google.com/file/d/1M6mW1jI2RdZhdSCEmlbFi5eoAXOR3u6G/view?usp=sharing)
# 2. Раззиповать средствами операционной системы содержимое архива
#       в папку python_base/lesson_012/trades
# 3. В каждом файле в папке trades содержится данные по сделакам по одному тикеру, разделенные запятыми.
#   Первая строка - название колонок:
#       SECID - тикер
#       TRADETIME - время сделки
#       PRICE - цена сделки
#       QUANTITY - количество бумаг в этой сделке
#   Все последующие строки в файле - данные о сделках
#
# Подсказка: нужно последовательно открывать каждый файл, вычитывать данные, высчитывать волатильность и запоминать.
# Вывод на консоль можно сделать только после обработки всех файлов.
#
# Для плавного перехода к мультипоточности, код оформить в обьектном стиле, используя следующий каркас
#
# class <Название класса>:
#
#     def __init__(self, <параметры>):
#         <сохранение параметров>
#
#     def run(self):
#         <обработка данных>
import os
import time
from collections import OrderedDict
import multiprocessing
from queue import Empty
import importlib

simple_volatility = importlib.import_module('01_volatility')


class TickerVolatility(multiprocessing.Process, simple_volatility.TickerVolatility):

    def __init__(self, filename, queue, lock, zero_queue, *args, **kwargs):
        super(TickerVolatility, self).__init__(*args, **kwargs)
        self.filename = filename
        self.queue = queue
        self.zero_queue = zero_queue
        self.dlock = lock

    def run(self):
        simple_volatility.TickerVolatility.run(self)

    def act(self):
        if self.volatility == 0:
            print('кладу в очередь', self.row_dict['SECID'], flush=True)
            self.zero_queue.put(self.row_dict['SECID'])
        else:
            self.queue.put([self.row_dict['SECID'], self.volatility])
            print('кладу в очередь', self.row_dict['SECID'], self.volatility, flush=True)


class Collector:

    def __init__(self, tickers, lock, *args, **kwargs):
        super(Collector, self).__init__(*args, **kwargs)
        self.tickers = tickers
        self.queue = multiprocessing.Queue()
        self.zero_queue = multiprocessing.Queue()
        self.collector_volatilities_dict = OrderedDict()
        self.collector_zero_volatilities = []
        self.lock = lock
        self.ticker_proc_list = []

    def sort_volatility_dict(self, dict, n=3, reverse=False):
        i = 0
        d = OrderedDict()
        sorted_dict = OrderedDict(sorted(dict.items(), key=lambda k: k[1], reverse=reverse))
        for ticket, value in sorted_dict.items():
            if i == n:
                break
            d[ticket] = value
            i += 1
        return d

    def run(self):
        for ticker in self.tickers:
            ticker_proc = TickerVolatility(filename=ticker, queue=self.queue, lock=self.lock,
                                           zero_queue=self.zero_queue)
            self.ticker_proc_list.append(ticker_proc)
        for proc in self.ticker_proc_list:
            proc.start()
        while True:
            try:
                receiver = self.queue.get(timeout=1)
                print('вытаскиваю из очереди', receiver, flush=True)
                self.collector_volatilities_dict[receiver[0]] = receiver[1]
            except Empty:
                if not any(ticker.is_alive() for ticker in self.ticker_proc_list):
                    break
        while True:
            try:
                zero_receiver = self.zero_queue.get(timeout=1)
                print('вытаскиваю из очереди', zero_receiver, flush=True)
                self.collector_zero_volatilities.append(zero_receiver)
            except Empty:
                if not any(ticker.is_alive() for ticker in self.ticker_proc_list):
                    break

        max_dict = self.sort_volatility_dict(dict=self.collector_volatilities_dict, n=3, reverse=True)
        min_dict = self.sort_volatility_dict(dict=self.collector_volatilities_dict, n=3, reverse=False)
        self.collector_zero_volatilities.sort()

        print(len(self.collector_volatilities_dict))
        print('Максимальная волатильность:')
        for ticket, value in max_dict.items():
            print('\t', ticket, round(value, 2), '%')

        print('Минимальная волатильность:')
        for ticket, value in reversed(min_dict.items()):
            print('\t', ticket, round(value, 2), '%')

        print('Нулевая волатильность:', end='\n\t')
        for ticket in self.collector_zero_volatilities:
            print(ticket, end=', ')

        for proc in self.ticker_proc_list:
            proc.join()


if __name__ == '__main__':
    start = time.time()
    path = 'trades'
    counters = []
    lock = multiprocessing.Lock()

    if not os.access(path, os.F_OK):
        raise BaseException('Такой папки не существует')
    files = os.walk(path)

    for dirpath, dirnames, filenames in os.walk(path):
        for file in filenames:
            counters.append(os.path.join(dirpath, file))

    collector = Collector(tickers=counters, lock=lock)
    collector.run()

    print(time.time() - start)

# зачёт! 🚀
